from __future__ import annotations

import os
import time
import json
import math
import pathlib
import secrets
import sqlite3
import smtplib
from email.message import EmailMessage
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
import urllib.request
import xml.etree.ElementTree as ET

import stripe  # type: ignore

from fastapi import FastAPI, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse


# -----------------------------------------------------------------------------
# Config (env) - non-Stripe stuff can be read once at startup
# -----------------------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()

YAHOO_SYMBOL = os.getenv("YAHOO_SYMBOL", "GC=F").strip()  # Gold futures
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "900"))  # 15 min

RSS_FEEDS_ENV = os.getenv("RSS_FEEDS", "https://www.fxstreet.com/rss/news")
RSS_FEEDS = [u.strip() for u in RSS_FEEDS_ENV.split(",") if u.strip()]

HISTORY_PATH = os.getenv("HISTORY_PATH", "data/history.jsonl").strip()

# Admin/dev key (alltid gyldig). Bytt når du deployer.
ADMIN_API_KEY = os.getenv("PREMIUM_API_KEY", "gullbrief-dev").strip()

# SQLite
DB_PATH = os.getenv("DB_PATH", "data/app.db").strip()

# Email (SMTP)
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
SMTP_FROM = os.getenv("SMTP_FROM", "").strip()  # f.eks. "Gullbrief <noreply@dittdomene.no>"
ALERT_BASE_URL = os.getenv("ALERT_BASE_URL", "http://127.0.0.1:8000").strip()

# Stripe URLs (read dynamically)
STRIPE_SUCCESS_URL_DEFAULT = os.getenv("STRIPE_SUCCESS_URL", "http://127.0.0.1:8000/success").strip()
STRIPE_CANCEL_URL_DEFAULT = os.getenv("STRIPE_CANCEL_URL", "http://127.0.0.1:8000/archive").strip()


# -----------------------------------------------------------------------------
# App + CORS
# -----------------------------------------------------------------------------
app = FastAPI(title="Gullbrief Research", version="1.8")

origins_env = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
allow_origins = [o.strip() for o in origins_env.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None

def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 25) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))

def http_get_text(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 25) -> str:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return data.decode("utf-8", errors="replace")

def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""


# -----------------------------------------------------------------------------
# Stripe helpers (IMPORTANT): read env dynamically so you don't fight restarts
# -----------------------------------------------------------------------------
def stripe_env() -> Dict[str, str]:
    sk = os.getenv("STRIPE_SECRET_KEY", "").strip()
    price = os.getenv("STRIPE_PRICE_ID", "").strip()
    success = os.getenv("STRIPE_SUCCESS_URL", STRIPE_SUCCESS_URL_DEFAULT).strip()
    cancel = os.getenv("STRIPE_CANCEL_URL", STRIPE_CANCEL_URL_DEFAULT).strip()
    whsec = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
    return {
        "secret_key": sk,
        "price_id": price,
        "success_url": success,
        "cancel_url": cancel,
        "webhook_secret": whsec,
    }

def stripe_ready() -> bool:
    e = stripe_env()
    return bool(e["secret_key"] and e["price_id"])

def require_stripe() -> Dict[str, str]:
    e = stripe_env()
    if not (e["secret_key"] and e["price_id"]):
        raise RuntimeError("STRIPE_NOT_CONFIGURED: Sett STRIPE_SECRET_KEY og STRIPE_PRICE_ID")
    stripe.api_key = e["secret_key"]
    return e


# -----------------------------------------------------------------------------
# DB (SQLite)
# -----------------------------------------------------------------------------
def _db() -> sqlite3.Connection:
    p = pathlib.Path(DB_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    conn = _db()
    cur = conn.cursor()

    cur.execute("""
      CREATE TABLE IF NOT EXISTS api_keys (
        api_key TEXT PRIMARY KEY,
        email TEXT,
        status TEXT NOT NULL DEFAULT 'inactive',
        created_at TEXT NOT NULL,
        stripe_customer_id TEXT,
        stripe_subscription_id TEXT
      )
    """)

    cur.execute("""
      CREATE TABLE IF NOT EXISTS email_subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        api_key TEXT NOT NULL,
        email TEXT NOT NULL,
        created_at TEXT NOT NULL,
        last_notified_signal TEXT,
        UNIQUE(api_key, email)
      )
    """)

    cur.execute("""
      CREATE TABLE IF NOT EXISTS stripe_events (
        event_id TEXT PRIMARY KEY,
        event_type TEXT,
        created_at TEXT NOT NULL
      )
    """)

    conn.commit()
    conn.close()

@app.on_event("startup")
def _startup():
    init_db()

def is_valid_key(k: Optional[str]) -> bool:
    if not k:
        return False
    if k == ADMIN_API_KEY:
        return True
    conn = _db()
    row = conn.execute("SELECT api_key,status FROM api_keys WHERE api_key=?", (k,)).fetchone()
    conn.close()
    return bool(row) and (row["status"] == "active")

def _upsert_key_for_stripe(email: str, customer_id: str, subscription_id: str) -> str:
    conn = _db()
    row = conn.execute(
        "SELECT api_key FROM api_keys WHERE stripe_customer_id=? OR stripe_subscription_id=?",
        (customer_id, subscription_id),
    ).fetchone()

    if row:
        api_key = row["api_key"]
        conn.execute(
            "UPDATE api_keys SET email=?, stripe_customer_id=?, stripe_subscription_id=? WHERE api_key=?",
            (email or None, customer_id or None, subscription_id or None, api_key),
        )
        conn.commit()
        conn.close()
        return api_key

    api_key = "gb_" + secrets.token_urlsafe(24)
    conn.execute(
        "INSERT INTO api_keys(api_key,email,status,created_at,stripe_customer_id,stripe_subscription_id) VALUES(?,?,?,?,?,?)",
        (api_key, email or None, "inactive", iso_now(), customer_id or None, subscription_id or None),
    )
    conn.commit()
    conn.close()
    return api_key

def _set_key_status_for_customer(customer_id: str, status: str) -> None:
    conn = _db()
    conn.execute("UPDATE api_keys SET status=? WHERE stripe_customer_id=?", (status, customer_id))
    conn.commit()
    conn.close()

def _already_processed(event_id: str) -> bool:
    conn = _db()
    row = conn.execute("SELECT 1 FROM stripe_events WHERE event_id=?", (event_id,)).fetchone()
    conn.close()
    return bool(row)

def _mark_processed(event_id: str, event_type: str) -> None:
    conn = _db()
    conn.execute(
        "INSERT OR IGNORE INTO stripe_events(event_id, event_type, created_at) VALUES(?,?,?)",
        (event_id, event_type, iso_now()),
    )
    conn.commit()
    conn.close()


# -----------------------------------------------------------------------------
# Yahoo Finance
# -----------------------------------------------------------------------------
@dataclass
class YahooPrice:
    symbol: str
    last: float
    prev: float
    change_pct: Optional[float]
    currency: Optional[str]
    ts: str

def fetch_yahoo_chart(symbol: str, range_: str, interval: str) -> Dict[str, Any]:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Gullbrief/1.8)"}
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?range={range_}&interval={interval}"
    return http_get_json(url, headers=headers)

def extract_closes(chart_json: Dict[str, Any]) -> List[float]:
    try:
        result = chart_json["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        return [float(x) for x in closes if x is not None]
    except Exception:
        return []

def fetch_yahoo_price(symbol: str) -> YahooPrice:
    chart = fetch_yahoo_chart(symbol, range_="5d", interval="1d")
    closes = extract_closes(chart)
    if len(closes) < 2:
        raise RuntimeError(f"Yahoo: insufficient closes for {symbol}")
    last, prev = closes[-1], closes[-2]
    change_pct = ((last - prev) / prev) * 100.0 if prev else None
    currency = None
    try:
        currency = chart["chart"]["result"][0]["meta"].get("currency")
    except Exception:
        pass
    return YahooPrice(
        symbol=symbol,
        last=float(last),
        prev=float(prev),
        change_pct=float(change_pct) if change_pct is not None else None,
        currency=currency,
        ts=iso_now()
    )

def sma(values: List[float], n: int) -> Optional[float]:
    if len(values) < n:
        return None
    return sum(values[-n:]) / n

def compute_signal(symbol: str) -> Tuple[str, Dict[str, Any]]:
    chart = fetch_yahoo_chart(symbol, range_="3mo", interval="1d")
    closes = extract_closes(chart)
    if len(closes) < 55:
        return "neutral", {"reason": "For lite historikk til SMA20/SMA50. Setter nøytral."}
    last = closes[-1]
    s20, s50 = sma(closes, 20), sma(closes, 50)
    if s20 is None or s50 is None:
        return "neutral", {"reason": "Kunne ikke beregne glidende snitt."}
    if last > s20 > s50:
        return "bullish", {"reason": "Pris over SMA20 og SMA50, med positiv trend."}
    if last < s20 < s50:
        return "bearish", {"reason": "Pris under SMA20 og SMA50, med negativ trend."}
    return "neutral", {"reason": "Blandet bilde mellom pris og glidende snitt."}


# -----------------------------------------------------------------------------
# RSS headlines
# -----------------------------------------------------------------------------
def parse_rss(xml_text: str, fallback_source: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return items

    channel = root.find("channel")
    if channel is None:
        return items

    channel_title = (channel.findtext("title") or fallback_source).strip() or fallback_source
    for item in channel.findall("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        if title and link:
            items.append({"title": title, "link": link, "source": channel_title, "published": pub})
    return items

def fetch_headlines(limit: int = 10) -> List[Dict[str, str]]:
    if not RSS_FEEDS:
        return []
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Gullbrief/1.8)"}
    all_items: List[Dict[str, str]] = []
    for feed_url in RSS_FEEDS:
        try:
            xml_text = http_get_text(feed_url, headers=headers, timeout=20)
            all_items.extend(parse_rss(xml_text, fallback_source=domain_of(feed_url) or "RSS"))
        except Exception:
            continue
    seen, out = set(), []
    for it in all_items:
        lk = it.get("link", "")
        if lk and lk not in seen:
            seen.add(lk)
            out.append(it)
        if len(out) >= limit:
            break
    return out


# -----------------------------------------------------------------------------
# OpenAI summary (optional)
# -----------------------------------------------------------------------------
def summarize_with_openai(headlines: List[Dict[str, str]], signal_state: str, signal_reason: str) -> str:
    if not headlines or not OPENAI_API_KEY:
        return ""
    titles = [h.get("title", "").strip() for h in headlines if h.get("title")][:10]
    if not titles:
        return ""
    prompt = (
        "Du er Gullbrief Research. Skriv et kort, nøkternt makrosammendrag (5–7 linjer) "
        "om hva som kan påvirke gullprisen. Ingen hype. Ingen emojis. Ingen investeringsråd.\n\n"
        f"Signal akkurat nå: {signal_state.upper()}\n"
        f"Kort årsak (indikator): {signal_reason}\n\n"
        "Nyhetsoverskrifter:\n- " + "\n- ".join(titles) + "\n\nSammendrag:"
    )
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.responses.create(model=OPENAI_MODEL, input=prompt)
        return (resp.output_text or "").strip()
    except Exception:
        return ""


# -----------------------------------------------------------------------------
# Cache + brief
# -----------------------------------------------------------------------------
@dataclass
class CacheState:
    ts: float = 0.0
    data: Optional[Dict[str, Any]] = None

CACHE = CacheState()

def build_brief() -> Dict[str, Any]:
    yp = fetch_yahoo_price(YAHOO_SYMBOL)
    signal_state, sig_meta = compute_signal(YAHOO_SYMBOL)
    signal_reason = sig_meta.get("reason", "")
    headlines = fetch_headlines(limit=10)

    macro_ai = summarize_with_openai(headlines, signal_state, signal_reason)
    macro_summary = macro_ai or (" | ".join([h["title"] for h in headlines[:3] if h.get("title")]) or
                                 "Ingen nyheter tilgjengelig akkurat nå.")
    return {
        "updated_at": yp.ts,
        "version": "1.1",
        "symbol": yp.symbol,
        "currency": yp.currency,
        "price_usd": yp.last,
        "change_pct": yp.change_pct,
        "signal": signal_state,
        "signal_reason": signal_reason,
        "macro_summary": macro_summary,
        "headlines": headlines,
    }


# -----------------------------------------------------------------------------
# History (JSONL): signalendring + maks 1 per døgn
# -----------------------------------------------------------------------------
def _ensure_history_dir() -> pathlib.Path:
    p = pathlib.Path(HISTORY_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _dt(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _read_last_snapshot() -> Optional[Dict[str, Any]]:
    p = _ensure_history_dir()
    if not p.exists():
        return None
    try:
        with p.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            if size == 0:
                return None
            f.seek(max(0, size - 8192), 0)
            tail = f.read().decode("utf-8", errors="replace").splitlines()
            for line in reversed(tail):
                line = line.strip()
                if line:
                    return json.loads(line)
    except Exception:
        return None
    return None

def _should_store_snapshot(new_data: Dict[str, Any], last: Optional[Dict[str, Any]]) -> bool:
    if last is None:
        return True
    new_signal = (new_data.get("signal") or "").lower()
    last_signal = (last.get("signal") or "").lower()
    if new_signal and new_signal != last_signal:
        return True
    new_dt = _dt(new_data.get("updated_at", "")) or datetime.now(timezone.utc)
    last_dt = _dt(last.get("updated_at", "")) or datetime.now(timezone.utc)
    return new_dt.date() != last_dt.date()

def store_snapshot_if_needed(data: Dict[str, Any]) -> bool:
    p = _ensure_history_dir()
    last = _read_last_snapshot()
    if not _should_store_snapshot(data, last):
        return False
    rec = {
        "updated_at": data.get("updated_at") or iso_now(),
        "version": data.get("version", "1.1"),
        "symbol": data.get("symbol"),
        "price_usd": data.get("price_usd"),
        "change_pct": data.get("change_pct"),
        "signal": data.get("signal"),
        "signal_reason": data.get("signal_reason", ""),
        "macro_summary": data.get("macro_summary", ""),
        "headlines": data.get("headlines", []),
    }
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return True

def read_history(limit: int = 500) -> List[Dict[str, Any]]:
    p = _ensure_history_dir()
    if not p.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows[-limit:]

def add_forward_returns(rows: List[Dict[str, Any]], days_list=(7, 30)) -> List[Dict[str, Any]]:
    parsed: List[Tuple[Optional[datetime], Dict[str, Any]]] = [(_dt(r.get("updated_at", "")), r) for r in rows]
    for t, r in parsed:
        p0 = safe_float(r.get("price_usd"))
        if t is None or p0 is None or p0 == 0:
            for d in days_list:
                r[f"return_{d}d_pct"] = None
            continue
        for d in days_list:
            target = t + timedelta(days=d)
            p1 = None
            for tt, rr in parsed:
                if tt and tt >= target:
                    p1 = safe_float(rr.get("price_usd"))
                    break
            r[f"return_{d}d_pct"] = None if not p1 else ((p1 - p0) / p0) * 100.0
    return rows


# -----------------------------------------------------------------------------
# Cached brief getter
# -----------------------------------------------------------------------------
def get_cached_brief(force_refresh: bool) -> Dict[str, Any]:
    now = time.time()
    if (not force_refresh) and CACHE.data and (now - CACHE.ts) < CACHE_TTL_SECONDS:
        return CACHE.data
    data = build_brief()
    try:
        store_snapshot_if_needed(data)
    except Exception:
        pass
    CACHE.data = data
    CACHE.ts = now
    return data

def map_to_public_today(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "updated_at": data.get("updated_at") or iso_now(),
        "version": data.get("version", "1.1"),
        "gold": {"price_usd": data.get("price_usd"), "change_pct": data.get("change_pct")},
        "signal": {"state": data.get("signal", "neutral"), "reason_short": data.get("signal_reason", "")},
        "macro": {"summary_short": data.get("macro_summary", "")},
        "headlines": data.get("headlines", []),
    }


# -----------------------------------------------------------------------------
# Email alerts (SMTP)
# -----------------------------------------------------------------------------
def smtp_configured() -> bool:
    return bool(SMTP_HOST and SMTP_FROM)

def send_email(to_email: str, subject: str, body: str) -> None:
    if not smtp_configured():
        raise RuntimeError("SMTP not configured")
    msg = EmailMessage()
    msg["From"] = SMTP_FROM
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
        server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

def latest_signal() -> Optional[str]:
    rows = read_history(limit=3)
    if not rows:
        return None
    return (rows[-1].get("signal") or "").lower()


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/health")
def health() -> Dict[str, Any]:
    e = stripe_env()
    return {
        "status": "ok",
        "ts": iso_now(),
        "yahoo_symbol": YAHOO_SYMBOL,
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
        "openai_enabled": bool(OPENAI_API_KEY),
        "rss_feeds": RSS_FEEDS,
        "history_path": HISTORY_PATH,
        "db_path": DB_PATH,
        "admin_key_configured": ADMIN_API_KEY != "gullbrief-dev",
        "stripe_enabled": stripe_ready(),
        "stripe_secret_len": len(e["secret_key"]),
        "stripe_price_id_prefix": (e["price_id"][:10] + "...") if e["price_id"] else "",
        "stripe_webhook_secret_set": bool(e["webhook_secret"]),
        "smtp_enabled": smtp_configured(),
        "version": "1.8",
    }

@app.get("/api/debug/stripe")
def debug_stripe() -> Dict[str, Any]:
    e = stripe_env()
    return {
        "stripe_ready": stripe_ready(),
        "secret_len": len(e["secret_key"]),
        "price_id": e["price_id"],
        "success_url": e["success_url"],
        "cancel_url": e["cancel_url"],
        "webhook_secret_set": bool(e["webhook_secret"]),
    }

@app.get("/api/brief")
def api_brief():
    try:
        return get_cached_brief(force_refresh=False)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "BRIEF_FAILED", "message": str(e)})

@app.get("/api/brief/refresh")
def api_brief_refresh():
    try:
        return get_cached_brief(force_refresh=True)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "BRIEF_REFRESH_FAILED", "message": str(e)})

@app.get("/api/public/today")
def api_public_today():
    try:
        raw = get_cached_brief(force_refresh=False)
        return map_to_public_today(raw)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "PUBLIC_TODAY_FAILED", "message": str(e)})

@app.get("/api/public/teaser-history")
def api_teaser_history():
    rows = read_history(limit=50)
    rows = add_forward_returns(rows, days_list=(7, 30))
    items = rows[-3:]
    out = []
    for r in reversed(items):
        out.append({
            "updated_at": r.get("updated_at"),
            "symbol": r.get("symbol"),
            "price_usd": r.get("price_usd"),
            "signal": r.get("signal"),
            "macro_summary": (r.get("macro_summary") or "")[:120],
            "return_7d_pct": r.get("return_7d_pct"),
            "return_30d_pct": r.get("return_30d_pct"),
        })
    return {"count": len(out), "items": out}

@app.get("/api/history")
def api_history(limit: int = 200, x_api_key: str | None = Header(default=None)):
    if not is_valid_key(x_api_key):
        return JSONResponse(
            status_code=401,
            content={"error": "PREMIUM_REQUIRED", "message": "Historikk er kun for medlemmer."},
        )
    rows = read_history(limit=limit)
    rows = add_forward_returns(rows, days_list=(7, 30))
    return {"count": len(rows), "items": rows}

@app.post("/api/premium/subscribe-email")
async def api_subscribe_email(req: Request, x_api_key: str | None = Header(default=None)):
    if not is_valid_key(x_api_key):
        return JSONResponse(status_code=401, content={"error":"PREMIUM_REQUIRED","message":"Premium kreves."})
    body = await req.json()
    email = (body.get("email") or "").strip().lower()
    if "@" not in email:
        return JSONResponse(status_code=400, content={"error":"BAD_EMAIL","message":"Ugyldig e-post."})

    conn = _db()
    conn.execute(
        "INSERT OR IGNORE INTO email_subscriptions(api_key,email,created_at,last_notified_signal) VALUES(?,?,?,?)",
        (x_api_key, email, iso_now(), None),
    )
    conn.commit()
    conn.close()
    return {"ok": True, "email": email}

@app.get("/api/tasks/check-signal")
def api_check_signal(admin_key: str = ""):
    if admin_key != ADMIN_API_KEY:
        return JSONResponse(status_code=401, content={"error":"UNAUTHORIZED","message":"admin_key feil."})

    try:
        get_cached_brief(force_refresh=True)
    except Exception:
        pass

    sig = latest_signal()
    if not sig:
        return {"ok": True, "sent": 0, "note": "No history yet."}

    if not smtp_configured():
        return {"ok": False, "sent": 0, "error": "SMTP_NOT_CONFIGURED"}

    conn = _db()
    rows = conn.execute("SELECT id, api_key, email, last_notified_signal FROM email_subscriptions").fetchall()
    sent = 0

    for row in rows:
        last_sig = (row["last_notified_signal"] or "").lower()
        if last_sig == sig:
            continue

        to_email = row["email"]
        subject = f"Gullbrief: signal endret til {sig.upper()}"
        body = (
            f"Signalet i Gullbrief har endret seg.\n\n"
            f"Nytt signal: {sig.upper()}\n"
            f"Se arkiv: {ALERT_BASE_URL}/archive\n\n"
            f"(Dette er et automatisk varsel.)\n"
        )
        try:
            send_email(to_email, subject, body)
            conn.execute("UPDATE email_subscriptions SET last_notified_signal=? WHERE id=?", (sig, row["id"]))
            conn.commit()
            sent += 1
        except Exception:
            continue

    conn.close()
    return {"ok": True, "sent": sent, "signal": sig}


# -----------------------------------------------------------------------------
# Stripe: opprett checkout-session
# -----------------------------------------------------------------------------
@app.post("/api/stripe/create-checkout")
async def api_stripe_create_checkout(req: Request):
    try:
        e = require_stripe()
    except Exception as ex:
        return JSONResponse(status_code=400, content={"error": "STRIPE_SETUP", "message": str(ex)})

    try:
        body = await req.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "BAD_JSON", "message": "Body må være gyldig JSON."})

    email = (body.get("email") or "").strip().lower()

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": e["price_id"], "quantity": 1}],
            success_url=f"{e['success_url']}?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=e["cancel_url"],
            customer_email=email if email else None,
        )
        return {"url": session.url}
    except Exception as ex:
        return JSONResponse(status_code=400, content={"error": "STRIPE_CREATE_CHECKOUT_FAILED", "message": str(ex)})


# -----------------------------------------------------------------------------
# Success-side
# -----------------------------------------------------------------------------
@app.get("/success", response_class=HTMLResponse)
def success_page(session_id: str = ""):
    return HTMLResponse(SUCCESS_HTML.replace("__SESSION_ID__", session_id or ""))


# -----------------------------------------------------------------------------
# Claim key (polling)
# -----------------------------------------------------------------------------
@app.get("/api/stripe/claim-key")
def api_stripe_claim_key(session_id: str = ""):
    try:
        require_stripe()
    except Exception as ex:
        return JSONResponse(status_code=400, content={"error": "STRIPE_SETUP", "message": str(ex)})

    if not session_id:
        return JSONResponse(status_code=400, content={"error": "MISSING_SESSION_ID", "message": "session_id mangler."})

    try:
        session = stripe.checkout.Session.retrieve(session_id)
        if session.get("status") != "complete":
            return JSONResponse(status_code=409, content={"error":"NOT_COMPLETE","message":"Checkout ikke complete."})

        customer = str(session.get("customer") or "")
        subscription = str(session.get("subscription") or "")
        email = (session.get("customer_details") or {}).get("email") or session.get("customer_email") or ""
        email = (email or "").strip().lower()

        if not customer or not subscription:
            return JSONResponse(status_code=400, content={"error":"MISSING_STRIPE_IDS","message":"Mangler customer/subscription i session."})

        api_key = _upsert_key_for_stripe(email=email, customer_id=customer, subscription_id=subscription)

        conn = _db()
        row = conn.execute("SELECT api_key, status FROM api_keys WHERE api_key=?", (api_key,)).fetchone()
        conn.close()

        if not row or row["status"] != "active":
            return JSONResponse(status_code=409, content={"error":"NOT_ACTIVE","message":"Abonnementet er ikke aktivert (venter på betaling/webhook)."})

        return {"api_key": row["api_key"], "email": email}
    except Exception as ex:
        return JSONResponse(status_code=400, content={"error":"STRIPE_CLAIM_FAILED","message":str(ex)})


# -----------------------------------------------------------------------------
# Stripe webhook (source of truth)
# -----------------------------------------------------------------------------
@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("Stripe-Signature")
    e = stripe_env()

    if not e["webhook_secret"]:
        return JSONResponse(status_code=500, content={"error": "Missing STRIPE_WEBHOOK_SECRET"})

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, e["webhook_secret"])
    except stripe.error.SignatureVerificationError:
        return JSONResponse(status_code=400, content={"error": "Invalid signature"})
    except Exception as ex:
        return JSONResponse(status_code=400, content={"error": "Webhook error", "message": str(ex)})

    event_id = event.get("id", "")
    event_type = event.get("type", "unknown")

    if event_id:
        if _already_processed(event_id):
            return JSONResponse(status_code=200, content={"status": "duplicate_ignored"})
        _mark_processed(event_id, event_type)

    obj = (event.get("data") or {}).get("object") or {}

    try:
        if event_type == "checkout.session.completed":
            customer = str(obj.get("customer") or "")
            subscription = str(obj.get("subscription") or "")
            email = (obj.get("customer_details") or {}).get("email") or obj.get("customer_email") or ""
            email = (email or "").strip().lower()

            if customer and subscription:
                _upsert_key_for_stripe(email=email, customer_id=customer, subscription_id=subscription)

                # Smooth: activate immediately if Stripe says it's paid
                payment_status = (obj.get("payment_status") or "").strip().lower()
                if payment_status == "paid":
                    _set_key_status_for_customer(customer, "active")

        elif event_type == "invoice.paid":
            customer = str(obj.get("customer") or "")
            subscription = str(obj.get("subscription") or "")
            if customer:
                # ensure key exists, then activate
                conn = _db()
                row = conn.execute("SELECT email FROM api_keys WHERE stripe_customer_id=?", (customer,)).fetchone()
                conn.close()
                email = (row["email"] if row else "") or ""
                if subscription:
                    _upsert_key_for_stripe(email=email, customer_id=customer, subscription_id=subscription)
                _set_key_status_for_customer(customer, "active")

        elif event_type == "invoice.payment_failed":
            customer = str(obj.get("customer") or "")
            if customer:
                _set_key_status_for_customer(customer, "inactive")

        elif event_type == "customer.subscription.deleted":
            customer = str(obj.get("customer") or "")
            if customer:
                _set_key_status_for_customer(customer, "inactive")

        elif event_type == "customer.subscription.updated":
            customer = str(obj.get("customer") or "")
            status = (obj.get("status") or "").strip().lower()
            if customer:
                _set_key_status_for_customer(customer, "active" if status in ("active", "trialing") else "inactive")

    except Exception as ex:
        return JSONResponse(status_code=500, content={"error": "WEBHOOK_HANDLER_FAILED", "message": str(ex)})

    return JSONResponse(status_code=200, content={"status": "ok", "type": event_type})


# -----------------------------------------------------------------------------
# Pages (UI)
# -----------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)

@app.get("/archive", response_class=HTMLResponse)
def archive() -> HTMLResponse:
    return HTMLResponse(ARCHIVE_HTML)


# -----------------------------------------------------------------------------
# HTML templates
# -----------------------------------------------------------------------------
INDEX_HTML = """<!doctype html>
<html lang="no">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Gullbrief</title>
  <style>
    :root{--bg:#0f1720;--card:#16212c;--text:#e5e7eb;--muted:#9aa3af;--gold:#d4af37;--ok:#34d399;--err:#fb7185;--max:920px;--r:14px;}
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(1200px 800px at 20% 10%,#142234 0%,var(--bg) 55%) no-repeat;color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;line-height:1.45;}
    a{color:var(--text);text-decoration:none} a:hover{text-decoration:underline}
    .wrap{max-width:var(--max);margin:0 auto;padding:28px 18px 64px}
    header{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:8px 0 20px}
    .brand{font-weight:700}
    .nav{display:flex;gap:14px;align-items:center;color:var(--muted);font-size:14px}
    .nav a{color:var(--muted)}
    .cta{background:var(--gold);color:#0b0f14;padding:10px 14px;border-radius:999px;font-weight:750}
    .hero h1{margin:10px 0 8px;font-size:36px;font-family:ui-serif,Georgia,Times}
    .hero p{margin:0;color:var(--muted);font-size:18px;max-width:70ch}
    .grid{display:grid;grid-template-columns:1fr;gap:14px;margin-top:18px}
    @media (min-width:880px){.grid{grid-template-columns:1.2fr .8fr}}
    .card{background:rgba(22,33,44,.92);border:1px solid rgba(255,255,255,.06);border-radius:var(--r);padding:18px}
    .title{display:flex;justify-content:space-between;gap:10px;align-items:baseline}
    .title h2{margin:0;font-size:16px;color:var(--muted);font-weight:650}
    .big{font-size:34px;font-weight:800;margin:8px 0 0}
    .sub{color:var(--muted);margin-top:2px}
    .pill{display:inline-flex;align-items:center;gap:8px;padding:7px 10px;border-radius:999px;background:rgba(255,255,255,.06);font-weight:750;margin-top:10px}
    .pill .dot{width:9px;height:9px;border-radius:99px;background:var(--muted)}
    .pill.bullish .dot{background:var(--ok)} .pill.bearish .dot{background:var(--err)} .pill.neutral .dot{background:var(--gold)}
    .muted{color:var(--muted)} ul{margin:10px 0 0;padding:0 0 0 16px} li{margin:10px 0}
    .btnrow{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px}
    button{border:0;border-radius:12px;padding:10px 12px;font-weight:750;cursor:pointer;background:rgba(255,255,255,.08);color:var(--text)}
    button:hover{background:rgba(255,255,255,.12)}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="brand">Gullbrief</div>
      <div class="nav">
        <a href="/">Analyse</a>
        <a href="/archive">Arkiv</a>
        <a class="cta" href="/archive">Premium</a>
      </div>
    </header>

    <section class="hero">
      <h1>GC=F, signal, makro. Pent og enkelt.</h1>
      <p>Gratis: dagens pris og kort kontekst. Premium: arkiv, signalhistorikk, performance og varsler.</p>
    </section>

    <section class="grid">
      <div class="card">
        <div class="title"><h2>Dagens status</h2><div class="muted" id="updatedAt">Oppdaterer…</div></div>
        <div class="big" id="price">$–</div>
        <div class="sub" id="change">–</div>
        <div class="pill neutral" id="signalPill"><span class="dot"></span><span id="signalText">Signal: –</span></div>
        <p class="muted" style="margin-top:12px" id="reason">–</p>
        <h2 style="margin-top:14px">Makro i dag</h2>
        <p class="muted" id="macro">–</p>
        <div class="btnrow">
          <button id="btnReload">Oppdater</button>
          <button id="btnRefresh">Hard refresh</button>
          <button onclick="location.href='/archive'">Åpne arkiv</button>
        </div>
        <div class="muted" id="status" style="margin-top:8px">Status: …</div>
      </div>

      <div class="card">
        <div class="title"><h2>Relevante nyheter</h2><div class="muted">Gratis</div></div>
        <ul id="headlines"></ul>
      </div>
    </section>
  </div>

<script>
  const $ = (id) => document.getElementById(id);
  const fmtPct = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ((Number(x)>0?"+":"") + Number(x).toFixed(2) + "%");
  const fmtPrice = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ("$" + Number(x).toLocaleString(undefined,{maximumFractionDigits:2}));
  const pillClass = (s) => (s||"").toLowerCase().includes("bull") ? "bullish" : ((s||"").toLowerCase().includes("bear") ? "bearish" : "neutral");

  async function loadToday(){
    try{
      $("status").textContent = "Status: Laster…";
      const res = await fetch("/api/public/today", {cache:"no-store"});
      const data = await res.json();
      if(!res.ok) throw new Error(data?.message || ("HTTP " + res.status));
      $("updatedAt").textContent = "Oppdatert: " + (data.updated_at || "–");
      $("price").textContent = fmtPrice(data?.gold?.price_usd);
      $("change").textContent = "Endring: " + fmtPct(data?.gold?.change_pct);
      const state = data?.signal?.state || "neutral";
      $("signalText").textContent = "Signal: " + state;
      $("signalPill").className = "pill " + pillClass(state);
      $("reason").textContent = data?.signal?.reason_short || "–";
      $("macro").textContent = data?.macro?.summary_short || "–";

      const ul = $("headlines"); ul.innerHTML = "";
      (data.headlines||[]).forEach(h=>{
        const li=document.createElement("li");
        const a=document.createElement("a");
        a.href=h.link; a.target="_blank"; a.rel="noopener noreferrer";
        a.textContent=h.title || "(uten tittel)";
        const d=document.createElement("div"); d.className="muted";
        d.textContent=(h.source||"Kilde") + (h.published?(" | "+h.published):"");
        li.appendChild(a); li.appendChild(d); ul.appendChild(li);
      });
      $("status").textContent = "Status: OK";
    }catch(e){
      $("status").textContent = "Status: Feil: " + e;
    }
  }

  $("btnReload").addEventListener("click", loadToday);
  $("btnRefresh").addEventListener("click", async () => {
    await fetch("/api/brief/refresh", {cache:"no-store"}).catch(()=>{});
    loadToday();
  });

  loadToday();
</script>
</body>
</html>
"""

ARCHIVE_HTML = """<!doctype html>
<html lang="no">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Gullbrief Arkiv</title>
  <style>
    :root{--bg:#0f1720;--card:#16212c;--text:#e5e7eb;--muted:#9aa3af;--gold:#d4af37;--ok:#34d399;--err:#fb7185;--max:980px;--r:14px;}
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(1200px 800px at 20% 10%,#142234 0%,var(--bg) 55%) no-repeat;color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;line-height:1.45;}
    a{color:var(--text);text-decoration:none} a:hover{text-decoration:underline}
    .wrap{max-width:var(--max);margin:0 auto;padding:28px 18px 64px}
    header{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:8px 0 20px}
    .brand{font-weight:800}
    .nav{display:flex;gap:14px;align-items:center;color:var(--muted);font-size:14px}
    .nav a{color:var(--muted)}
    .cta{background:var(--gold);color:#0b0f14;padding:10px 14px;border-radius:999px;font-weight:800}
    .card{background:rgba(22,33,44,.92);border:1px solid rgba(255,255,255,.06);border-radius:var(--r);padding:18px}
    .row{display:flex;gap:12px;flex-wrap:wrap;align-items:center}
    input{width:min(520px,100%);padding:10px 12px;border-radius:12px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.06);color:var(--text);outline:none}
    button{border:0;border-radius:12px;padding:10px 12px;font-weight:800;cursor:pointer;background:rgba(255,255,255,.08);color:var(--text)}
    button:hover{background:rgba(255,255,255,.12)}
    .btn-primary{background:var(--gold);color:#0b0f14}
    .muted{color:var(--muted)}
    .pill{display:inline-flex;align-items:center;gap:8px;padding:6px 10px;border-radius:999px;background:rgba(255,255,255,.06);font-weight:800}
    .dot{width:9px;height:9px;border-radius:99px;background:var(--muted)}
    .pill.bullish .dot{background:var(--ok)} .pill.bearish .dot{background:var(--err)} .pill.neutral .dot{background:var(--gold)}
    table{width:100%;border-collapse:collapse;margin-top:14px}
    th,td{padding:10px 8px;border-bottom:1px solid rgba(255,255,255,.06);text-align:left;vertical-align:top}
    th{color:var(--muted);font-weight:800;font-size:13px}
    td{font-size:14px}
    .small{font-size:12px;color:var(--muted)}
    code{background:rgba(255,255,255,.07);padding:2px 6px;border-radius:8px}
    .split{display:grid;grid-template-columns:1fr;gap:14px}
    @media (min-width: 920px){.split{grid-template-columns:1fr 1fr}}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="brand">Gullbrief Arkiv</div>
      <div class="nav">
        <a href="/">Til analyse</a>
        <a class="cta" href="/">Gullbrief</a>
      </div>
    </header>

    <div class="split">
      <div class="card">
        <div style="font-size:18px;font-weight:900">Teaser (gratis)</div>
        <div class="muted">Siste 3 snapshots. Full historikk ligger bak premium.</div>
        <div id="teaserStatus" class="small" style="margin-top:10px"></div>
        <table id="teaserTbl" style="display:none">
          <thead><tr><th>Dato</th><th>Pris</th><th>Signal</th><th>7d</th><th>30d</th><th>Notat</th></tr></thead>
          <tbody id="teaserBody"></tbody>
        </table>
      </div>

      <div class="card">
        <div style="font-size:18px;font-weight:900">Premium</div>
        <div class="muted">Lim inn premium-nøkkel. Den lagres lokalt i nettleseren (localStorage).</div>

        <div class="row" style="margin-top:12px">
          <input id="key" placeholder="Premium-nøkkel" autocomplete="off" />
          <button class="btn-primary" id="btnSave">Lagre</button>
          <button id="btnClear">Fjern</button>
          <button id="btnLoad">Last arkiv</button>
        </div>

        <div class="row" style="margin-top:10px">
          <input id="email" placeholder="E-post for varsel (premium)" autocomplete="email" />
          <button id="btnEmail">Aktiver e-postvarsel</button>
        </div>

        <div class="row" style="margin-top:12px">
          <input id="payEmail" placeholder="E-post for kjøp (Stripe)" autocomplete="email" />
          <button class="btn-primary" id="btnPay">Kjøp premium</button>
        </div>

        <div id="status" class="small" style="margin-top:10px"></div>

        <table id="tbl" style="display:none">
          <thead><tr><th>Dato</th><th>Pris</th><th>Signal</th><th>7d</th><th>30d</th><th>Notat</th></tr></thead>
          <tbody id="body"></tbody>
        </table>

        <div class="small" style="margin-top:10px">
          API: <code>/api/history</code> med header <code>x-api-key</code>.
        </div>
      </div>
    </div>
  </div>

<script>
  const LS_KEY = "gullbrief_premium_key";
  const $ = (id) => document.getElementById(id);

  const pillClass = (s) => (s||"").toLowerCase().includes("bull") ? "bullish" : ((s||"").toLowerCase().includes("bear") ? "bearish" : "neutral");
  const fmtPct = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ((Number(x)>0?"+":"") + Number(x).toFixed(2) + "%");
  const fmtPrice = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ("$" + Number(x).toLocaleString(undefined,{maximumFractionDigits:2}));

  function setStatus(msg){ $("status").textContent = msg; }
  function setTeaser(msg){ $("teaserStatus").textContent = msg; }

  function loadSavedKey(){
    $("key").value = localStorage.getItem(LS_KEY) || "";
  }

  async function loadTeaser(){
    try{
      setTeaser("Laster…");
      const res = await fetch("/api/public/teaser-history", {cache:"no-store"});
      const data = await res.json();
      const items = data.items || [];
      $("teaserBody").innerHTML = "";
      if(items.length === 0){
        $("teaserTbl").style.display = "none";
        setTeaser("Ingen snapshots ennå. (Kjør /api/brief/refresh i dag og i morgen.)");
        return;
      }
      items.forEach(r=>{
        const tr=document.createElement("tr");
        const sig=r.signal||"neutral";
        tr.innerHTML = `
          <td><strong>${r.updated_at||""}</strong><div class="small">${r.symbol||""}</div></td>
          <td>${fmtPrice(r.price_usd)}</td>
          <td><span class="pill ${pillClass(sig)}"><span class="dot"></span>${sig}</span></td>
          <td>${fmtPct(r.return_7d_pct)}</td>
          <td>${fmtPct(r.return_30d_pct)}</td>
          <td class="small">${(r.macro_summary||"").slice(0,140)}</td>
        `;
        $("teaserBody").appendChild(tr);
      });
      $("teaserTbl").style.display = "";
      setTeaser(`OK: viser ${items.length} snapshots.`);
    }catch(e){
      setTeaser("Feil: " + e);
    }
  }

  async function loadArchive(){
    const k = $("key").value.trim();
    if(!k){ setStatus("Legg inn premium-nøkkel først."); return; }
    setStatus("Laster…");
    $("tbl").style.display="none";
    $("body").innerHTML="";
    try{
      const res = await fetch("/api/history?limit=200", {headers:{"x-api-key":k}, cache:"no-store"});
      const data = await res.json();
      if(!res.ok){ setStatus(data?.message || ("HTTP "+res.status)); return; }
      const items = (data.items||[]).slice().reverse();
      items.forEach(r=>{
        const tr=document.createElement("tr");
        const sig=r.signal||"neutral";
        tr.innerHTML = `
          <td><strong>${r.updated_at||""}</strong><div class="small">${r.symbol||""}</div></td>
          <td>${fmtPrice(r.price_usd)}</td>
          <td><span class="pill ${pillClass(sig)}"><span class="dot"></span>${sig}</span><div class="small">${(r.signal_reason||"").slice(0,80)}</div></td>
          <td>${fmtPct(r.return_7d_pct)}</td>
          <td>${fmtPct(r.return_30d_pct)}</td>
          <td class="small">${(r.macro_summary||"").slice(0,140)}</td>
        `;
        $("body").appendChild(tr);
      });
      $("tbl").style.display="";
      setStatus(`OK: ${data.count} snapshots (viser ${items.length}).`);
    }catch(e){
      setStatus("Feil: " + e);
    }
  }

  async function subscribeEmail(){
    const k = $("key").value.trim();
    const email = $("email").value.trim();
    if(!k){ setStatus("Legg inn premium-nøkkel først."); return; }
    if(!email.includes("@")){ setStatus("Skriv inn gyldig e-post."); return; }
    try{
      setStatus("Lagrer e-post…");
      const res = await fetch("/api/premium/subscribe-email", {
        method:"POST",
        headers: {"Content-Type":"application/json", "x-api-key": k},
        body: JSON.stringify({email})
      });
      const data = await res.json();
      if(!res.ok){ setStatus(data?.message || ("HTTP "+res.status)); return; }
      setStatus("E-postvarsel aktivert ✅ (sendes ved signalendring)");
    }catch(e){
      setStatus("Feil: " + e);
    }
  }

  async function startCheckout(){
    const email = $("payEmail").value.trim();
    if(!email.includes("@")){ setStatus("Skriv inn gyldig e-post for kjøp."); return; }
    try{
      setStatus("Åpner Stripe checkout…");
      const res = await fetch("/api/stripe/create-checkout",{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body: JSON.stringify({email})
      });
      const data = await res.json();
      if(!res.ok){ setStatus(data?.message || ("HTTP "+res.status)); return; }
      location.href = data.url;
    }catch(e){
      setStatus("Feil: " + e);
    }
  }

  $("btnSave").addEventListener("click", ()=>{
    localStorage.setItem(LS_KEY, $("key").value.trim());
    setStatus("Nøkkel lagret lokalt ✅");
  });
  $("btnClear").addEventListener("click", ()=>{
    localStorage.removeItem(LS_KEY);
    $("key").value="";
    setStatus("Nøkkel fjernet.");
    $("tbl").style.display="none";
    $("body").innerHTML="";
  });
  $("btnLoad").addEventListener("click", loadArchive);
  $("btnEmail").addEventListener("click", subscribeEmail);
  $("btnPay").addEventListener("click", startCheckout);

  loadSavedKey();
  loadTeaser();
  if($("key").value.trim()){ loadArchive(); }
</script>
</body>
</html>
"""

SUCCESS_HTML = """<!doctype html>
<html lang="no">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Gullbrief - Success</title>
  <style>
    body{margin:0;padding:30px;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;background:#0f1720;color:#e5e7eb}
    .card{max-width:780px;margin:0 auto;background:#16212c;border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:18px}
    code{background:rgba(255,255,255,.07);padding:2px 6px;border-radius:8px}
    button{border:0;border-radius:12px;padding:10px 12px;font-weight:800;cursor:pointer;background:#d4af37;color:#0b0f14}
  </style>
</head>
<body>
  <div class="card">
    <h2>Betaling registrert</h2>
    <p>Vi henter premium-nøkkelen din nå.</p>
    <p id="status">Laster…</p>
    <p><strong>Premium-nøkkel:</strong> <code id="key">–</code></p>
    <button id="btn">Åpne arkiv</button>
  </div>

<script>
  const sessionId = "__SESSION_ID__";
  const statusEl = document.getElementById("status");
  const keyEl = document.getElementById("key");
  document.getElementById("btn").addEventListener("click", ()=>{
    if(keyEl.textContent && keyEl.textContent !== "–"){
      localStorage.setItem("gullbrief_premium_key", keyEl.textContent);
    }
    location.href="/archive";
  });

  async function loadKey(){
    try{
      const res = await fetch("/api/stripe/claim-key?session_id=" + encodeURIComponent(sessionId), {cache:"no-store"});
      const data = await res.json();
      if(!res.ok) throw new Error(data?.message || ("HTTP "+res.status));
      keyEl.textContent = data.api_key || "–";
      statusEl.textContent = "OK. Nøkkel klar.";
    }catch(e){
      statusEl.textContent = "Venter på bekreftelse fra Stripe (webhook)… " + e;
      setTimeout(loadKey, 1200);
    }
  }
  if(sessionId){ loadKey(); } else { statusEl.textContent = "Mangler session_id."; }
</script>
</body>
</html>
"""