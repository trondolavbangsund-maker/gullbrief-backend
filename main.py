from __future__ import annotations

import os
import time
import json
import math
import pathlib
import secrets
import sqlite3
import traceback
import socket
import urllib.request
import xml.etree.ElementTree as ET

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import stripe  # type: ignore

from fastapi import FastAPI, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse


# =============================================================================
# Config
# =============================================================================

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()

YAHOO_SYMBOL = os.getenv("YAHOO_SYMBOL", "GC=F").strip()
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "900"))

RSS_FEEDS_ENV = os.getenv("RSS_FEEDS", "https://www.fxstreet.com/rss/news")
RSS_FEEDS = [u.strip() for u in RSS_FEEDS_ENV.split(",") if u.strip()]

HISTORY_PATH = os.getenv("HISTORY_PATH", "data/history.jsonl").strip()

# Admin/dev key (alltid gyldig). Sett i Render env (PREMIUM_API_KEY).
ADMIN_API_KEY = os.getenv("PREMIUM_API_KEY", "gullbrief-dev").strip()

DB_PATH = os.getenv("DB_PATH", "data/app.db").strip()
ALERT_BASE_URL = os.getenv("ALERT_BASE_URL", "http://127.0.0.1:8000").strip()

# Brevo Transactional API
BREVO_API_KEY = os.getenv("BREVO_API_KEY", "").strip()
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "").strip()
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "Gullbrief").strip()

# Stripe defaults (read dynamically too)
STRIPE_SUCCESS_URL_DEFAULT = os.getenv("STRIPE_SUCCESS_URL", "http://127.0.0.1:8000/success").strip()
STRIPE_CANCEL_URL_DEFAULT = os.getenv("STRIPE_CANCEL_URL", "http://127.0.0.1:8000/premium").strip()


# =============================================================================
# App + CORS
# =============================================================================

app = FastAPI(title="Gullbrief Research", version="2.1")

origins_env = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
allow_origins = [o.strip() for o in origins_env.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Utils
# =============================================================================

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


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""


def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 25) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))


def http_get_text(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 25) -> str:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("latin-1", errors="replace")


# =============================================================================
# Stripe helpers (read env dynamically)
# =============================================================================

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


# =============================================================================
# DB (SQLite)
# =============================================================================

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
        last_daily_sent_date TEXT,
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
def _startup() -> None:
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


# =============================================================================
# Yahoo Finance + indicators
# =============================================================================

@dataclass
class YahooPrice:
    symbol: str
    last: float
    prev: float
    change_pct: Optional[float]
    currency: Optional[str]
    ts: str


def fetch_yahoo_chart(symbol: str, range_: str, interval: str) -> Dict[str, Any]:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Gullbrief/2.1)"}
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
    return YahooPrice(symbol=symbol, last=float(last), prev=float(prev), change_pct=change_pct, currency=currency, ts=iso_now())


def sma(values: List[float], n: int) -> Optional[float]:
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        diff = values[i] - values[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses += -diff
    if gains == 0 and losses == 0:
        return 50.0
    if losses == 0:
        return 100.0
    rs = gains / losses
    return 100.0 - (100.0 / (1.0 + rs))


def trend_score_from_mas(last: float, s20: Optional[float], s50: Optional[float]) -> Optional[int]:
    if s20 is None or s50 is None:
        return None
    score = 50
    score += 15 if last > s20 else -15
    score += 20 if s20 > s50 else -20
    return max(0, min(100, int(score)))


def compute_signal(symbol: str) -> Tuple[str, Dict[str, Any]]:
    chart = fetch_yahoo_chart(symbol, range_="3mo", interval="1d")
    closes = extract_closes(chart)
    if len(closes) < 55:
        return "neutral", {"reason": "For lite historikk til SMA20/SMA50. Setter nøytral.", "rsi14": None, "trend_score": None}

    last = closes[-1]
    s20, s50 = sma(closes, 20), sma(closes, 50)
    rsi14 = rsi(closes, 14)
    tscore = trend_score_from_mas(last, s20, s50)

    if s20 is None or s50 is None:
        return "neutral", {"reason": "Kunne ikke beregne glidende snitt.", "rsi14": rsi14, "trend_score": tscore}

    if last > s20 > s50:
        return "bullish", {"reason": "Pris over SMA20 og SMA50, med positiv trend.", "rsi14": rsi14, "trend_score": tscore}
    if last < s20 < s50:
        return "bearish", {"reason": "Pris under SMA20 og SMA50, med negativ trend.", "rsi14": rsi14, "trend_score": tscore}
    return "neutral", {"reason": "Blandet bilde mellom pris og glidende snitt.", "rsi14": rsi14, "trend_score": tscore}


# =============================================================================
# RSS headlines
# =============================================================================

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
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Gullbrief/2.1)"}
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


# =============================================================================
# OpenAI summaries (optional)
# =============================================================================

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


def premium_report_ai(
    headlines: List[Dict[str, str]],
    signal_state: str,
    signal_reason: str,
    price_usd: Optional[float],
    change_pct: Optional[float],
    rsi14: Optional[float],
    trend_score: Optional[int],
) -> str:
    titles = [h.get("title", "").strip() for h in headlines if h.get("title")][:12]
    if not titles:
        titles = ["(Ingen nyhetsoverskrifter tilgjengelig)"]

    if not OPENAI_API_KEY:
        bits = []
        if isinstance(price_usd, (int, float)):
            bits.append(f"Pris: {price_usd:.2f} USD")
        if isinstance(change_pct, (int, float)):
            bits.append(f"Døgnendring: {change_pct:+.2f}%")
        if isinstance(rsi14, (int, float)):
            bits.append(f"RSI(14): {rsi14:.1f}")
        if isinstance(trend_score, int):
            bits.append(f"Trend score: {trend_score}/100")
        header = " | ".join(bits) if bits else "Dagens nøkkeltall: (ukjent)"
        return (
            f"Gullbrief Premium ({datetime.now(timezone.utc).date().isoformat()})\n"
            f"{header}\n"
            f"Signal: {signal_state.upper()} ({signal_reason})\n\n"
            "Nyhetsdriver (utdrag):\n- " + "\n- ".join(titles[:6]) + "\n\n"
            "Hva kan endre bildet (24–72t):\n"
            "- USD/renter beveger seg raskt\n"
            "- Inflasjon-/vekstdata overrasker\n"
            "- Geopolitikk/risikoappetitt skifter\n"
        )

    price_line = f"Pris: {price_usd:.2f} USD" if isinstance(price_usd, (int, float)) else "Pris: (ukjent)"
    chg_line = f"Døgnendring: {change_pct:+.2f}%" if isinstance(change_pct, (int, float)) else "Døgnendring: (ukjent)"
    rsi_line = f"RSI(14): {rsi14:.1f}" if isinstance(rsi14, (int, float)) else "RSI(14): (ukjent)"
    ts_line = f"Trend score: {trend_score}/100" if isinstance(trend_score, int) else "Trend score: (ukjent)"

    prompt = (
        "Du er Gullbrief Premium. Skriv en daglig makrokommentar om gull (GC=F) på norsk.\n"
        "Krav:\n"
        "- 10–16 linjer, korte avsnitt, svært nøkternt.\n"
        "- Fokus på makro: renter, USD, inflasjon, vekst, geopolitikk, posisjonering.\n"
        "- Ikke investeringsråd. Ikke oppfordring til kjøp/salg.\n"
        "- Ikke bruk emojis.\n"
        "- Avslutt med 'Hva kan endre bildet (24–72t)' med 3 punkt.\n\n"
        f"{price_line}\n{chg_line}\n{rsi_line}\n{ts_line}\n"
        f"Signal: {signal_state.upper()}\n"
        f"Indikator-årsak: {signal_reason}\n\n"
        "Nyhetsoverskrifter:\n- " + "\n- ".join(titles) + "\n\n"
        "Svarformat:\n"
        "Tittel: (1 linje)\n"
        "Makro: (6–10 linjer)\n"
        "Hva kan endre bildet (24–72t):\n"
        "- ...\n- ...\n- ...\n"
    )

    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.responses.create(model=OPENAI_MODEL, input=prompt)
        return (resp.output_text or "").strip()
    except Exception:
        return premium_report_ai(
            headlines=headlines,
            signal_state=signal_state,
            signal_reason=signal_reason,
            price_usd=price_usd,
            change_pct=change_pct,
            rsi14=rsi14,
            trend_score=trend_score,
        )


# =============================================================================
# Cache + brief
# =============================================================================

@dataclass
class CacheState:
    ts: float = 0.0
    data: Optional[Dict[str, Any]] = None


CACHE = CacheState()


def build_brief() -> Dict[str, Any]:
    yp = fetch_yahoo_price(YAHOO_SYMBOL)
    signal_state, sig_meta = compute_signal(YAHOO_SYMBOL)
    signal_reason = sig_meta.get("reason", "")
    rsi14v = safe_float(sig_meta.get("rsi14"))
    tscore = sig_meta.get("trend_score") if isinstance(sig_meta.get("trend_score"), int) else None

    headlines = fetch_headlines(limit=10)

    macro_ai = summarize_with_openai(headlines, signal_state, signal_reason)
    macro_summary = macro_ai or (" | ".join([h["title"] for h in headlines[:3] if h.get("title")]) or "Ingen nyheter tilgjengelig akkurat nå.")

    return {
        "updated_at": yp.ts,
        "version": "2.1",
        "symbol": yp.symbol,
        "currency": yp.currency,
        "price_usd": yp.last,
        "change_pct": yp.change_pct,
        "signal": signal_state,
        "signal_reason": signal_reason,
        "rsi14": rsi14v,
        "trend_score": tscore,
        "macro_summary": macro_summary,
        "headlines": headlines,
    }


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
        "version": data.get("version", "2.1"),
        "gold": {"price_usd": data.get("price_usd"), "change_pct": data.get("change_pct")},
        "signal": {"state": data.get("signal", "neutral"), "reason_short": data.get("signal_reason", "")},
        "macro": {"summary_short": data.get("macro_summary", "")},
        "headlines": data.get("headlines", []),
    }


# =============================================================================
# History (JSONL)
# =============================================================================

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

    premium_report = premium_report_ai(
        headlines=data.get("headlines", []),
        signal_state=str(data.get("signal") or "neutral"),
        signal_reason=str(data.get("signal_reason") or ""),
        price_usd=safe_float(data.get("price_usd")),
        change_pct=safe_float(data.get("change_pct")),
        rsi14=safe_float(data.get("rsi14")),
        trend_score=data.get("trend_score") if isinstance(data.get("trend_score"), int) else None,
    )

    rec = {
        "updated_at": data.get("updated_at") or iso_now(),
        "version": data.get("version", "2.1"),
        "symbol": data.get("symbol"),
        "price_usd": data.get("price_usd"),
        "change_pct": data.get("change_pct"),
        "signal": data.get("signal"),
        "signal_reason": data.get("signal_reason", ""),
        "rsi14": data.get("rsi14"),
        "trend_score": data.get("trend_score"),
        "macro_summary": data.get("macro_summary", ""),
        "premium_report": premium_report or "",
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


def latest_signal() -> str:
    last = _read_last_snapshot()
    return (last.get("signal") if last else "") or ""


# =============================================================================
# Email (Brevo)
# =============================================================================

def brevo_configured() -> bool:
    return bool(BREVO_API_KEY and SMTP_FROM_EMAIL)


def send_email(to_email: str, subject: str, body: str) -> None:
    if not brevo_configured():
        raise RuntimeError("BREVO_NOT_CONFIGURED (mangler BREVO_API_KEY/SMTP_FROM_EMAIL)")

    payload = {
        "sender": {"name": SMTP_FROM_NAME, "email": SMTP_FROM_EMAIL},
        "to": [{"email": to_email}],
        "subject": subject,
        "textContent": body,
    }

    r = requests.post(
        "https://api.brevo.com/v3/smtp/email",
        headers={
            "accept": "application/json",
            "content-type": "application/json",
            "api-key": BREVO_API_KEY,
        },
        json=payload,
        timeout=20,
    )

    if r.status_code >= 400:
        raise RuntimeError(f"BREVO_HTTP_{r.status_code}: {r.text}")


# =============================================================================
# Routes
# =============================================================================

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
        "brevo_enabled": brevo_configured(),
        "version": "2.1",
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
def api_brief(force_refresh: bool = False):
    try:
        return get_cached_brief(force_refresh=force_refresh)
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
        return JSONResponse(status_code=401, content={"error": "PREMIUM_REQUIRED", "message": "Historikk er kun for medlemmer."})
    rows = read_history(limit=limit)
    rows = add_forward_returns(rows, days_list=(7, 30))
    return {"count": len(rows), "items": rows}


@app.get("/api/premium/report/today")
def api_premium_report_today(x_api_key: str | None = Header(default=None)):
    if not is_valid_key(x_api_key):
        return JSONResponse(status_code=401, content={"error": "PREMIUM_REQUIRED", "message": "Premium kreves."})

    last = _read_last_snapshot() or {}
    rep = (last.get("premium_report") or "").strip()
    if rep:
        return {"updated_at": last.get("updated_at") or iso_now(), "report": rep}

    raw = get_cached_brief(force_refresh=False)
    rep2 = premium_report_ai(
        headlines=raw.get("headlines", []),
        signal_state=str(raw.get("signal") or "neutral"),
        signal_reason=str(raw.get("signal_reason") or ""),
        price_usd=safe_float(raw.get("price_usd")),
        change_pct=safe_float(raw.get("change_pct")),
        rsi14=safe_float(raw.get("rsi14")),
        trend_score=raw.get("trend_score") if isinstance(raw.get("trend_score"), int) else None,
    )
    return {"updated_at": iso_now(), "report": rep2 or ""}


@app.post("/api/premium/subscribe-email")
async def api_subscribe_email(req: Request, x_api_key: str | None = Header(default=None)):
    if not is_valid_key(x_api_key):
        return JSONResponse(status_code=401, content={"error": "PREMIUM_REQUIRED", "message": "Premium kreves."})

    body = await req.json()
    email = (body.get("email") or "").strip().lower()
    if "@" not in email:
        return JSONResponse(status_code=400, content={"error": "BAD_EMAIL", "message": "Ugyldig e-post."})

    conn = _db()
    conn.execute(
        "INSERT OR IGNORE INTO email_subscriptions(api_key,email,created_at,last_notified_signal,last_daily_sent_date) VALUES(?,?,?,?,?)",
        (x_api_key, email, iso_now(), None, None),
    )
    conn.commit()
    conn.close()
    return {"ok": True, "email": email}


# --- Admin tasks (manual trigger / cron)

@app.get("/api/tasks/check-signal")
def api_check_signal(admin_key: str = ""):
    if admin_key != ADMIN_API_KEY:
        return JSONResponse(status_code=401, content={"error": "UNAUTHORIZED", "message": "admin_key feil."})

    try:
        get_cached_brief(force_refresh=True)
    except Exception:
        pass

    sig = latest_signal().lower()
    if not sig:
        return {"ok": True, "sent": 0, "note": "No history yet."}

    if not brevo_configured():
        return {"ok": False, "sent": 0, "error": "BREVO_NOT_CONFIGURED"}

    conn = _db()
    rows = conn.execute("SELECT id, api_key, email, last_notified_signal FROM email_subscriptions").fetchall()
    sent = 0

    for row in rows:
        if not is_valid_key(row["api_key"]):
            continue

        last_sig = (row["last_notified_signal"] or "").lower()
        if last_sig == sig:
            continue

        to_email = row["email"]
        subject = f"Gullbrief: signal endret til {sig.upper()}"
        body = (
            "Signalet i Gullbrief har endret seg.\n\n"
            f"Nytt signal: {sig.upper()}\n"
            f"Se arkiv: {ALERT_BASE_URL}/archive\n\n"
            "(Dette er et automatisk varsel.)\n"
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


@app.get("/api/tasks/send-daily-premium")
def api_send_daily_premium(admin_key: str = ""):
    if admin_key != ADMIN_API_KEY:
        return JSONResponse(status_code=401, content={"error": "UNAUTHORIZED", "message": "admin_key feil."})

    if not brevo_configured():
        return JSONResponse(status_code=500, content={"error": "BREVO_NOT_CONFIGURED"})

    try:
        get_cached_brief(force_refresh=True)
    except Exception:
        pass

    today = datetime.now(timezone.utc).date().isoformat()
    last = _read_last_snapshot() or {}

    sig = ((last.get("signal") or "neutral").upper())
    price = safe_float(last.get("price_usd"))
    chg = safe_float(last.get("change_pct"))
    rsi14v = safe_float(last.get("rsi14"))
    tscore = last.get("trend_score") if isinstance(last.get("trend_score"), int) else None
    rep = (last.get("premium_report") or "").strip()

    subject = f"Gullbrief Premium ({today}) – {sig}"

    header_bits = []
    if price is not None:
        header_bits.append(f"Pris: {price:.2f} USD")
    if chg is not None:
        header_bits.append(f"Døgnendring: {chg:+.2f}%")
    if rsi14v is not None:
        header_bits.append(f"RSI(14): {rsi14v:.1f}")
    if tscore is not None:
        header_bits.append(f"Trend score: {tscore}/100")

    body = (
        "Gullbrief Premium – daglig kommentar\n"
        + ((" | ".join(header_bits)) + "\n\n" if header_bits else "\n")
        + (rep + "\n\n" if rep else "Premium-rapport ikke tilgjengelig i dag.\n\n")
        + f"Arkiv: {ALERT_BASE_URL}/archive\n"
        + "(Automatisk utsendelse.)\n"
    )

    conn = _db()
    rows = conn.execute("SELECT id, api_key, email, last_daily_sent_date FROM email_subscriptions").fetchall()
    sent = 0

    for row in rows:
        if not is_valid_key(row["api_key"]):
            continue

        if (row["last_daily_sent_date"] or "") == today:
            continue

        try:
            send_email(row["email"], subject, body)
            conn.execute("UPDATE email_subscriptions SET last_daily_sent_date=? WHERE id=?", (today, row["id"]))
            conn.commit()
            sent += 1
        except Exception:
            continue

    conn.close()
    return {"ok": True, "sent": sent, "date": today}


@app.post("/api/tasks/send-test-email")
async def api_send_test_email(req: Request, admin_key: str = ""):
    try:
        if admin_key != ADMIN_API_KEY:
            return JSONResponse(status_code=401, content={"error": "UNAUTHORIZED", "message": "admin_key feil."})

        if not brevo_configured():
            return JSONResponse(status_code=500, content={"error": "BREVO_NOT_CONFIGURED"})

        body = await req.json()
        email = (body.get("email") or "").strip().lower()
        if "@" not in email:
            return JSONResponse(status_code=400, content={"error": "BAD_EMAIL", "message": "Ugyldig e-post."})

        subject = "Gullbrief test ✅"
        msg = (
            "Dette er en testmail fra Gullbrief.\n\n"
            "Hvis du leser dette i innboksen (ikke spam), er Brevo-oppsettet i orden.\n"
        )

        send_email(email, subject, msg)
        return {"ok": True, "email": email}

    except Exception as e:
        print("SEND_TEST_EMAIL_FAILED:\n" + traceback.format_exc())
        return JSONResponse(status_code=500, content={"error": "SEND_FAILED", "message": str(e)})


@app.get("/api/tasks/tcp-ping")
def tcp_ping(host: str = "", port: int = 443, admin_key: str = ""):
    if admin_key != ADMIN_API_KEY:
        return JSONResponse(status_code=401, content={"error": "UNAUTHORIZED", "message": "admin_key feil."})
    if not host:
        return JSONResponse(status_code=400, content={"error": "MISSING_HOST", "message": "host mangler."})
    try:
        with socket.create_connection((host, int(port)), timeout=8):
            return {"ok": True, "host": host, "port": int(port)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "host": host, "port": int(port), "error": str(e)})


# --- Stripe: opprett checkout-session (POST)

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


# --- Success-side page

@app.get("/success", response_class=HTMLResponse)
def success_page(session_id: str = ""):
    return HTMLResponse(SUCCESS_HTML.replace("__SESSION_ID__", session_id or ""))


# --- Claim key (polling)

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
            return JSONResponse(status_code=409, content={"error": "NOT_COMPLETE", "message": "Checkout ikke complete."})

        customer = str(session.get("customer") or "")
        subscription = str(session.get("subscription") or "")
        email = (session.get("customer_details") or {}).get("email") or session.get("customer_email") or ""
        email = (email or "").strip().lower()

        if not customer or not subscription:
            return JSONResponse(status_code=400, content={"error": "MISSING_STRIPE_IDS", "message": "Mangler customer/subscription i session."})

        api_key = _upsert_key_for_stripe(email=email, customer_id=customer, subscription_id=subscription)

        conn = _db()
        row = conn.execute("SELECT api_key, status FROM api_keys WHERE api_key=?", (api_key,)).fetchone()
        conn.close()

        if not row or row["status"] != "active":
            return JSONResponse(status_code=409, content={"error": "NOT_ACTIVE", "message": "Abonnementet er ikke aktivert (venter på betaling/webhook)."})

        return {"api_key": row["api_key"], "email": email}
    except Exception as ex:
        return JSONResponse(status_code=400, content={"error": "STRIPE_CLAIM_FAILED", "message": str(ex)})


# --- Stripe webhook (source of truth)

@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    body_bytes = await request.body()
    sig_header = request.headers.get("Stripe-Signature")
    e = stripe_env()

    if not e["webhook_secret"]:
        return JSONResponse(status_code=500, content={"error": "Missing STRIPE_WEBHOOK_SECRET"})

    try:
        event = stripe.Webhook.construct_event(body_bytes, sig_header, e["webhook_secret"])
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

                payment_status = (obj.get("payment_status") or "").strip().lower()
                if payment_status == "paid":
                    _set_key_status_for_customer(customer, "active")

        elif event_type == "invoice.paid":
            customer = str(obj.get("customer") or "")
            subscription = str(obj.get("subscription") or "")
            if customer:
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


# =============================================================================
# Pages (UI)
# =============================================================================

@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)


@app.get("/premium", response_class=HTMLResponse)
def premium_page() -> HTMLResponse:
    return HTMLResponse(PREMIUM_HTML)


@app.get("/archive", response_class=HTMLResponse)
def archive() -> HTMLResponse:
    return HTMLResponse(ARCHIVE_HTML)


# =============================================================================
# HTML templates
# =============================================================================

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
        <a class="cta" href="/premium">Premium</a>
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
          <button onclick="location.href='/premium'">Se Premium</button>
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

PREMIUM_HTML = """<!doctype html>
<html lang="no">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Gullbrief Premium</title>
  <style>
    :root{--bg:#0f1720;--card:#16212c;--text:#e5e7eb;--muted:#9aa3af;--gold:#d4af37;--ok:#34d399;--err:#fb7185;--max:980px;--r:14px;}
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(1200px 800px at 20% 10%,#142234 0%,var(--bg) 55%) no-repeat;color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;line-height:1.5;}
    a{color:var(--text);text-decoration:none} a:hover{text-decoration:underline}
    .wrap{max-width:var(--max);margin:0 auto;padding:28px 18px 64px}
    header{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:8px 0 20px}
    .brand{font-weight:800}
    .nav{display:flex;gap:14px;align-items:center;color:var(--muted);font-size:14px}
    .nav a{color:var(--muted)}
    .cta{background:var(--gold);color:#0b0f14;padding:10px 14px;border-radius:999px;font-weight:800}
    .hero{margin-top:4px}
    .hero h1{margin:10px 0 8px;font-size:40px;font-family:ui-serif,Georgia,Times}
    .hero p{margin:0;color:var(--muted);font-size:18px;max-width:75ch}
    .grid{display:grid;grid-template-columns:1fr;gap:14px;margin-top:18px}
    @media (min-width: 920px){.grid{grid-template-columns:1.2fr .8fr}}
    .card{background:rgba(22,33,44,.92);border:1px solid rgba(255,255,255,.06);border-radius:var(--r);padding:18px}
    .kicker{color:var(--muted);font-weight:800;font-size:13px;text-transform:uppercase;letter-spacing:.08em}
    .price{font-size:34px;font-weight:900;margin:10px 0 6px}
    .small{font-size:12px;color:var(--muted)}
    ul{margin:10px 0 0;padding:0 0 0 16px} li{margin:10px 0}
    .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:12px}
    input{width:min(520px,100%);padding:10px 12px;border-radius:12px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.06);color:var(--text);outline:none}
    button{border:0;border-radius:12px;padding:10px 12px;font-weight:900;cursor:pointer;background:rgba(255,255,255,.08);color:var(--text)}
    button:hover{background:rgba(255,255,255,.12)}
    .btn-primary{background:var(--gold);color:#0b0f14}
    .note{margin-top:10px;color:var(--muted);font-size:13px}
    .faq h3{margin:12px 0 6px;font-size:16px}
    .divider{height:1px;background:rgba(255,255,255,.06);margin:14px 0}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="brand">Gullbrief</div>
      <div class="nav">
        <a href="/">Analyse</a>
        <a href="/archive">Arkiv</a>
        <a class="cta" href="/premium">Premium</a>
      </div>
    </header>

    <section class="hero">
      <div class="kicker">Premium</div>
      <h1>Daglig gullanalyse uten støy.</h1>
      <p>
        Gullbrief Premium gir deg en kort, nøktern daglig kommentar på norsk,
        basert på pris, trend, makro og nyhetsstrøm. Du får også arkiv, signalhistorikk
        og e-postvarsler.
      </p>
    </section>

    <section class="grid">
      <div class="card">
        <div style="font-size:18px;font-weight:900">Hva du får</div>
        <ul>
          <li><strong>Daglig premium-rapport</strong> (makro, drivere, hva som kan endre bildet)</li>
          <li><strong>Signal + indikatorforklaring</strong> (bullish / bearish / neutral)</li>
          <li><strong>Arkiv</strong> med historikk og avkastning 7d/30d fra signalpunkter</li>
          <li><strong>E-post</strong>: daglig utsendelse + varsel ved signalendring</li>
        </ul>

        <div class="divider"></div>

        <div style="font-size:18px;font-weight:900">Kjøp Premium</div>
        <div class="note">Skriv inn e-post, trykk kjøp, og du sendes til Stripe checkout.</div>

        <div class="row">
          <input id="payEmail" placeholder="E-post for kjøp" autocomplete="email" />
          <button class="btn-primary" id="btnPay">Kjøp Premium</button>
          <button onclick="location.href='/archive'">Jeg har allerede nøkkel</button>
        </div>

        <div id="status" class="note"></div>

        <div class="note">
          Etter betaling blir premium-nøkkelen vist på <code>/success</code> og lagres automatisk i nettleseren.
        </div>
      </div>

      <div class="card faq">
        <div style="font-size:18px;font-weight:900">Spørsmål</div>

        <h3>Hvor får jeg premium-nøkkelen?</h3>
        <div class="note">Etter checkout sendes du til success-siden. Der hentes nøkkelen automatisk.</div>

        <h3>Hvor ligger arkivet?</h3>
        <div class="note">På <a href="/archive">/archive</a>. Teaser er gratis, full historikk krever nøkkel.</div>

        <h3>Kan jeg avbryte?</h3>
        <div class="note">Ja, via Stripe. Når abonnementet stopper, blir nøkkelen inaktiv.</div>

        <h3>Er dette investeringsråd?</h3>
        <div class="note">Nei. Det er markedsanalyse og oppsummering, ikke kjøp/salg-anbefaling.</div>
      </div>
    </section>
  </div>

<script>
  const $ = (id) => document.getElementById(id);

  function setStatus(msg){ $("status").textContent = msg; }

  async function startCheckout(){
    const email = $("payEmail").value.trim();
    if(!email.includes("@")){ setStatus("Skriv inn gyldig e-post."); return; }
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

  $("btnPay").addEventListener("click", startCheckout);
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
        <a href="/">Analyse</a>
        <a class="cta" href="/premium">Premium</a>
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
        <div style="font-size:18px;font-weight:900">Medlemsområde</div>
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
          <button class="btn-primary" onclick="location.href='/premium'">Kjøp premium</button>
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
      setStatus("E-postvarsel aktivert ✅ (sendes ved signalendring + daglig premium)");
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