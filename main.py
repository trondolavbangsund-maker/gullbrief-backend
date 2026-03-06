from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import pathlib
import secrets
import sqlite3
import time
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

import requests
import stripe  # type: ignore
from fastapi import FastAPI, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles


# =============================================================================
# Gullbrief main.py – v3.9
# - Snapshot-basert public rendering for raskere lastetid
# - Direkte X/Twitter-posting med OAuth 1.0a
# - Gull/XAUUSD/makro-filter for nyheter
# - Engelske SEO-signaler i norske sider
# - 5 gratis nyheter / flere i premium
# - Premium / archive / Stripe / feed / sitemap / news-sitemap beholdt
# =============================================================================


# =============================================================================
# Config
# =============================================================================

APP_NAME = os.getenv("APP_NAME", "Gullbrief").strip()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()

YAHOO_SYMBOL = os.getenv("YAHOO_SYMBOL", "GC=F").strip()
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "900"))

RSS_FEEDS_ENV = os.getenv(
    "RSS_FEEDS",
    "https://www.reuters.com/markets/rss,"
    "https://www.kitco.com/rss/news,"
    "https://feeds.bloomberg.com/markets/news.rss,"
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GC%3DF&region=US&lang=en-US,"
    "https://www.investing.com/rss/news_11.rss",
)
RSS_FEEDS = [u.strip() for u in RSS_FEEDS_ENV.split(",") if u.strip()]

HISTORY_PATH = os.getenv("HISTORY_PATH", "data/history.jsonl").strip()
DB_PATH = os.getenv("DB_PATH", "data/app.db").strip()
PUBLIC_SNAPSHOT_PATH = os.getenv("PUBLIC_SNAPSHOT_PATH", "data/public_snapshot.json").strip()

ADMIN_API_KEY = os.getenv("PREMIUM_API_KEY", "gullbrief-dev").strip()
BASE_URL = os.getenv("BASE_URL", "").strip().rstrip("/")

BREVO_API_KEY = os.getenv("BREVO_API_KEY", "").strip()
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "").strip()
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", APP_NAME).strip()

STRIPE_SUCCESS_URL_DEFAULT = os.getenv("STRIPE_SUCCESS_URL", "").strip()
STRIPE_CANCEL_URL_DEFAULT = os.getenv("STRIPE_CANCEL_URL", "").strip()

GOOGLE_SITE_VERIFICATION = os.getenv("GOOGLE_SITE_VERIFICATION", "").strip()
if not GOOGLE_SITE_VERIFICATION:
    GOOGLE_SITE_VERIFICATION = "google-site-verification=W5dv0qhSwRLBDZH6YcVwJtqybjReTSmbjggqvhTJvVI"

if GOOGLE_SITE_VERIFICATION.startswith("google-site-verification="):
    GOOGLE_SITE_VERIFICATION_CONTENT = GOOGLE_SITE_VERIFICATION.split("=", 1)[1].strip()
else:
    GOOGLE_SITE_VERIFICATION_CONTENT = GOOGLE_SITE_VERIFICATION

TWITTER_SITE = os.getenv("TWITTER_SITE", "").strip()
SITEMAP_ARCHIVE_DAYS = int(os.getenv("SITEMAP_ARCHIVE_DAYS", "45"))
FEED_ITEMS = int(os.getenv("FEED_ITEMS", "20"))

FREE_HEADLINES_LIMIT = int(os.getenv("FREE_HEADLINES_LIMIT", "5"))
FULL_HEADLINES_LIMIT = int(os.getenv("FULL_HEADLINES_LIMIT", "15"))

SOCIAL_DAILY_ENABLED = os.getenv("SOCIAL_DAILY_ENABLED", "false").strip().lower() == "true"

PRIMARY_KEYWORDS = [
    "gold",
    "bullion",
    "xau",
    "xauusd",
    "precious metal",
]

SECONDARY_KEYWORDS = [
    "fed",
    "rates",
    "rate cut",
    "rate hike",
    "inflation",
    "cpi",
    "pce",
    "dollar",
    "usd",
    "treasury",
    "treasuries",
    "bond",
    "bonds",
    "yield",
    "yields",
    "real yields",
    "central bank",
    "recession",
    "safe haven",
    "safe-haven",
    "geopolitical",
    "war",
    "oil",
    "crude",
    "energy",
    "middle east",
    "middle-east",
]

CONTEXT_WORDS = [
    "market",
    "markets",
    "price",
    "prices",
    "risk",
    "stocks",
    "economy",
    "economic",
    "investors",
    "demand",
    "supply",
    "commodity",
    "commodities",
    "shipping",
    "energy",
    "futures",
    "outlook",
    "trade",
    "trading",
]


# =============================================================================
# App + CORS + Static
# =============================================================================

app = FastAPI(title=f"{APP_NAME} Backend", version="3.9", docs_url=None, redoc_url=None)

origins_env = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
allow_origins = [o.strip() for o in origins_env.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware  # type: ignore
    app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
except Exception:
    pass

try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception:
    pass


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
    h = headers or {}
    h.setdefault("User-Agent", "Mozilla/5.0 (compatible; Gullbrief/3.9)")
    h.setdefault("Accept", "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.1")
    r = requests.get(url, headers=h, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text


def get_base_url(request: Request) -> str:
    if BASE_URL:
        return BASE_URL
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{proto}://{host}".rstrip("/")


def dt_from_rss(pub: str) -> Optional[datetime]:
    try:
        return parsedate_to_datetime(pub) if pub else None
    except Exception:
        return None


def parse_iso_or_rss(dt_str: str) -> Optional[datetime]:
    t = dt_from_rss(dt_str)
    if t:
        return t
    try:
        return datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
    except Exception:
        return None


def date_yyyy_mm_dd_from_iso_or_rss(dt_str: str) -> Optional[str]:
    t = parse_iso_or_rss(dt_str)
    if not t:
        return None
    return t.date().isoformat()


def _escape_html(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _replace_many(template: str, mapping: Dict[str, str]) -> str:
    out = template
    for k, v in mapping.items():
        out = out.replace(k, v)
    return out


def _hash_email(email: str) -> str:
    return hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()[:16]


def _clip_text(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: max(0, n - 1)].rstrip() + "…"


def _ensure_parent_file(path_str: str) -> pathlib.Path:
    p = pathlib.Path(path_str)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def read_json_file(path_str: str) -> Optional[Dict[str, Any]]:
    p = _ensure_parent_file(path_str)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_json_file_atomic(path_str: str, data: Dict[str, Any]) -> None:
    p = _ensure_parent_file(path_str)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    tmp.replace(p)


def read_public_snapshot() -> Optional[Dict[str, Any]]:
    data = read_json_file(PUBLIC_SNAPSHOT_PATH)
    if not isinstance(data, dict):
        return None
    return data


def write_public_snapshot(data: Dict[str, Any]) -> None:
    payload = dict(data)
    payload["snapshot_saved_at"] = iso_now()
    write_json_file_atomic(PUBLIC_SNAPSHOT_PATH, payload)


def json_for_html(data: Dict[str, Any]) -> str:
    return (
        json.dumps(data, ensure_ascii=False)
        .replace("</", "<\\/")
        .replace("<!--", "<\\!--")
    )


# =============================================================================
# X / Twitter helpers
# =============================================================================

def x_configured() -> bool:
    return bool(
        os.getenv("X_API_KEY", "").strip()
        and os.getenv("X_API_SECRET", "").strip()
        and os.getenv("X_ACCESS_TOKEN", "").strip()
        and os.getenv("X_ACCESS_SECRET", "").strip()
    )


def _oauth1_header(
    *,
    method: str,
    url: str,
    consumer_key: str,
    consumer_secret: str,
    token: str,
    token_secret: str,
) -> str:
    nonce = secrets.token_hex(16)
    timestamp = str(int(time.time()))

    oauth_params = {
        "oauth_consumer_key": consumer_key,
        "oauth_nonce": nonce,
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": timestamp,
        "oauth_token": token,
        "oauth_version": "1.0",
    }

    def enc(v: str) -> str:
        return quote(str(v), safe="~-._")

    param_string = "&".join(f"{enc(k)}={enc(v)}" for k, v in sorted(oauth_params.items()))
    base_string = "&".join([
        method.upper(),
        enc(url),
        enc(param_string),
    ])

    signing_key = f"{enc(consumer_secret)}&{enc(token_secret)}"
    digest = hmac.new(signing_key.encode("utf-8"), base_string.encode("utf-8"), hashlib.sha1).digest()
    signature = base64.b64encode(digest).decode("utf-8")

    oauth_params["oauth_signature"] = signature

    header = "OAuth " + ", ".join(
        f'{enc(k)}="{enc(v)}"' for k, v in sorted(oauth_params.items())
    )
    return header


def send_social_post(text: str) -> Dict[str, Any]:
    consumer_key = os.getenv("X_API_KEY", "").strip()
    consumer_secret = os.getenv("X_API_SECRET", "").strip()
    access_token = os.getenv("X_ACCESS_TOKEN", "").strip()
    access_secret = os.getenv("X_ACCESS_SECRET", "").strip()

    if not (consumer_key and consumer_secret and access_token and access_secret):
        return {"ok": False, "message": "X_NOT_CONFIGURED"}

    url = "https://api.x.com/2/tweets"

    try:
        auth_header = _oauth1_header(
            method="POST",
            url=url,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            token=access_token,
            token_secret=access_secret,
        )

        r = requests.post(
            url,
            headers={
                "Authorization": auth_header,
                "Content-Type": "application/json",
            },
            json={"text": text},
            timeout=20,
        )

        body_preview = r.text[:1000]

        if r.status_code >= 400:
            return {"ok": False, "status_code": r.status_code, "body": body_preview}

        try:
            payload = r.json()
        except Exception:
            payload = {"raw": body_preview}

        return {"ok": True, "status_code": r.status_code, "body": payload}
    except Exception as e:
        return {"ok": False, "message": str(e)}


# =============================================================================
# Stripe helpers
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


def require_stripe(request: Optional[Request] = None) -> Dict[str, str]:
    e = stripe_env()
    if not (e["secret_key"] and e["price_id"]):
        raise RuntimeError("STRIPE_NOT_CONFIGURED: Sett STRIPE_SECRET_KEY og STRIPE_PRICE_ID")
    stripe.api_key = e["secret_key"]

    if request:
        base = get_base_url(request)
        if not e["success_url"]:
            e["success_url"] = f"{base}/success?session_id={{CHECKOUT_SESSION_ID}}"
        if not e["cancel_url"]:
            e["cancel_url"] = f"{base}/premium"

    return e


# =============================================================================
# DB
# =============================================================================

def _db() -> sqlite3.Connection:
    p = pathlib.Path(DB_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _try_add_column(conn: sqlite3.Connection, table: str, column_sql: str) -> None:
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_sql}")
        conn.commit()
    except Exception:
        pass


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
        last_macro_sent_date TEXT,
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

    _try_add_column(conn, "email_subscriptions", "last_macro_sent_date TEXT")

    conn.commit()
    conn.close()


@app.on_event("startup")
def _startup() -> None:
    init_db()
    snap = read_public_snapshot()
    if snap:
        CACHE.data = snap
        CACHE.ts = time.time()


def is_valid_key(k: Optional[str]) -> bool:
    if not k:
        return False
    if k == ADMIN_API_KEY:
        return True
    conn = _db()
    row = conn.execute("SELECT api_key,status FROM api_keys WHERE api_key=?", (k,)).fetchone()
    conn.close()
    return bool(row) and row["status"] == "active"


def _upsert_key_for_stripe(email: str, customer_id: str, subscription_id: str) -> str:
    conn = _db()
    row = conn.execute(
        "SELECT api_key FROM api_keys WHERE stripe_customer_id=? OR stripe_subscription_id=? OR email=?",
        (customer_id, subscription_id, email or None),
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


def _set_key_status_for_subscription(subscription_id: str, status: str) -> None:
    conn = _db()
    conn.execute("UPDATE api_keys SET status=? WHERE stripe_subscription_id=?", (status, subscription_id))
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
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Gullbrief/3.9)"}
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
        change_pct=change_pct,
        currency=currency,
        ts=iso_now(),
    )


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
        return "neutral", {
            "reason": "For lite historikk til SMA20/SMA50. Setter nøytral.",
            "rsi14": None,
            "trend_score": None,
        }

    last = closes[-1]
    s20, s50 = sma(closes, 20), sma(closes, 50)
    rsi14v = rsi(closes, 14)
    tscore = trend_score_from_mas(last, s20, s50)

    if s20 is None or s50 is None:
        return "neutral", {
            "reason": "Kunne ikke beregne glidende snitt.",
            "rsi14": rsi14v,
            "trend_score": tscore,
        }

    if last > s20 > s50:
        return "bullish", {
            "reason": "Pris over SMA20 og SMA50, med positiv trend.",
            "rsi14": rsi14v,
            "trend_score": tscore,
        }
    if last < s20 < s50:
        return "bearish", {
            "reason": "Pris under SMA20 og SMA50, med negativ trend.",
            "rsi14": rsi14v,
            "trend_score": tscore,
        }
    return "neutral", {
        "reason": "Blandet bilde mellom pris og glidende snitt.",
        "rsi14": rsi14v,
        "trend_score": tscore,
    }


def compute_technical_levels(symbol: str) -> Dict[str, Any]:
    chart = fetch_yahoo_chart(symbol, range_="6mo", interval="1d")
    closes = extract_closes(chart)

    if len(closes) < 60:
        return {
            "support_near": None,
            "support_major": None,
            "resistance_near": None,
            "resistance_major": None,
            "sma20": None,
            "sma50": None,
            "high_20d": None,
            "low_20d": None,
            "high_60d": None,
            "low_60d": None,
        }

    last_20 = closes[-20:]
    last_60 = closes[-60:]

    s20 = sma(closes, 20)
    s50 = sma(closes, 50)

    low_20 = min(last_20) if last_20 else None
    high_20 = max(last_20) if last_20 else None
    low_60 = min(last_60) if last_60 else None
    high_60 = max(last_60) if last_60 else None

    return {
        "support_near": low_20,
        "support_major": low_60,
        "resistance_near": high_20,
        "resistance_major": high_60,
        "sma20": s20,
        "sma50": s50,
        "high_20d": high_20,
        "low_20d": low_20,
        "high_60d": high_60,
        "low_60d": low_60,
    }


# =============================================================================
# RSS headlines + relevance filter
# =============================================================================

def parse_rss(xml_text: str, fallback_source: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    try:
        xml_bytes = xml_text.encode("utf-8", errors="ignore")
        root = ET.fromstring(xml_bytes)
    except Exception:
        return items

    channel = root.find("channel") or root.find(".//channel")
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


def is_gold_relevant_title(title: str) -> bool:
    t = (title or "").strip().lower()
    if not t:
        return False

    if any(k in t for k in PRIMARY_KEYWORDS):
        return True

    macro_hit = any(k in t for k in SECONDARY_KEYWORDS)
    context_hit = any(k in t for k in CONTEXT_WORDS)
    return macro_hit and context_hit


def fetch_headlines(limit: int = FULL_HEADLINES_LIMIT) -> List[Dict[str, str]]:
    if not RSS_FEEDS:
        return []

    headers = {"User-Agent": "Mozilla/5.0 (compatible; Gullbrief/3.9)"}
    all_items: List[Dict[str, str]] = []

    for feed_url in RSS_FEEDS:
        try:
            xml_text = http_get_text(feed_url, headers=headers, timeout=20)
            all_items.extend(parse_rss(xml_text, fallback_source=domain_of(feed_url) or "RSS"))
        except Exception:
            continue

    def _sort_key(x: Dict[str, str]) -> Tuple[int, float]:
        d = dt_from_rss(x.get("published", "") or "")
        if not d:
            return (0, 0.0)
        return (1, d.timestamp())

    all_items.sort(key=_sort_key, reverse=True)

    seen = set()
    filtered: List[Dict[str, str]] = []
    fallback: List[Dict[str, str]] = []

    for it in all_items:
        lk = (it.get("link") or "").strip()
        if not lk or lk in seen:
            continue
        if "/news/videos/" in lk:
            continue

        title = (it.get("title") or "").strip()
        seen.add(lk)

        if is_gold_relevant_title(title):
            filtered.append(it)
        else:
            fallback.append(it)

    out = filtered[:limit]
    if len(out) < limit:
        need = limit - len(out)
        out.extend(fallback[:need])

    return out[:limit]


# =============================================================================
# OpenAI bundle
# =============================================================================

def summarize_bundle_with_openai(
    *,
    headlines: List[Dict[str, str]],
    signal_state: str,
    signal_reason: str,
    price_usd: Optional[float],
    change_pct: Optional[float],
    rsi14: Optional[float],
    trend_score: Optional[int],
    levels: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    out = {"analysis": "", "forecast": "", "xauusd": "", "premium": ""}

    if not OPENAI_API_KEY or not headlines:
        return out

    titles = [h.get("title", "").strip() for h in headlines if h.get("title")][:12]
    if not titles:
        return out

    levels = levels or {}

    def fmt_level(x: Any) -> str:
        v = safe_float(x)
        return f"{v:.2f}" if v is not None else "ukjent"

    price_line = f"{price_usd:.2f} USD" if isinstance(price_usd, (int, float)) else "ukjent"
    chg_line = f"{change_pct:+.2f}%" if isinstance(change_pct, (int, float)) else "ukjent"
    rsi_line = f"{rsi14:.1f}" if isinstance(rsi14, (int, float)) else "ukjent"
    ts_line = f"{trend_score}" if isinstance(trend_score, int) else "ukjent"

    prompt = (
        f"Du er {APP_NAME}. Du skal skrive fire forskjellige tekster om gull basert på overskriftene.\n"
        "Viktig:\n"
        "- Norsk, nøkternt, ingen emojis, ingen investeringsråd.\n"
        "- Ikke finn opp fakta. Hvis overskriftene ikke støtter noe, si 'uklart' eller 'ikke bekreftet i overskriftene'.\n"
        "- Ikke gjenta samme formulering i alle feltene.\n"
        "- Svar KUN som gyldig JSON med nøyaktig disse nøklene:\n"
        '  {"analysis":"...", "forecast":"...", "xauusd":"...", "premium":"..."}\n\n'
        "Kontekst:\n"
        f"- Symbol: {YAHOO_SYMBOL}\n"
        f"- Pris: {price_line}\n"
        f"- Døgnendring: {chg_line}\n"
        f"- RSI(14): {rsi_line}\n"
        f"- Trend score: {ts_line}/100\n"
        f"- Signal: {signal_state.upper()}\n"
        f"- Indikator-årsak: {signal_reason}\n"
        f"- Nær støtte: {fmt_level(levels.get('support_near'))}\n"
        f"- Hovedstøtte: {fmt_level(levels.get('support_major'))}\n"
        f"- Nær motstand: {fmt_level(levels.get('resistance_near'))}\n"
        f"- Hovedmotstand: {fmt_level(levels.get('resistance_major'))}\n"
        f"- SMA20: {fmt_level(levels.get('sma20'))}\n"
        f"- SMA50: {fmt_level(levels.get('sma50'))}\n\n"
        "Overskrifter:\n- " + "\n- ".join(titles) + "\n\n"
        "Skriv:\n"
        "- analysis: 5–7 linjer. Forklar hva som driver gull nå og hvorfor.\n"
        "- forecast: 6–10 linjer. 24–72t scenario med base/bull/bear og tydelige triggere.\n"
        "- xauusd: 5–7 linjer. Fokuser på spot gull mot USD, DXY, renter og risk-on/off.\n"
        "- premium: 12–20 linjer. Struktur:\n"
        "  Tittel: én linje\n"
        "  Marked akkurat nå: 3–5 linjer\n"
        "  Teknisk bilde: 3–5 linjer, bruk støtte/motstand/SMA\n"
        "  Scenarier 24–72t:\n"
        "  - Base: ...\n"
        "  - Bull: ...\n"
        "  - Bear: ...\n"
        "  Hva bryter signalet:\n"
        "  - ...\n"
        "  - ...\n"
        "  Watchlist neste 24–72t:\n"
        "  - ...\n"
        "  - ...\n"
        "  - ...\n"
    )

    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.responses.create(model=OPENAI_MODEL, input=prompt)
        txt = (resp.output_text or "").strip()

        i = txt.find("{")
        j = txt.rfind("}")
        if i >= 0 and j > i:
            txt = txt[i:j + 1]

        data = json.loads(txt)

        for k in ("analysis", "forecast", "xauusd", "premium"):
            v = data.get(k)
            if isinstance(v, str):
                out[k] = v.strip()

        return out
    except Exception:
        return out


def premium_report_ai_from_bundle(
    *,
    bundle: Dict[str, str],
    signal_state: str,
    signal_reason: str,
    price_usd: Optional[float],
    change_pct: Optional[float],
    rsi14: Optional[float],
    trend_score: Optional[int],
    headlines: List[Dict[str, str]],
    levels: Optional[Dict[str, Any]] = None,
) -> str:
    levels = levels or {}

    def fmt_num(x: Any, suffix: str = "") -> str:
        v = safe_float(x)
        if v is None:
            return "ukjent"
        return f"{v:.2f}{suffix}"

    price_line = fmt_num(price_usd, " USD")
    chg_line = f"{change_pct:+.2f}%" if isinstance(change_pct, (int, float)) else "ukjent"
    rsi_line = f"{rsi14:.1f}" if isinstance(rsi14, (int, float)) else "ukjent"
    ts_line = f"{trend_score}/100" if isinstance(trend_score, int) else "ukjent"

    support_near = fmt_num(levels.get("support_near"))
    support_major = fmt_num(levels.get("support_major"))
    resistance_near = fmt_num(levels.get("resistance_near"))
    resistance_major = fmt_num(levels.get("resistance_major"))
    sma20_line = fmt_num(levels.get("sma20"))
    sma50_line = fmt_num(levels.get("sma50"))

    premium_text = (bundle.get("premium") or "").strip()
    analysis_text = (bundle.get("analysis") or "").strip()
    forecast_text = (bundle.get("forecast") or "").strip()
    xauusd_text = (bundle.get("xauusd") or "").strip()

    if premium_text:
        return (
            f"{APP_NAME} Premium ({datetime.now(timezone.utc).date().isoformat()})\n"
            f"Pris: {price_line} | Døgnendring: {chg_line} | RSI(14): {rsi_line} | Trend score: {ts_line}\n"
            f"Signal: {signal_state.upper()} ({signal_reason})\n"
            f"Støtte nær: {support_near} | Hovedstøtte: {support_major}\n"
            f"Motstand nær: {resistance_near} | Hovedmotstand: {resistance_major}\n"
            f"SMA20: {sma20_line} | SMA50: {sma50_line}\n\n"
            f"{premium_text}"
        )

    titles = [h.get("title", "").strip() for h in headlines if h.get("title")][:6]
    titles_block = "\n- ".join(titles) if titles else "(Ingen overskrifter tilgjengelig)"

    return (
        f"{APP_NAME} Premium ({datetime.now(timezone.utc).date().isoformat()})\n"
        f"Pris: {price_line} | Døgnendring: {chg_line} | RSI(14): {rsi_line} | Trend score: {ts_line}\n"
        f"Signal: {signal_state.upper()} ({signal_reason})\n"
        f"Støtte nær: {support_near} | Hovedstøtte: {support_major}\n"
        f"Motstand nær: {resistance_near} | Hovedmotstand: {resistance_major}\n"
        f"SMA20: {sma20_line} | SMA50: {sma50_line}\n\n"
        "Marked akkurat nå:\n"
        f"{analysis_text or 'Markedet er blandet og nyhetsbildet gir ikke nok til et tydelig premium-sammendrag akkurat nå.'}\n\n"
        "Scenarier 24–72t:\n"
        f"{forecast_text or 'Base: videre konsolidering. Bull: svakere USD/renter. Bear: sterkere USD og høyere realrenter.'}\n\n"
        "XAUUSD-fokus:\n"
        f"{xauusd_text or 'Se spesielt på DXY, amerikanske renter og bred risk-on/off i markedet.'}\n\n"
        "Nyhetsdriver (utdrag):\n- "
        f"{titles_block}\n\n"
        "Hva bryter signalet:\n"
        "- Pris klart under kortsiktig støtte og SMA20\n"
        "- Tydelig styrking i USD eller løft i renter\n\n"
        "Watchlist neste 24–72t:\n"
        "- DXY\n"
        "- 10Y-renter / realrenter\n"
        "- Makrooverskrifter med direkte effekt på gull"
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

    levels = compute_technical_levels(YAHOO_SYMBOL)
    headlines = fetch_headlines(limit=FULL_HEADLINES_LIMIT)

    bundle = summarize_bundle_with_openai(
        headlines=headlines,
        signal_state=signal_state,
        signal_reason=signal_reason,
        price_usd=yp.last,
        change_pct=yp.change_pct,
        rsi14=rsi14v,
        trend_score=tscore,
        levels=levels,
    )

    fallback = (" | ".join([h["title"] for h in headlines[:3] if h.get("title")]) or "Ingen nyheter tilgjengelig akkurat nå.")

    analysis_text = (bundle.get("analysis") or "").strip() or fallback
    forecast_text = (bundle.get("forecast") or "").strip() or fallback
    xauusd_text = (bundle.get("xauusd") or "").strip() or fallback
    premium_insight = (bundle.get("premium") or "").strip()

    return {
        "updated_at": yp.ts,
        "version": "3.9",
        "symbol": yp.symbol,
        "currency": yp.currency,
        "price_usd": yp.last,
        "change_pct": yp.change_pct,
        "signal": signal_state,
        "signal_reason": signal_reason,
        "rsi14": rsi14v,
        "trend_score": tscore,
        "levels": levels,
        "macro_summary": analysis_text,
        "analysis": analysis_text,
        "forecast": forecast_text,
        "xauusd": xauusd_text,
        "premium_insight": premium_insight,
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

    try:
        write_public_snapshot(data)
    except Exception:
        pass

    CACHE.data = data
    CACHE.ts = now
    return data


def get_public_brief(force_build: bool = False) -> Dict[str, Any]:
    if not force_build:
        snap = read_public_snapshot()
        if snap:
            return snap
        if CACHE.data:
            return CACHE.data

    return get_cached_brief(force_refresh=True)


def map_to_public_today(data: Dict[str, Any], mode: str = "analysis") -> Dict[str, Any]:
    mode = (mode or "analysis").strip().lower()
    if mode not in ("analysis", "forecast", "xauusd"):
        mode = "analysis"

    if mode == "forecast":
        summary = data.get("forecast") or data.get("macro_summary") or ""
    elif mode == "xauusd":
        summary = data.get("xauusd") or data.get("macro_summary") or ""
    else:
        summary = data.get("analysis") or data.get("macro_summary") or ""

    return {
        "updated_at": data.get("updated_at") or iso_now(),
        "version": data.get("version", "3.9"),
        "gold": {"price_usd": data.get("price_usd"), "change_pct": data.get("change_pct")},
        "signal": {"state": data.get("signal", "neutral"), "reason_short": data.get("signal_reason", "")},
        "macro": {"mode": mode, "summary_short": summary},
        "headlines": (data.get("headlines") or [])[:FREE_HEADLINES_LIMIT],
        "headlines_total": len(data.get("headlines") or []),
        "headlines_free_limit": FREE_HEADLINES_LIMIT,
    }


def get_public_today_payload(mode: str = "analysis") -> Dict[str, Any]:
    data = get_public_brief(force_build=False)
    return map_to_public_today(data, mode)


# =============================================================================
# History
# =============================================================================

def _ensure_history_dir() -> pathlib.Path:
    p = pathlib.Path(HISTORY_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


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
            f.seek(max(0, size - 16384), 0)
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

    new_dt = parse_iso_or_rss(new_data.get("updated_at", "")) or datetime.now(timezone.utc)
    last_dt = parse_iso_or_rss(last.get("updated_at", "")) or datetime.now(timezone.utc)
    return new_dt.date() != last_dt.date()


def store_snapshot_if_needed(data: Dict[str, Any]) -> bool:
    p = _ensure_history_dir()
    last = _read_last_snapshot()
    if not _should_store_snapshot(data, last):
        return False

    rep = premium_report_ai_from_bundle(
        bundle={
            "premium": (data.get("premium_insight") or ""),
            "analysis": (data.get("analysis") or data.get("macro_summary") or ""),
            "forecast": (data.get("forecast") or ""),
            "xauusd": (data.get("xauusd") or ""),
        },
        signal_state=str(data.get("signal") or "neutral"),
        signal_reason=str(data.get("signal_reason") or ""),
        price_usd=safe_float(data.get("price_usd")),
        change_pct=safe_float(data.get("change_pct")),
        rsi14=safe_float(data.get("rsi14")),
        trend_score=data.get("trend_score") if isinstance(data.get("trend_score"), int) else None,
        headlines=data.get("headlines", []),
        levels=data.get("levels") if isinstance(data.get("levels"), dict) else {},
    )

    rec = {
        "updated_at": data.get("updated_at") or iso_now(),
        "version": data.get("version", "3.9"),
        "symbol": data.get("symbol"),
        "price_usd": data.get("price_usd"),
        "change_pct": data.get("change_pct"),
        "signal": data.get("signal"),
        "signal_reason": data.get("signal_reason", ""),
        "rsi14": data.get("rsi14"),
        "trend_score": data.get("trend_score"),
        "levels": data.get("levels", {}),
        "macro_summary": data.get("macro_summary", ""),
        "analysis": data.get("analysis", ""),
        "forecast": data.get("forecast", ""),
        "xauusd": data.get("xauusd", ""),
        "premium_insight": data.get("premium_insight", ""),
        "premium_report": rep or "",
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
    parsed: List[Tuple[Optional[datetime], Dict[str, Any]]] = [(parse_iso_or_rss(r.get("updated_at", "")), r) for r in rows]
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


def signal_stats_last30(rows_newest_first: List[Dict[str, Any]]) -> Dict[str, Any]:
    sig_rows = []
    for r in rows_newest_first:
        s = (r.get("signal") or "").lower()
        if s in ("bullish", "bearish"):
            sig_rows.append(r)
        if len(sig_rows) >= 30:
            break

    bullish: List[float] = []
    bearish: List[float] = []
    hits = 0
    evals = 0

    for r in sig_rows:
        r7 = safe_float(r.get("return_7d_pct"))
        if r7 is None:
            continue
        s = (r.get("signal") or "").lower()
        evals += 1
        if s == "bullish":
            bullish.append(r7)
            if r7 > 0:
                hits += 1
        elif s == "bearish":
            bearish.append(r7)
            if r7 < 0:
                hits += 1

    def avg(xs: List[float]) -> Optional[float]:
        if not xs:
            return None
        return sum(xs) / len(xs)

    return {
        "signals_considered": len(sig_rows),
        "evaluated_with_7d": evals,
        "bullish_avg_7d": avg(bullish),
        "bearish_avg_7d": avg(bearish),
        "hit_rate_7d": (hits / evals * 100.0) if evals else None,
    }


def get_archive_dates(last_n_days: int = 45) -> List[str]:
    rows = read_history(limit=2000)
    today_utc = datetime.now(timezone.utc).date()
    cutoff = today_utc - timedelta(days=max(0, last_n_days - 1))

    seen = set()
    dates: List[str] = []

    for r in rows:
        d = date_yyyy_mm_dd_from_iso_or_rss(str(r.get("updated_at") or ""))
        if not d:
            continue
        try:
            dd = date.fromisoformat(d)
        except Exception:
            continue
        if dd < cutoff:
            continue
        if d not in seen:
            seen.add(d)
            dates.append(d)

    dates.sort(reverse=True)
    return dates


def load_snapshot_for_date(day: str) -> Optional[Dict[str, Any]]:
    p = _ensure_history_dir()
    if not p.exists():
        return None
    best: Optional[Dict[str, Any]] = None
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            d = date_yyyy_mm_dd_from_iso_or_rss(str(r.get("updated_at") or ""))
            if d == day:
                best = r
    return best


# =============================================================================
# Email
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
# Social / X
# =============================================================================

def build_daily_social_post(data: Dict[str, Any], request: Optional[Request] = None) -> Dict[str, Any]:
    signal_state = str(data.get("signal") or "neutral").upper()
    price = safe_float(data.get("price_usd"))
    change_pct = safe_float(data.get("change_pct"))
    link_base = get_base_url(request) if request else (BASE_URL or "https://gullbrief.no")
    link = f"{link_base}/gullpris-analyse"

    price_txt = f"${price:,.2f}" if price is not None else "N/A"
    change_txt = f"{change_pct:+.2f}%" if change_pct is not None else "N/A"

    if signal_state == "BULLISH":
        summary = "Gold remains supported by geopolitical tension and a positive technical trend."
    elif signal_state == "BEARISH":
        summary = "Gold is under pressure as momentum weakens and the technical picture softens."
    else:
        summary = "Gold is trading in a mixed range as markets weigh macro and geopolitical drivers."

    post = (
        f"Gold price update\n\n"
        f"Gold: {price_txt} ({change_txt})\n"
        f"Signal: {signal_state}\n\n"
        f"{summary}\n\n"
        f"More: {link}\n\n"
        f"#gold #xauusd #goldprice #commodities"
    )

    return {
        "date": datetime.now(timezone.utc).date().isoformat(),
        "signal": signal_state,
        "price_usd": price,
        "change_pct": change_pct,
        "text": post,
        "url": link,
        "enabled": SOCIAL_DAILY_ENABLED,
        "configured": x_configured(),
    }


# =============================================================================
# Navigation / UI helpers
# =============================================================================

def nav_tabs(active: str) -> str:
    tabs = [
        ("/gullpris-analyse", "analysis", "📈 Analyse"),
        ("/gullpris-prognose", "forecast", "🔮 Prognose"),
        ("/xauusd", "xauusd", "💵 XAUUSD"),
        ("/gullpris-signal", "signal", "🚦 Signal"),
        ("/premium", "premium", "⭐ Premium"),
    ]
    links = []
    for href, key, label in tabs:
        cls = "tab active" if key == active else "tab"
        links.append(f'<a class="{cls}" href="{href}">{_escape_html(label)}</a>')
    return '<div class="page-tabs">' + "".join(links) + "</div>"


# =============================================================================
# SEO helpers
# =============================================================================

def jsonld_website(base: str) -> str:
    data = {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": APP_NAME,
        "url": f"{base}/",
        "inLanguage": "no",
        "potentialAction": {
            "@type": "SearchAction",
            "target": f"{base}/gullpris?q={{search_term_string}}",
            "query-input": "required name=search_term_string",
        },
    }
    return '<script type="application/ld+json">' + json.dumps(data, ensure_ascii=False) + "</script>"


def jsonld_article(base: str, title: str, description: str, url_path: str, date_published: Optional[str] = None) -> str:
    data: Dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": title,
        "description": description,
        "inLanguage": "no",
        "mainEntityOfPage": {"@type": "WebPage", "@id": f"{base}{url_path}"},
        "publisher": {"@type": "Organization", "name": APP_NAME},
        "dateModified": iso_now(),
    }
    if date_published:
        data["datePublished"] = date_published
    return '<script type="application/ld+json">' + json.dumps(data, ensure_ascii=False) + "</script>"


COMMON_STYLE = """
<style>
  :root{
    --bg:#0f1720;
    --card:#16212c;
    --card-2:#1a2633;
    --text:#e5e7eb;
    --muted:#9aa3af;
    --gold:#d4af37;
    --gold-soft:#e1c15b;
    --ok:#34d399;
    --err:#fb7185;
    --max:1120px;
    --r:18px;
    --line:rgba(255,255,255,.07);
  }
  *{box-sizing:border-box}
  body{
    margin:0;
    background:
      radial-gradient(1200px 800px at 20% 10%,#142234 0%,var(--bg) 55%) no-repeat,
      linear-gradient(180deg,#0d1520 0%,#0f1720 100%);
    color:var(--text);
    font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;
    line-height:1.5;
  }
  a{color:var(--text);text-decoration:none}
  a:hover{text-decoration:none}
  .wrap{max-width:var(--max);margin:0 auto;padding:28px 18px 72px}
  header{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:8px 0 18px}
  .brand{font-weight:850;letter-spacing:.2px;font-size:15px}
  .nav{display:flex;gap:14px;align-items:center;color:var(--muted);font-size:14px;flex-wrap:wrap}
  .nav a{color:var(--muted)}
  .cta{background:var(--gold);color:#0b0f14;padding:10px 14px;border-radius:999px;font-weight:850}
  .hero{padding:18px 0 4px}
  .hero h1{margin:10px 0 8px;font-size:40px;font-family:ui-serif,Georgia,Times;letter-spacing:-.3px}
  .hero p{margin:0;color:var(--muted);font-size:18px;max-width:78ch}
  .page-tabs{
    display:flex;
    gap:10px;
    flex-wrap:wrap;
    margin:20px 0 18px;
    padding:10px 12px;
    border:1px solid var(--line);
    background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.025));
    border-radius:18px;
    box-shadow:0 8px 24px rgba(0,0,0,.14);
    backdrop-filter: blur(4px);
  }
  .tab{
    display:inline-flex;
    align-items:center;
    gap:8px;
    padding:11px 15px;
    border-radius:999px;
    color:#e8e1c6;
    font-weight:800;
    font-size:15px;
    letter-spacing:.1px;
    transition:all .15s ease;
  }
  .tab:hover{
    background:rgba(255,255,255,.06);
    color:#fff0bf;
  }
  .tab.active{
    background:var(--gold);
    color:#10141b;
    box-shadow:0 6px 18px rgba(212,175,55,.22);
  }
  .grid{display:grid;grid-template-columns:1fr;gap:16px;margin-top:10px}
  @media (min-width:980px){.grid{grid-template-columns:1.15fr .85fr}}
  .card{
    background:linear-gradient(180deg,rgba(22,33,44,.95),rgba(20,30,40,.95));
    border:1px solid var(--line);
    border-radius:var(--r);
    padding:18px;
    box-shadow:0 10px 28px rgba(0,0,0,.18);
  }
  .title{display:flex;justify-content:space-between;gap:10px;align-items:baseline}
  .title h2{margin:0;font-size:16px;color:var(--muted);font-weight:780}
  .big{font-size:34px;font-weight:900;margin:8px 0 0}
  .sub{color:var(--muted);margin-top:2px}
  .pill{
    display:inline-flex;
    align-items:center;
    gap:8px;
    padding:7px 10px;
    border-radius:999px;
    background:rgba(255,255,255,.06);
    font-weight:850;
    margin-top:10px
  }
  .pill .dot{width:9px;height:9px;border-radius:99px;background:var(--muted)}
  .pill.bullish .dot{background:var(--ok)}
  .pill.bearish .dot{background:var(--err)}
  .pill.neutral .dot{background:var(--gold)}
  .muted{color:var(--muted)}
  ul{margin:10px 0 0;padding:0 0 0 16px}
  li{margin:10px 0}
  .btnrow{display:flex;gap:10px;flex-wrap:wrap;margin-top:12px}
  button{
    border:0;
    border-radius:12px;
    padding:10px 12px;
    font-weight:850;
    cursor:pointer;
    background:rgba(255,255,255,.08);
    color:var(--text)
  }
  button:hover{background:rgba(255,255,255,.12)}
  input{
    width:min(520px,100%);
    padding:10px 12px;
    border-radius:12px;
    border:1px solid rgba(255,255,255,.10);
    background:rgba(255,255,255,.06);
    color:var(--text);
    outline:none
  }
  table{width:100%;border-collapse:collapse;margin-top:14px}
  th,td{padding:10px 8px;border-bottom:1px solid rgba(255,255,255,.06);text-align:left;vertical-align:top}
  th{color:var(--muted);font-weight:850;font-size:13px}
  td{font-size:14px}
  .small{font-size:12px;color:var(--muted)}
  code{background:rgba(255,255,255,.07);padding:2px 6px;border-radius:8px}
  footer{margin-top:22px;color:var(--muted);font-size:13px}
  .links{display:flex;gap:12px;flex-wrap:wrap;margin-top:6px}
  .links a{color:var(--muted)}
  .premiumhint{margin-top:12px;padding:10px 12px;border-radius:12px;background:rgba(212,175,55,.08);border:1px solid rgba(212,175,55,.18);color:#f1e2a7}
</style>
"""


def html_shell(
    request: Request,
    *,
    title: str,
    description: str,
    path: str,
    body_html: str,
    article_date: Optional[str] = None,
) -> str:
    base = get_base_url(request)
    canonical = f"{base}{path}"
    og_image = f"{base}/og.svg"

    robots = "index,follow,max-image-preview:large,max-snippet:-1,max-video-preview:-1"
    twitter_site_meta = f'<meta name="twitter:site" content="{_escape_html(TWITTER_SITE)}" />' if TWITTER_SITE else ""

    favicon_meta = (
        '<link rel="icon" href="/static/favicon.ico" sizes="any" />'
        '<link rel="apple-touch-icon" href="/static/apple-touch-icon.png" />'
    )

    head = (
        '<meta charset="utf-8" />'
        '<meta name="viewport" content="width=device-width,initial-scale=1" />'
        f"<title>{_escape_html(title)}</title>"
        f'<meta name="description" content="{_escape_html(description)}" />'
        f'<meta name="robots" content="{robots}" />'
        f'<link rel="canonical" href="{canonical}" />'
        f'<link rel="alternate" type="application/rss+xml" title="{_escape_html(APP_NAME)} feed" href="{base}/feed.xml" />'
        f'<meta name="google-site-verification" content="{_escape_html(GOOGLE_SITE_VERIFICATION_CONTENT)}" />'
        f'<meta property="og:site_name" content="{_escape_html(APP_NAME)}" />'
        '<meta property="og:locale" content="nb_NO" />'
        '<meta property="og:type" content="website" />'
        f'<meta property="og:title" content="{_escape_html(title)}" />'
        f'<meta property="og:description" content="{_escape_html(description)}" />'
        f'<meta property="og:url" content="{canonical}" />'
        f'<meta property="og:image" content="{og_image}" />'
        '<meta name="twitter:card" content="summary_large_image" />'
        f'<meta name="twitter:title" content="{_escape_html(title)}" />'
        f'<meta name="twitter:description" content="{_escape_html(description)}" />'
        f'<meta name="twitter:image" content="{og_image}" />'
        + twitter_site_meta
        + favicon_meta
        + jsonld_website(base)
        + jsonld_article(base, title, description, path, date_published=article_date)
    )

    return (
        "<!doctype html>"
        '<html lang="no"><head>'
        + head
        + COMMON_STYLE
        + "</head><body>"
        + body_html
        + "</body></html>"
    )


# =============================================================================
# Templates
# =============================================================================

def footer_links() -> str:
    return """
    <footer>
      <div class="links">
        <a href="/gullpris-prognose">Gullpris prognose</a>
        <a href="/gullpris">Gullpris i dag</a>
        <a href="/gullpris-analyse">Gullpris analyse</a>
        <a href="/gullpris-signal">Gullpris signal</a>
        <a href="/xauusd">XAUUSD</a>
        <a href="/premium">Premium</a>
        <a href="/archive">Arkiv</a>
      </div>
      <div style="margin-top:8px">© Gullbrief. Ikke investeringsråd.</div>
    </footer>
    """


INDEX_BODY_TEMPLATE = """
<div class="wrap">
  <header>
    <div class="brand">__APP_NAME__</div>
    <div class="nav">
      <a href="/">Analyse</a>
      <a href="/gullpris">Gullpris</a>
      <a href="/archive">Arkiv</a>
      <a class="cta" href="/premium">Premium</a>
    </div>
  </header>

  <section class="hero">
    <h1>Gullpris analyse</h1>
    <p>__DESC__</p>
  </section>

  __NAV_TABS__

  <section class="grid">
    <div class="card">
      <div class="title"><h2>Dagens status</h2><div class="muted" id="updatedAt">Oppdaterer…</div></div>
      <div class="big" id="price">$–</div>
      <div class="sub" id="change">–</div>
      <div class="pill neutral" id="signalPill"><span class="dot"></span><span id="signalText">Signal: –</span></div>
      <p class="muted" style="margin-top:12px" id="reason">–</p>

      <h2 style="margin-top:14px">Analyse</h2>
      <p class="muted" id="macro">–</p>
      <div class="premiumhint">
        Les full analyse, flere nyheter og signalhistorikk i <a href="/premium">Premium</a>.
      </div>

      <div class="btnrow">
        <button id="btnReload">Oppdater</button>
        <button id="btnRefresh">Hard refresh</button>
        <button onclick="location.href='/premium'">Premium</button>
        <button onclick="location.href='/archive'">Arkiv</button>
      </div>

      <div class="muted" id="status" style="margin-top:8px">Status: …</div>
    </div>

    <div class="card">
      <div class="title"><h2>Relevante nyheter</h2><div class="muted">Direkte kilder</div></div>
      <ul id="headlines"></ul>
      <div id="premiumNewsHint" class="premiumhint" style="display:none"></div>
    </div>
  </section>

  __FOOTER__
</div>

<script id="initialTodayData" type="application/json">__INITIAL_JSON__</script>
<script>
  const MODE = "analysis";
  const $ = (id) => document.getElementById(id);
  const fmtPct = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ((Number(x)>0?"+":"") + Number(x).toFixed(2) + "%");
  const fmtPrice = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ("$" + Number(x).toLocaleString(undefined,{maximumFractionDigits:2}));
  const pillClass = (s) => (s||"").toLowerCase().includes("bull") ? "bullish" : ((s||"").toLowerCase().includes("bear") ? "bearish" : "neutral");

  function renderHeadlines(data){
    const ul = $("headlines");
    ul.innerHTML = "";
    (data.headlines||[]).forEach(h=>{
      const li=document.createElement("li");
      const a=document.createElement("a");
      a.href=h.link; a.target="_blank"; a.rel="noopener noreferrer";
      a.textContent=h.title || "(uten tittel)";
      const d=document.createElement("div"); d.className="muted";
      d.textContent=(h.source||"Kilde") + (h.published?(" | "+h.published):"");
      li.appendChild(a); li.appendChild(d); ul.appendChild(li);
    });
    const total = Number(data.headlines_total || 0);
    const freeLimit = Number(data.headlines_free_limit || 0);
    const hint = $("premiumNewsHint");
    if(total > freeLimit){
      hint.style.display = "";
      hint.innerHTML = `Viser ${freeLimit} gratis nyheter. Premium gir tilgang til flere markedssaker og arkiv. <a href="/premium">Åpne Premium</a>`;
    }else{
      hint.style.display = "none";
      hint.textContent = "";
    }
  }

  function renderToday(data){
    $("updatedAt").textContent = "Oppdatert: " + (data.updated_at || "–");
    $("price").textContent = fmtPrice(data?.gold?.price_usd);
    $("change").textContent = "Endring: " + fmtPct(data?.gold?.change_pct);
    const state = data?.signal?.state || "neutral";
    $("signalText").textContent = "Signal: " + state;
    $("signalPill").className = "pill " + pillClass(state);
    $("reason").textContent = data?.signal?.reason_short || "–";
    $("macro").textContent = data?.macro?.summary_short || "–";
    renderHeadlines(data);
  }

  function renderInitial(){
    try{
      const raw = $("initialTodayData")?.textContent || "{}";
      const data = JSON.parse(raw);
      if(data && data.gold){
        renderToday(data);
        $("status").textContent = "Status: Snapshot lastet";
        return true;
      }
    }catch(e){}
    return false;
  }

  async function loadToday(){
    try{
      $("status").textContent = "Status: Laster snapshot…";
      const res = await fetch("/api/public/today?mode=" + encodeURIComponent(MODE), {cache:"no-store"});
      const data = await res.json();
      if(!res.ok) throw new Error(data?.message || ("HTTP " + res.status));
      renderToday(data);
      $("status").textContent = "Status: OK";
    }catch(e){
      $("status").textContent = "Status: Feil: " + e;
    }
  }

  $("btnReload").addEventListener("click", loadToday);
  $("btnRefresh").addEventListener("click", async () => {
    $("status").textContent = "Status: Bygger ny snapshot…";
    await fetch("/api/brief/refresh", {cache:"no-store"}).catch(()=>{});
    await loadToday();
  });

  if(!renderInitial()){
    loadToday();
  }
</script>
"""


PREMIUM_BODY_TEMPLATE = """
<div class="wrap">
  <header>
    <div class="brand">__APP_NAME__</div>
    <div class="nav">
      <a href="/">Analyse</a>
      <a href="/gullpris">Gullpris</a>
      <a href="/archive">Arkiv</a>
      <a class="cta" href="/premium">Premium</a>
    </div>
  </header>

  <section class="hero">
    <h1>Premium</h1>
    <p>Mer data, mindre støy. Daglig premium-rapport, signalhistorikk, flere nyheter og arkiv.</p>
  </section>

  __NAV_TABS__

  <section class="grid">
    <div class="card">
      <div class="title"><h2>Dette får du</h2><div class="muted">Premium</div></div>
      <ul>
        <li><b>Signalhistorikk (siste 30)</b> + treffsikkerhet</li>
        <li><b>Arkiv</b> med 7d/30d etter signal</li>
        <li><b>Daglig premium-rapport</b> på norsk</li>
        <li><b>Flere nyheter</b> enn gratisversjonen</li>
        <li><b>E-postvarsler</b> ved signalendring og daglig utsendelse</li>
      </ul>

      <h2 style="margin-top:14px">Kjøp Premium</h2>
      <p class="muted">Skriv e-post og gå til Stripe checkout.</p>
      <div class="btnrow">
        <input id="payEmail" placeholder="E-post for kjøp" autocomplete="email" />
        <button class="cta" id="btnPay" style="border:0">Kjøp premium</button>
      </div>
      <div class="small" id="status" style="margin-top:10px"></div>
    </div>

    <div class="card">
      <div class="title"><h2>Hva rapporten inneholder</h2><div class="muted">Daglig</div></div>
      <ul>
        <li>Marked akkurat nå</li>
        <li>Teknisk bilde med støtte og motstand</li>
        <li>Base / Bull / Bear-scenario</li>
        <li>Hva som bryter signalet</li>
        <li>Watchlist neste 24–72t</li>
      </ul>
    </div>
  </section>

  __FOOTER__
</div>

<script>
  const $ = (id)=>document.getElementById(id);
  async function startCheckout(){
    const email = $("payEmail").value.trim();
    if(!email.includes("@")){ $("status").textContent="Skriv inn gyldig e-post."; return; }
    try{
      $("status").textContent="Åpner Stripe checkout…";
      const res = await fetch("/api/stripe/create-checkout", {
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body: JSON.stringify({email})
      });
      const data = await res.json();
      if(!res.ok){ $("status").textContent = data?.message || ("HTTP "+res.status); return; }
      location.href = data.url;
    }catch(e){
      $("status").textContent = "Feil: " + e;
    }
  }
  $("btnPay").addEventListener("click", startCheckout);
</script>
"""


SEO_LANDING_TEMPLATE = """
<div class="wrap">
  <header>
    <div class="brand">__APP_NAME__</div>
    <div class="nav">
      <a href="/">Analyse</a>
      <a href="/gullpris">Gullpris</a>
      <a href="/archive">Arkiv</a>
      <a class="cta" href="/premium">Premium</a>
    </div>
  </header>

  <section class="hero">
    <h1>__H1__</h1>
    <p>__INTRO__</p>
  </section>

  __NAV_TABS__

  <section class="grid">
    <div class="card">
      <div class="title"><h2>Dagens status</h2><div class="muted" id="updatedAt">Oppdaterer…</div></div>
      <div class="big" id="price">$–</div>
      <div class="sub" id="change">–</div>
      <div class="pill neutral" id="signalPill"><span class="dot"></span><span id="signalText">Signal: –</span></div>
      <p class="muted" style="margin-top:12px" id="reason">–</p>
      <h2 style="margin-top:14px">Kort tekst</h2>
      <p class="muted" id="macro">–</p>
      <div class="premiumhint">
        Full analyse, flere nyheter og signalhistorikk ligger i <a href="/premium">Premium</a>.
      </div>

      <div class="btnrow">
        <button id="btnReload">Oppdater</button>
        <button onclick="location.href='/premium'">Premium</button>
        <button onclick="location.href='/archive'">Arkiv</button>
      </div>
      <div class="muted" id="status" style="margin-top:8px">Status: …</div>
    </div>

    <div class="card">
      <div class="title"><h2>Relevante nyheter</h2><div class="muted">Direkte kilder</div></div>
      <ul id="headlines"></ul>
      <div id="premiumNewsHint" class="premiumhint" style="display:none"></div>
    </div>
  </section>

  __FOOTER__
</div>

<script id="initialTodayData" type="application/json">__INITIAL_JSON__</script>
<script>
  const MODE = "__MODE__";
  const $ = (id) => document.getElementById(id);
  const fmtPct = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ((Number(x)>0?"+":"") + Number(x).toFixed(2) + "%");
  const fmtPrice = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ("$" + Number(x).toLocaleString(undefined,{maximumFractionDigits:2}));
  const pillClass = (s) => (s||"").toLowerCase().includes("bull") ? "bullish" : ((s||"").toLowerCase().includes("bear") ? "bearish" : "neutral");

  function renderHeadlines(data){
    const ul = $("headlines");
    ul.innerHTML = "";
    (data.headlines||[]).forEach(h=>{
      const li=document.createElement("li");
      const a=document.createElement("a");
      a.href=h.link; a.target="_blank"; a.rel="noopener noreferrer";
      a.textContent=h.title || "(uten tittel)";
      const d=document.createElement("div"); d.className="muted";
      d.textContent=(h.source||"Kilde") + (h.published?(" | "+h.published):"");
      li.appendChild(a); li.appendChild(d); ul.appendChild(li);
    });
    const total = Number(data.headlines_total || 0);
    const freeLimit = Number(data.headlines_free_limit || 0);
    const hint = $("premiumNewsHint");
    if(total > freeLimit){
      hint.style.display = "";
      hint.innerHTML = `Viser ${freeLimit} gratis nyheter. Premium gir tilgang til flere markedssaker og arkiv. <a href="/premium">Åpne Premium</a>`;
    }else{
      hint.style.display = "none";
      hint.textContent = "";
    }
  }

  function renderToday(data){
    $("updatedAt").textContent = "Oppdatert: " + (data.updated_at || "–");
    $("price").textContent = fmtPrice(data?.gold?.price_usd);
    $("change").textContent = "Endring: " + fmtPct(data?.gold?.change_pct);
    const state = data?.signal?.state || "neutral";
    $("signalText").textContent = "Signal: " + state;
    $("signalPill").className = "pill " + pillClass(state);
    $("reason").textContent = data?.signal?.reason_short || "–";
    $("macro").textContent = data?.macro?.summary_short || "–";
    renderHeadlines(data);
  }

  function renderInitial(){
    try{
      const raw = $("initialTodayData")?.textContent || "{}";
      const data = JSON.parse(raw);
      if(data && data.gold){
        renderToday(data);
        $("status").textContent = "Status: Snapshot lastet";
        return true;
      }
    }catch(e){}
    return false;
  }

  async function loadToday(){
    try{
      $("status").textContent = "Status: Laster snapshot…";
      const res = await fetch("/api/public/today?mode=" + encodeURIComponent(MODE), {cache:"no-store"});
      const data = await res.json();
      if(!res.ok) throw new Error(data?.message || ("HTTP " + res.status));
      renderToday(data);
      $("status").textContent = "Status: OK";
    }catch(e){
      $("status").textContent = "Status: Feil: " + e;
    }
  }

  $("btnReload").addEventListener("click", loadToday);

  if(!renderInitial()){
    loadToday();
  }
</script>
"""


ARCHIVE_BODY_INNER = """
<div class="wrap">
  <header>
    <div class="brand">__APP_NAME__ Arkiv</div>
    <div class="nav">
      <a href="/">Analyse</a>
      <a class="cta" href="/premium">Premium</a>
    </div>
  </header>

  __NAV_TABS__

  <div class="grid" style="grid-template-columns:1fr">
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

      <div class="btnrow" style="margin-top:12px">
        <input id="key" placeholder="Premium-nøkkel" autocomplete="off" />
        <button class="cta" id="btnSave">Lagre</button>
        <button id="btnClear">Fjern</button>
        <button id="btnLoad">Last arkiv</button>
      </div>

      <div class="btnrow" style="margin-top:10px">
        <input id="email" placeholder="E-post for varsel (premium)" autocomplete="email" />
        <button id="btnEmail">Aktiver e-postvarsel</button>
      </div>

      <div class="btnrow" style="margin-top:12px">
        <input id="payEmail" placeholder="E-post for kjøp (Stripe)" autocomplete="email" />
        <button class="cta" id="btnPay">Kjøp premium</button>
      </div>

      <div id="status" class="small" style="margin-top:10px"></div>

      <div class="card" style="margin-top:12px">
        <div style="font-weight:900">Signalhistorikk (siste 30 signaler)</div>
        <div class="small" id="statsBox" style="margin-top:8px">Laster…</div>
      </div>

      <table id="tbl" style="display:none">
        <thead><tr><th>Dato</th><th>Pris</th><th>Signal</th><th>7d</th><th>30d</th><th>Notat</th></tr></thead>
        <tbody id="body"></tbody>
      </table>

      <div class="small" style="margin-top:10px">
        API: <code>/api/history</code> med header <code>x-api-key</code>.
      </div>
    </div>
  </div>

  __FOOTER__
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

      const s = data.stats || {};
      const fmt = (x)=> (x==null ? "–" : (Number(x).toFixed(1) + " %"));
      $("statsBox").innerHTML = `
        Bullish: <b>${fmt(s.bullish_avg_7d)}</b> etter 7d<br/>
        Bearish: <b>${fmt(s.bearish_avg_7d)}</b> etter 7d<br/>
        Treffsikkerhet: <b>${s.hit_rate_7d==null ? "–" : (Number(s.hit_rate_7d).toFixed(0) + " %")}</b>
      `;

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
      setStatus("E-postvarsel aktivert ✅");
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
"""


SUCCESS_TEMPLATE = """
<div class="wrap">
  <header>
    <div class="brand">__APP_NAME__</div>
    <div class="nav">
      <a href="/">Analyse</a>
      <a href="/archive">Arkiv</a>
      <a class="cta" href="/premium">Premium</a>
    </div>
  </header>

  <section class="hero">
    <h1>Betaling registrert</h1>
    <p>Hvis Stripe-webhooken har rukket å kjøre, ligger premium-nøkkelen din klar under.</p>
  </section>

  __NAV_TABS__

  <section class="grid" style="grid-template-columns:1fr">
    <div class="card">
      <div class="title"><h2>Premium-nøkkel</h2><div class="muted">Aktivering</div></div>
      <div class="big" style="font-size:24px;word-break:break-word">__KEY__</div>
      <p class="muted" style="margin-top:12px">__STATUS__</p>
      <div class="btnrow">
        <button onclick="location.href='/archive'">Åpne arkiv</button>
        <button onclick="navigator.clipboard.writeText('__KEY_RAW__').catch(()=>{})">Kopier nøkkel</button>
      </div>
    </div>
  </section>

  __FOOTER__
</div>
"""


def seo_landing(request: Request, path: str, title: str, desc: str, h1: str, intro: str, mode: str, nav_active: str) -> HTMLResponse:
    initial_payload = get_public_today_payload(mode)

    body = _replace_many(
        SEO_LANDING_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__H1__": _escape_html(h1),
            "__INTRO__": _escape_html(intro),
            "__FOOTER__": footer_links(),
            "__MODE__": _escape_html(mode),
            "__NAV_TABS__": nav_tabs(nav_active),
            "__INITIAL_JSON__": json_for_html(initial_payload),
        },
    )
    return HTMLResponse(html_shell(request, title=title, description=desc, path=path, body_html=body))


# =============================================================================
# Pages
# =============================================================================

@app.get("/analysis")
def analysis_redirect():
    return RedirectResponse(url="/", status_code=302)


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    title = "Gullpris analyse | Gold price analysis | daglig gullbrief og markedssignal"
    desc = "Nøktern daglig analyse av gull og XAUUSD. Gold price analysis, trend, signal, forecast og makro."

    initial_payload = get_public_today_payload("analysis")

    body = _replace_many(
        INDEX_BODY_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__DESC__": _escape_html(desc),
            "__FOOTER__": footer_links(),
            "__NAV_TABS__": nav_tabs("analysis"),
            "__INITIAL_JSON__": json_for_html(initial_payload),
        },
    )
    return HTMLResponse(html_shell(request, title=title, description=desc, path="/", body_html=body))


@app.get("/premium", response_class=HTMLResponse)
def premium_page(request: Request) -> HTMLResponse:
    title = "Gullbrief Premium – gullpris analyse, signalhistorikk og arkiv"
    desc = "Premium: daglig rapport, signalhistorikk, flere nyheter, arkiv med 7d/30d etter signal, og e-postvarsler."
    body = _replace_many(
        PREMIUM_BODY_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__FOOTER__": footer_links(),
            "__NAV_TABS__": nav_tabs("premium"),
        },
    )
    return HTMLResponse(html_shell(request, title=title, description=desc, path="/premium", body_html=body))


@app.get("/archive", response_class=HTMLResponse)
def archive_page(request: Request) -> HTMLResponse:
    title = "Gullbrief arkiv – signalhistorikk og avkastning etter signal"
    desc = "Se siste snapshots gratis. Premium gir full historikk, signalhistorikk og 7d/30d etter signal."

    dates = get_archive_dates(last_n_days=SITEMAP_ARCHIVE_DAYS)

    if not dates:
        snap = read_public_snapshot()
        if snap:
            try:
                store_snapshot_if_needed(snap)
            except Exception:
                pass
        dates = get_archive_dates(last_n_days=SITEMAP_ARCHIVE_DAYS)

    links = []
    for d in dates[:60]:
        links.append(f'<li><a href="/archive/{_escape_html(d)}">Arkiv {_escape_html(d)}</a></li>')

    archive_map_html = (
        "<div class='wrap'><div class='card' style='margin-top:12px'>"
        "<div style='font-size:18px;font-weight:900'>Arkivkart</div>"
        "<div class='muted'>Lenker til de siste dagene.</div>"
        f"<ul>{''.join(links) if links else '<li class=\"muted\">Ingen arkiv-dager ennå.</li>'}</ul>"
        "</div></div>"
    )

    body = _replace_many(
        ARCHIVE_BODY_INNER,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__FOOTER__": footer_links(),
            "__NAV_TABS__": nav_tabs("premium"),
        },
    )

    body = archive_map_html + body
    return HTMLResponse(html_shell(request, title=title, description=desc, path="/archive", body_html=body))


@app.get("/archive/{day}", response_class=HTMLResponse)
def archive_day_page(request: Request, day: str) -> HTMLResponse:
    try:
        date.fromisoformat(day)
    except Exception:
        return HTMLResponse(
            html_shell(
                request,
                title=f"{APP_NAME} – Arkiv",
                description="Ugyldig dato.",
                path=f"/archive/{_escape_html(day)}",
                body_html="<div class='wrap'><div class='card'>Ugyldig dato.</div></div>",
            ),
            status_code=404,
        )

    snap = load_snapshot_for_date(day)
    if not snap:
        return HTMLResponse(
            html_shell(
                request,
                title=f"{APP_NAME} – Arkiv {day}",
                description="Ingen snapshot funnet for denne dagen ennå.",
                path=f"/archive/{day}",
                body_html="<div class='wrap'><div class='card'>Ingen snapshot funnet for denne dagen ennå.</div></div>",
                article_date=day,
            ),
            status_code=404,
        )

    sig = str(snap.get("signal") or "neutral").upper()
    price = safe_float(snap.get("price_usd"))
    chg = safe_float(snap.get("change_pct"))
    macro = (snap.get("macro_summary") or "").strip()
    reason = (snap.get("signal_reason") or "").strip()

    bits = []
    if price is not None:
        bits.append(f"Pris: {price:.2f} USD")
    if chg is not None:
        bits.append(f"Døgn: {chg:+.2f}%")
    header = " | ".join(bits) if bits else "Nøkkeltall: –"

    inner = """
    <div class="wrap">
      <header>
        <div class="brand">__APP_NAME__</div>
        <div class="nav">
          <a href="/">Analyse</a>
          <a href="/archive">Arkiv</a>
          <a class="cta" href="/premium">Premium</a>
        </div>
      </header>
      __NAV_TABS__
      <section class="hero">
        <h1>Gullpris analyse __DAY__</h1>
        <p>
          Dette er Gullbrief sin daglige analyse av gullpris og XAUUSD for __DAY__.
          Dette er også en gold price analysis / daily gold market update for __DAY__.
          Her finner du markedssignal, teknisk trend og makrodrivere som påvirker gull.
          Premium gir tilgang til full signalhistorikk, flere nyheter og arkiv.
        </p>
      </section>
      <section class="grid" style="grid-template-columns:1fr">
        <div class="card">
          <div class="title"><h2>Dagens snapshot</h2><div class="muted">__HEADER__</div></div>
          <div class="big">Signal: __SIG__</div>
          <p class="muted" style="margin-top:10px">__REASON__</p>
          <h2 style="margin-top:14px">Kort makro</h2>
          <p class="muted">__MACRO__</p>
          <div class="btnrow">
            <button onclick="location.href='/archive'">Åpne arkiv</button>
            <button class="cta" onclick="location.href='/premium'">Premium</button>
          </div>
        </div>
      </section>
      __FOOTER__
    </div>
    """
    body = _replace_many(
        inner,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__DAY__": _escape_html(day),
            "__SIG__": _escape_html(sig),
            "__HEADER__": _escape_html(header),
            "__REASON__": _escape_html(reason or "—"),
            "__MACRO__": _escape_html(macro or "—"),
            "__FOOTER__": footer_links(),
            "__NAV_TABS__": nav_tabs("premium"),
        },
    )

    title = f"{APP_NAME} arkiv {day} – {sig} | gold price analysis {day}"
    desc = f"{APP_NAME} snapshot {day}: {sig}. {header}. Gullpris analyse og gold price analysis for {day}."
    return HTMLResponse(
        html_shell(request, title=title, description=desc, path=f"/archive/{day}", body_html=body, article_date=day)
    )


@app.get("/success", response_class=HTMLResponse)
def success_page(request: Request, session_id: Optional[str] = None) -> HTMLResponse:
    key = "Nøkkel opprettes..."
    status_text = "Vent noen sekunder og oppdater siden hvis nøkkelen ikke vises med en gang."

    if session_id and stripe_ready():
        try:
            require_stripe(request)
            sess = stripe.checkout.Session.retrieve(session_id)
            customer_id = getattr(sess, "customer", None)
            subscription_id = getattr(sess, "subscription", None)
            email = ""
            try:
                email = sess.get("customer_details", {}).get("email", "")  # type: ignore
            except Exception:
                pass

            if customer_id or subscription_id or email:
                conn = _db()
                row = conn.execute(
                    "SELECT api_key,status FROM api_keys WHERE stripe_customer_id=? OR stripe_subscription_id=? OR email=? ORDER BY created_at DESC LIMIT 1",
                    (customer_id or "", subscription_id or "", email or ""),
                ).fetchone()
                conn.close()
                if row:
                    key = row["api_key"]
                    status_text = f"Status: {row['status']}. Lagre nøkkelen og bruk den på arkivsiden."
        except Exception:
            pass

    body = _replace_many(
        SUCCESS_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__KEY__": _escape_html(key),
            "__KEY_RAW__": _escape_html(key),
            "__STATUS__": _escape_html(status_text),
            "__FOOTER__": footer_links(),
            "__NAV_TABS__": nav_tabs("premium"),
        },
    )
    return HTMLResponse(html_shell(request, title=f"{APP_NAME} – Betaling OK", description="Premium aktivert.", path="/success", body_html=body))


@app.get("/gullpris-prognose", response_class=HTMLResponse)
def page_gullpris_prognose(request: Request) -> HTMLResponse:
    return seo_landing(
        request,
        path="/gullpris-prognose",
        title="Gullpris prognose | Gold price forecast | scenario for de neste dagene",
        desc="Gullpris prognose og gold price forecast basert på trend, signal og makrodrivere som renter, USD og geopolitikk.",
        h1="Gullpris prognose",
        intro="Fremoverskuende scenario for de neste 24–72 timene. Gold price forecast og XAUUSD outlook.",
        mode="forecast",
        nav_active="forecast",
    )


@app.get("/gullpris-analyse", response_class=HTMLResponse)
def page_gullpris_analyse(request: Request) -> HTMLResponse:
    return seo_landing(
        request,
        path="/gullpris-analyse",
        title="Gullpris analyse | Gold price analysis | daglig signal og makro",
        desc="Daglig gullpris analyse og gold price analysis: signal, trend og makrodrivere. Se dagens status og oppdateringer.",
        h1="Gullpris analyse",
        intro="Nøktern daglig analyse av gull. Fokus på trend, signal og makro. Gold price analysis og XAUUSD signal.",
        mode="analysis",
        nav_active="analysis",
    )


@app.get("/xauusd", response_class=HTMLResponse)
def page_xauusd(request: Request) -> HTMLResponse:
    return seo_landing(
        request,
        path="/xauusd",
        title="XAUUSD analyse | gold vs USD | signal og marked",
        desc="XAUUSD analyse: gull mot dollar, trend, signal og drivere. Gold vs USD, rates, dollar and risk sentiment.",
        h1="XAUUSD",
        intro="Spot gull mot USD med fokus på dollar, renter og risk-on/off.",
        mode="xauusd",
        nav_active="xauusd",
    )


@app.get("/gullpris-signal", response_class=HTMLResponse)
def page_gullpris_signal(request: Request) -> HTMLResponse:
    return seo_landing(
        request,
        path="/gullpris-signal",
        title="Gullpris signal | gold signal | bullish, bearish eller nøytral",
        desc="Gullpris signal og gold signal med forklaring. Premium viser signalhistorikk og 7d/30d etter signal.",
        h1="Gullpris signal",
        intro="Se dagens signal og hvorfor det er satt. Premium viser historikk, 7d/30d og treffsikkerhet.",
        mode="analysis",
        nav_active="signal",
    )


@app.get("/gullpris", response_class=HTMLResponse)
def page_gullpris(request: Request) -> HTMLResponse:
    return seo_landing(
        request,
        path="/gullpris",
        title="Gullpris i dag | Gold price today | pris, signal og nyheter",
        desc="Gullpris i dag og gold price today: pris (USD), signal og de viktigste nyhetene som påvirker gull og XAUUSD.",
        h1="Gullpris i dag",
        intro="Dagens pris og signal, med korte drivere og relevante nyheter.",
        mode="analysis",
        nav_active="analysis",
    )


# =============================================================================
# Public API
# =============================================================================

@app.get("/api/public/today")
def api_public_today(mode: str = "analysis"):
    try:
        return JSONResponse(get_public_today_payload(mode))
    except Exception as e:
        return JSONResponse({"message": str(e)}, status_code=500)


@app.get("/api/public/teaser-history")
def api_public_teaser_history():
    rows = read_history(limit=50)
    rows = add_forward_returns(rows)
    items = rows[-3:] if rows else []
    return JSONResponse({"items": items, "count": len(items)})


# =============================================================================
# Private / premium API
# =============================================================================

@app.get("/api/brief")
def api_brief():
    try:
        data = get_cached_brief(force_refresh=False)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"message": str(e)}, status_code=500)


@app.get("/api/brief/refresh")
def api_brief_refresh():
    try:
        data = get_cached_brief(force_refresh=True)
        return JSONResponse({"ok": True, "updated_at": data.get("updated_at"), "version": data.get("version")})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


@app.get("/api/history")
def api_history(limit: int = 200, x_api_key: Optional[str] = Header(default=None)):
    if not is_valid_key(x_api_key):
        return JSONResponse({"message": "UNAUTHORIZED"}, status_code=401)

    limit = max(1, min(limit, 1000))
    rows = read_history(limit=limit)
    rows = add_forward_returns(rows)
    rows_out = list(reversed(rows))
    stats = signal_stats_last30(rows_out)
    return JSONResponse({"items": rows, "count": len(rows), "stats": stats})


@app.post("/api/premium/subscribe-email")
async def api_subscribe_email(request: Request, x_api_key: Optional[str] = Header(default=None)):
    if not is_valid_key(x_api_key):
        return JSONResponse({"message": "UNAUTHORIZED"}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        body = {}
    email = str(body.get("email") or "").strip()
    if "@" not in email:
        return JSONResponse({"message": "Ugyldig e-post."}, status_code=400)

    conn = _db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO email_subscriptions(api_key,email,created_at,last_notified_signal,last_daily_sent_date,last_macro_sent_date) VALUES(?,?,?,?,?,?)",
            (x_api_key, email, iso_now(), None, None, None),
        )
        conn.commit()
    finally:
        conn.close()

    return JSONResponse({"ok": True, "email": email})


# =============================================================================
# Social API
# =============================================================================

@app.get("/api/social/daily-post-text")
def api_social_daily_post_text(request: Request):
    try:
        data = get_cached_brief(force_refresh=False)
        post = build_daily_social_post(data, request)
        return JSONResponse(post)
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


@app.post("/api/social/daily-post")
def api_social_daily_post(request: Request, x_api_key: Optional[str] = Header(default=None)):
    if x_api_key != ADMIN_API_KEY:
        return JSONResponse({"message": "UNAUTHORIZED"}, status_code=401)

    try:
        data = get_cached_brief(force_refresh=False)
        post = build_daily_social_post(data, request)

        result = {
            "enabled": SOCIAL_DAILY_ENABLED,
            "configured": x_configured(),
            "text": post["text"],
        }

        if SOCIAL_DAILY_ENABLED and x_configured():
            send_result = send_social_post(post["text"])
            result["send_result"] = send_result
        else:
            result["send_result"] = {"ok": False, "message": "SOCIAL_DISABLED_OR_NOT_CONFIGURED"}

        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


# =============================================================================
# Stripe API
# =============================================================================

@app.post("/api/stripe/create-checkout")
async def api_stripe_create_checkout(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    email = str(body.get("email") or "").strip()
    if "@" not in email:
        return JSONResponse({"message": "Ugyldig e-post."}, status_code=400)

    try:
        env = require_stripe(request)
        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            customer_email=email,
            line_items=[{"price": env["price_id"], "quantity": 1}],
            success_url=env["success_url"],
            cancel_url=env["cancel_url"],
            allow_promotion_codes=True,
            metadata={"app": APP_NAME, "email_hash": _hash_email(email)},
        )
        return JSONResponse({"ok": True, "url": session.url})
    except Exception as e:
        return JSONResponse({"message": str(e)}, status_code=500)


@app.post("/api/stripe/webhook")
async def api_stripe_webhook(request: Request):
    raw = await request.body()
    sig = request.headers.get("stripe-signature", "")

    try:
        env = require_stripe(None)
        if env["webhook_secret"]:
            event = stripe.Webhook.construct_event(raw, sig, env["webhook_secret"])
        else:
            event = json.loads(raw.decode("utf-8"))
    except Exception as e:
        return JSONResponse({"error": "BAD_SIGNATURE", "message": str(e)}, status_code=400)

    event_id = str(event.get("id") or "")
    event_type = str(event.get("type") or "")
    if event_id and _already_processed(event_id):
        return JSONResponse({"ok": True, "duplicate": True})

    data_obj = event.get("data", {}).get("object", {})

    try:
        if event_type == "checkout.session.completed":
            customer_id = str(data_obj.get("customer") or "")
            subscription_id = str(data_obj.get("subscription") or "")
            email = (
                str(data_obj.get("customer_details", {}).get("email") or "")
                or str(data_obj.get("customer_email") or "")
            )
            if customer_id or subscription_id or email:
                _upsert_key_for_stripe(email=email, customer_id=customer_id, subscription_id=subscription_id)
                if customer_id:
                    _set_key_status_for_customer(customer_id, "active")
                if subscription_id:
                    _set_key_status_for_subscription(subscription_id, "active")

        elif event_type in ("customer.subscription.created", "customer.subscription.updated"):
            customer_id = str(data_obj.get("customer") or "")
            subscription_id = str(data_obj.get("id") or "")
            status = str(data_obj.get("status") or "")
            mapped = "active" if status in ("active", "trialing") else "inactive"
            if customer_id:
                _set_key_status_for_customer(customer_id, mapped)
            if subscription_id:
                _set_key_status_for_subscription(subscription_id, mapped)

        elif event_type in ("customer.subscription.deleted",):
            customer_id = str(data_obj.get("customer") or "")
            subscription_id = str(data_obj.get("id") or "")
            if customer_id:
                _set_key_status_for_customer(customer_id, "inactive")
            if subscription_id:
                _set_key_status_for_subscription(subscription_id, "inactive")

        if event_id:
            _mark_processed(event_id, event_type)

        return JSONResponse({"ok": True, "type": event_type})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


# =============================================================================
# Utility endpoints
# =============================================================================

@app.get("/health")
def health():
    snapshot = read_public_snapshot()
    return JSONResponse(
        {
            "status": "ok",
            "ts": iso_now(),
            "yahoo_symbol": YAHOO_SYMBOL,
            "cache_ttl_seconds": CACHE_TTL_SECONDS,
            "openai_enabled": bool(OPENAI_API_KEY),
            "rss_feeds": RSS_FEEDS,
            "history_path": HISTORY_PATH,
            "db_path": DB_PATH,
            "public_snapshot_path": PUBLIC_SNAPSHOT_PATH,
            "public_snapshot_exists": bool(snapshot),
            "admin_key_configured": bool(ADMIN_API_KEY),
            "stripe_enabled": stripe_ready(),
            "stripe_secret_len": len(stripe_env()["secret_key"]),
            "stripe_price_id_prefix": stripe_env()["price_id"][:10] + "..." if stripe_env()["price_id"] else "",
            "stripe_webhook_secret_set": bool(stripe_env()["webhook_secret"]),
            "smtp_enabled": brevo_configured(),
            "social_daily_enabled": SOCIAL_DAILY_ENABLED,
            "social_configured": x_configured(),
            "version": "3.9",
        }
    )


@app.get("/robots.txt")
def robots_txt(request: Request):
    base = get_base_url(request)
    txt = (
        f"User-agent: *\n"
        f"Allow: /\n\n"
        f"Sitemap: {base}/sitemap.xml\n"
        f"Sitemap: {base}/news-sitemap.xml\n"
    )
    return PlainTextResponse(txt)


@app.get("/feed.xml")
def feed_xml(request: Request):
    base = get_base_url(request)
    rows = list(reversed(read_history(limit=max(FEED_ITEMS, 5))))
    if not rows:
        try:
            snap = read_public_snapshot()
            rows = [snap] if snap else []
        except Exception:
            rows = []

    items = []
    for r in rows[:FEED_ITEMS]:
        updated = str(r.get("updated_at") or iso_now())
        title = f"{APP_NAME}: {str(r.get('signal') or 'neutral').upper()} | {updated[:10]}"
        link = f"{base}/gullpris-analyse"
        desc = _escape_html(str(r.get("macro_summary") or ""))
        pub = updated
        items.append(
            f"<item><title>{_escape_html(title)}</title>"
            f"<link>{_escape_html(link)}</link>"
            f"<guid>{_escape_html(link)}#{_escape_html(updated)}</guid>"
            f"<pubDate>{_escape_html(pub)}</pubDate>"
            f"<description>{desc}</description></item>"
        )

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel>'
        f"<title>{_escape_html(APP_NAME)} feed</title>"
        f"<link>{_escape_html(base)}</link>"
        f"<description>{_escape_html(APP_NAME)} – siste signaler og analyser</description>"
        + "".join(items)
        + "</channel></rss>"
    )
    return Response(content=xml, media_type="application/rss+xml")


@app.get("/sitemap.xml")
def sitemap_xml(request: Request):
    base = get_base_url(request)

    static_urls = [
        "/",
        "/gullpris",
        "/gullpris-analyse",
        "/gullpris-prognose",
        "/gullpris-signal",
        "/xauusd",
        "/premium",
        "/archive",
        "/feed.xml",
    ]

    archive_urls = [f"/archive/{d}" for d in get_archive_dates(last_n_days=SITEMAP_ARCHIVE_DAYS)]

    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

    for p in static_urls + archive_urls:
        changefreq = "daily" if p not in ("/premium", "/archive") else "weekly"
        parts.append(
            "<url>"
            f"<loc>{_escape_html(base + p)}</loc>"
            f"<changefreq>{changefreq}</changefreq>"
            "</url>"
        )

    parts.append("</urlset>")
    return Response("".join(parts), media_type="application/xml")


@app.get("/news-sitemap.xml")
def news_sitemap(request: Request):
    base = get_base_url(request)
    dates = get_archive_dates(last_n_days=30)

    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">')

    for d in dates:
        parts.append(f"""
<url>
<loc>{base}/archive/{d}</loc>
<news:news>
<news:publication>
<news:name>Gullbrief</news:name>
<news:language>no</news:language>
</news:publication>
<news:publication_date>{d}</news:publication_date>
<news:title>Gullpris analyse {d}</news:title>
</news:news>
</url>
""")

    parts.append("</urlset>")
    return Response("".join(parts), media_type="application/xml")


@app.get(f"/{GOOGLE_SITE_VERIFICATION}")
def google_site_verification():
    return PlainTextResponse(GOOGLE_SITE_VERIFICATION)