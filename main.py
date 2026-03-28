from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
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
from fastapi import Cookie, FastAPI, Form, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles


# =============================================================================
# Gullbrief main.py – v4.4
# - Bevarer eksisterende snapshot/history/signal/Stripe/X/logikk
# - Legger til eToro affiliate-bokser på utvalgte sider
# - Legger til nye trade gold-sider (NO/EN)
# - Forbedrer robust arkiv-/news-fallback ved deploy
# =============================================================================


# =============================================================================
# Config
# =============================================================================

APP_NAME = os.getenv("APP_NAME", "Gullbrief").strip()
APP_VERSION = "4.5"

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
NEWS_PATH = os.getenv("NEWS_PATH", "data/news.json").strip()
NEWS_ARCHIVE_PATH = os.getenv("NEWS_ARCHIVE_PATH", "data/news_archive.jsonl").strip()

ADMIN_API_KEY = os.getenv("PREMIUM_API_KEY", "gullbrief-dev").strip()
BASE_URL = os.getenv("BASE_URL", "").strip().rstrip("/")

BREVO_API_KEY = os.getenv("BREVO_API_KEY", "").strip()
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "").strip()
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", APP_NAME).strip()

STRIPE_SUCCESS_URL_DEFAULT = os.getenv("STRIPE_SUCCESS_URL", "").strip()
STRIPE_CANCEL_URL_DEFAULT = os.getenv("STRIPE_CANCEL_URL", "").strip()

CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", SMTP_FROM_EMAIL or "kontakt@gullbrief.no").strip()
LEGAL_COMPANY_NAME = os.getenv("LEGAL_COMPANY_NAME", APP_NAME).strip()
LEGAL_ADDRESS = os.getenv("LEGAL_ADDRESS", "").strip()
LEGAL_ORGNO = os.getenv("LEGAL_ORGNO", "").strip()

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

MAGIC_LINK_TTL_MINUTES = int(os.getenv("MAGIC_LINK_TTL_MINUTES", "30"))
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "30"))
SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "gullbrief_session").strip() or "gullbrief_session"
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "false").strip().lower() == "true"

APP_SECRET = os.getenv("APP_SECRET", "").strip()
if not APP_SECRET:
    APP_SECRET = ADMIN_API_KEY or "gullbrief-dev-secret"

NEWS_DAILY_ENABLED = os.getenv("NEWS_DAILY_ENABLED", "true").strip().lower() == "true"
NEWS_PUBLISHER_NAME = os.getenv("NEWS_PUBLISHER_NAME", APP_NAME).strip() or APP_NAME
NEWS_DEFAULT_AUTHOR = os.getenv("NEWS_DEFAULT_AUTHOR", APP_NAME).strip() or APP_NAME
NEWS_PUBLISHER_LOGO = os.getenv("NEWS_PUBLISHER_LOGO", "/static/apple-touch-icon.png").strip() or "/static/apple-touch-icon.png"

ETORO_AFFILIATE_NO = os.getenv(
    "ETORO_AFFILIATE_NO",
    "https://med.etoro.com/B7987_A128914_TClick_Sgullbrief_no.aspx",
).strip()

ETORO_AFFILIATE_EN = os.getenv(
    "ETORO_AFFILIATE_EN",
    "https://med.etoro.com/B12087_A128914_TClick_Sgullbrief_en.aspx",
).strip()

AFFILIATE_DISCLAIMER_NO = (
    "Noen lenker på denne siden kan være affiliate-lenker. Vi kan motta provisjon dersom du registrerer deg via dem. "
    "Trading innebærer risiko og passer ikke for alle investorer."
)
AFFILIATE_DISCLAIMER_EN = (
    "Some links on this site may be affiliate links. We may receive a commission if you register through them. "
    "Trading involves risk and may not be suitable for all investors."
)

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

EN_NEWS_TOPICS = [
    ("news", "gold-market-update", "Gold market update"),
    ("analysis", "gold-price-forecast", "Gold price forecast"),
]

NO_NEWS_TOPICS = [
    ("news", "gull-marked-oppdatering", "Gull marked oppdatering"),
    ("analysis", "gullpris-analyse", "Gullpris analyse"),
]


# =============================================================================
# App + CORS + Static
# =============================================================================

app = FastAPI(title=f"{APP_NAME} Backend", version=APP_VERSION, docs_url=None, redoc_url=None)

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


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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


def extract_levels(text: str):
    support = None
    resistance = None

    s = re.search(r"Støttenivå.*?([0-9]+\.[0-9]+)", text)
    r = re.search(r"Motstand.*?([0-9]+\.[0-9]+)", text)

    if s:
        support = s.group(1)
    if r:
        resistance = r.group(1)

    return support, resistance


def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 25) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))


def http_get_text(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 25) -> str:
    h = headers or {}
    h.setdefault("User-Agent", "Mozilla/5.0 (compatible; Gullbrief/4.4)")
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


def absolute_url(base: str, path: str) -> str:
    if not path:
        return base
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}"


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


def read_news_store() -> Dict[str, Any]:
    data = read_json_file(NEWS_PATH)
    if not isinstance(data, dict):
        return {"version": APP_VERSION, "updated_at": iso_now(), "articles": []}
    if not isinstance(data.get("articles"), list):
        data["articles"] = []
    return data


def write_news_store(data: Dict[str, Any]) -> None:
    payload = dict(data)
    payload["version"] = APP_VERSION
    payload["updated_at"] = iso_now()
    if not isinstance(payload.get("articles"), list):
        payload["articles"] = []
    write_json_file_atomic(NEWS_PATH, payload)


def json_for_html(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False).replace("</", "<\\/").replace("<!--", "<\\!--")


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def slugify(value: str) -> str:
    s = (value or "").strip().lower()
    replacements = {
        "æ": "ae",
        "ø": "o",
        "å": "a",
        "ä": "a",
        "ö": "o",
        "ü": "u",
        "é": "e",
        "è": "e",
        "ê": "e",
    }
    for a, b in replacements.items():
        s = s.replace(a, b)

    out = []
    prev_dash = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                out.append("-")
                prev_dash = True

    text = "".join(out).strip("-")
    return text or "article"


def session_expires_iso(days: int = SESSION_TTL_DAYS) -> str:
    return (utc_now() + timedelta(days=days)).isoformat()


def magic_expires_iso(minutes: int = MAGIC_LINK_TTL_MINUTES) -> str:
    return (utc_now() + timedelta(minutes=minutes)).isoformat()


def is_not_expired(dt_str: str) -> bool:
    dt = parse_iso_or_rss(dt_str)
    if not dt:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt >= utc_now()


def sign_token(value: str, purpose: str) -> str:
    msg = f"{purpose}:{value}".encode("utf-8")
    return hmac.new(APP_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def generate_token_urlsafe(nbytes: int = 24) -> str:
    return secrets.token_urlsafe(nbytes)


def build_signed_magic_token(email: str, token: str) -> str:
    email_n = normalize_email(email)
    payload = f"{email_n}|{token}|{sign_token(email_n + '|' + token, 'magic')}"
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("utf-8")


def parse_signed_magic_token(value: str) -> Optional[Tuple[str, str]]:
    try:
        raw = base64.urlsafe_b64decode(value.encode("utf-8")).decode("utf-8")
        email, token, sig = raw.split("|", 2)
        if sig != sign_token(email + "|" + token, "magic"):
            return None
        return normalize_email(email), token
    except Exception:
        return None


def _short_hash(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()[:24]


def request_ip_hash(request: Optional[Request]) -> Optional[str]:
    if request is None:
        return None
    ip = (
        request.headers.get("x-forwarded-for", "").split(",")[0].strip()
        or request.headers.get("cf-connecting-ip", "").strip()
        or (request.client.host if request.client else "")
    )
    return _short_hash(ip) if ip else None


def request_user_agent_hash(request: Optional[Request]) -> Optional[str]:
    if request is None:
        return None
    ua = request.headers.get("user-agent", "").strip()
    return _short_hash(ua) if ua else None


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
    base_string = "&".join([method.upper(), enc(url), enc(param_string)])

    signing_key = f"{enc(consumer_secret)}&{enc(token_secret)}"
    digest = hmac.new(signing_key.encode("utf-8"), base_string.encode("utf-8"), hashlib.sha1).digest()
    signature = base64.b64encode(digest).decode("utf-8")

    oauth_params["oauth_signature"] = signature

    header = "OAuth " + ", ".join(f'{enc(k)}="{enc(v)}"' for k, v in sorted(oauth_params.items()))
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
            headers={"Authorization": auth_header, "Content-Type": "application/json"},
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

    cur.execute(
        """
      CREATE TABLE IF NOT EXISTS api_keys (
        api_key TEXT PRIMARY KEY,
        email TEXT,
        status TEXT NOT NULL DEFAULT 'inactive',
        created_at TEXT NOT NULL,
        stripe_customer_id TEXT,
        stripe_subscription_id TEXT
      )
    """
    )

    cur.execute(
        """
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
    """
    )

    cur.execute(
        """
      CREATE TABLE IF NOT EXISTS stripe_events (
        event_id TEXT PRIMARY KEY,
        event_type TEXT,
        created_at TEXT NOT NULL
      )
    """
    )

    cur.execute(
        """
      CREATE TABLE IF NOT EXISTS users (
        email TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        premium_status TEXT NOT NULL DEFAULT 'inactive',
        stripe_customer_id TEXT,
        stripe_subscription_id TEXT,
        last_login_at TEXT,
        last_magic_link_sent_at TEXT
      )
    """
    )

    cur.execute(
        """
      CREATE TABLE IF NOT EXISTS magic_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL,
        token TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        consumed_at TEXT,
        next_url TEXT,
        api_key TEXT,
        ip_hash TEXT,
        user_agent_hash TEXT
      )
    """
    )

    cur.execute(
        """
      CREATE TABLE IF NOT EXISTS web_sessions (
        session_token TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        api_key TEXT,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        revoked_at TEXT,
        last_seen_at TEXT,
        ip_hash TEXT,
        user_agent_hash TEXT
      )
    """
    )

    _try_add_column(conn, "email_subscriptions", "last_macro_sent_date TEXT")
    _try_add_column(conn, "users", "last_login_at TEXT")
    _try_add_column(conn, "users", "last_magic_link_sent_at TEXT")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_email ON api_keys(email)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_customer ON api_keys(stripe_customer_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_subscription ON api_keys(stripe_subscription_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_customer ON users(stripe_customer_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_subscription ON users(stripe_subscription_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_magic_links_email ON magic_links(email)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_email ON web_sessions(email)")

    conn.commit()
    conn.close()


# =============================================================================
# History + news file helpers
# =============================================================================

def _ensure_history_dir() -> pathlib.Path:
    p = pathlib.Path(HISTORY_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _read_jsonl_objects(path_str: str) -> List[Dict[str, Any]]:
    p = pathlib.Path(path_str)
    if not p.exists():
        return []

    items: List[Dict[str, Any]] = []
    try:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        items.append(obj)
                except Exception:
                    continue
    except Exception:
        return []

    return items


def load_news_archive() -> List[Dict[str, Any]]:
    return _read_jsonl_objects(NEWS_ARCHIVE_PATH)


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
        "version": data.get("version", APP_VERSION),
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
        "forecast_en": data.get("forecast_en", ""),
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


def ensure_snapshot_persisted_from_public() -> None:
    try:
        if read_history(limit=1):
            return
        snap = read_public_snapshot()
        if snap:
            store_snapshot_if_needed(snap)
    except Exception:
        pass


def get_history_rows_resilient(limit: int = 500) -> List[Dict[str, Any]]:
    rows = read_history(limit=limit)
    if rows:
        return rows

    snap = read_public_snapshot()
    if snap:
        return [snap]

    if CACHE.data:
        return [CACHE.data]

    return []


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
    rows = get_history_rows_resilient(limit=2000)
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
    rows = get_history_rows_resilient(limit=4000)
    best: Optional[Dict[str, Any]] = None

    for r in rows:
        d = date_yyyy_mm_dd_from_iso_or_rss(str(r.get("updated_at") or ""))
        if d == day:
            best = r

    if best:
        return best

    snap = read_public_snapshot()
    if snap:
        d = date_yyyy_mm_dd_from_iso_or_rss(str(snap.get("updated_at") or ""))
        if d == day:
            return snap

    return None


# =============================================================================
# Startup
# =============================================================================

@app.on_event("startup")
def _startup() -> None:
    init_db()
    snap = read_public_snapshot()
    if snap:
        CACHE.data = snap
        CACHE.ts = time.time()
    ensure_snapshot_persisted_from_public()


# =============================================================================
# Users / premium state
# =============================================================================

def ensure_user(email: str) -> None:
    email_n = normalize_email(email)
    if not email_n:
        return
    conn = _db()
    now = iso_now()
    conn.execute(
        """
        INSERT INTO users(email, created_at, updated_at, premium_status)
        VALUES(?,?,?,?)
        ON CONFLICT(email) DO UPDATE SET updated_at=excluded.updated_at
        """,
        (email_n, now, now, "inactive"),
    )
    conn.commit()
    conn.close()


def get_user_by_email(email: str) -> Optional[sqlite3.Row]:
    email_n = normalize_email(email)
    if not email_n:
        return None
    conn = _db()
    row = conn.execute("SELECT * FROM users WHERE email=?", (email_n,)).fetchone()
    conn.close()
    return row


def update_user_premium_state(
    *,
    email: str,
    premium_status: str,
    stripe_customer_id: str = "",
    stripe_subscription_id: str = "",
) -> None:
    email_n = normalize_email(email)
    if not email_n:
        return
    conn = _db()
    now = iso_now()
    conn.execute(
        """
        INSERT INTO users(email, created_at, updated_at, premium_status, stripe_customer_id, stripe_subscription_id)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(email) DO UPDATE SET
          updated_at=excluded.updated_at,
          premium_status=excluded.premium_status,
          stripe_customer_id=CASE WHEN excluded.stripe_customer_id!='' THEN excluded.stripe_customer_id ELSE users.stripe_customer_id END,
          stripe_subscription_id=CASE WHEN excluded.stripe_subscription_id!='' THEN excluded.stripe_subscription_id ELSE users.stripe_subscription_id END
        """,
        (email_n, now, now, premium_status, stripe_customer_id, stripe_subscription_id),
    )
    conn.commit()
    conn.close()


def get_user_premium_status(email: str) -> str:
    row = get_user_by_email(email)
    if not row:
        return "inactive"
    return str(row["premium_status"] or "inactive")


def email_has_active_premium(email: str) -> bool:
    email_n = normalize_email(email)
    if not email_n:
        return False

    conn = _db()
    try:
        user_row = conn.execute("SELECT premium_status FROM users WHERE email=?", (email_n,)).fetchone()
        if user_row and str(user_row["premium_status"] or "inactive") == "active":
            return True

        key_row = conn.execute(
            "SELECT status FROM api_keys WHERE email=? ORDER BY created_at DESC LIMIT 1",
            (email_n,),
        ).fetchone()
        return bool(key_row) and str(key_row["status"] or "inactive") == "active"
    finally:
        conn.close()


# =============================================================================
# Premium keys / subscriptions
# =============================================================================

def generate_api_key() -> str:
    return "gb_" + secrets.token_urlsafe(24)


def create_api_key(email: str, status: str = "inactive") -> str:
    email_n = normalize_email(email)
    key = generate_api_key()
    conn = _db()
    conn.execute(
        """
        INSERT INTO api_keys(api_key,email,status,created_at)
        VALUES(?,?,?,?)
        """,
        (key, email_n, status, iso_now()),
    )
    conn.commit()
    conn.close()
    return key


def get_active_api_key_for_email(email: str) -> Optional[str]:
    email_n = normalize_email(email)
    if not email_n:
        return None
    conn = _db()
    row = conn.execute(
        """
        SELECT api_key FROM api_keys
        WHERE email=? AND status='active'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (email_n,),
    ).fetchone()
    conn.close()
    return str(row["api_key"]) if row else None


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


def sync_premium_from_stripe(
    *,
    email: str,
    customer_id: str,
    subscription_id: str,
    status: str,
) -> None:
    email_n = normalize_email(email)
    conn = _db()

    row = None
    if customer_id:
        row = conn.execute("SELECT api_key FROM api_keys WHERE stripe_customer_id=? LIMIT 1", (customer_id,)).fetchone()

    if not row and subscription_id:
        row = conn.execute(
            "SELECT api_key FROM api_keys WHERE stripe_subscription_id=? LIMIT 1",
            (subscription_id,),
        ).fetchone()

    if not row and email_n:
        row = conn.execute(
            "SELECT api_key FROM api_keys WHERE email=? ORDER BY created_at DESC LIMIT 1",
            (email_n,),
        ).fetchone()

    if row:
        api_key = row["api_key"]
        conn.execute(
            """
            UPDATE api_keys
            SET email=?,
                status=?,
                stripe_customer_id=?,
                stripe_subscription_id=?
            WHERE api_key=?
            """,
            (email_n or None, status, customer_id or None, subscription_id or None, api_key),
        )
    else:
        api_key = generate_api_key()
        conn.execute(
            """
            INSERT INTO api_keys(api_key,email,status,created_at,stripe_customer_id,stripe_subscription_id)
            VALUES(?,?,?,?,?,?)
            """,
            (api_key, email_n or None, status, iso_now(), customer_id or None, subscription_id or None),
        )

    conn.commit()
    conn.close()

    if email_n:
        update_user_premium_state(
            email=email_n,
            premium_status=status,
            stripe_customer_id=customer_id,
            stripe_subscription_id=subscription_id,
        )


# =============================================================================
# Magic link login
# =============================================================================

def store_magic_link(*, email: str, token: str, next_url: str, request: Optional[Request] = None) -> None:
    conn = _db()
    api_key = get_active_api_key_for_email(email)
    conn.execute(
        """
        INSERT INTO magic_links(email,api_key,token,created_at,expires_at,next_url,ip_hash,user_agent_hash)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            normalize_email(email),
            api_key,
            token,
            iso_now(),
            magic_expires_iso(),
            next_url,
            request_ip_hash(request),
            request_user_agent_hash(request),
        ),
    )
    conn.commit()
    conn.close()


def consume_magic_link(token: str, request: Optional[Request] = None) -> Optional[Dict[str, Any]]:
    conn = _db()
    row = conn.execute("SELECT * FROM magic_links WHERE token=?", (token,)).fetchone()

    if not row:
        conn.close()
        return None

    if row["consumed_at"]:
        conn.close()
        return None

    if not is_not_expired(row["expires_at"]):
        conn.close()
        return None

    conn.execute("UPDATE magic_links SET consumed_at=? WHERE token=?", (iso_now(), token))

    session_token = generate_token_urlsafe(32)

    conn.execute(
        """
        INSERT INTO web_sessions(session_token,email,api_key,created_at,expires_at,ip_hash,user_agent_hash)
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            session_token,
            row["email"],
            row["api_key"],
            iso_now(),
            session_expires_iso(),
            request_ip_hash(request),
            request_user_agent_hash(request),
        ),
    )

    conn.commit()
    conn.close()

    return {"email": row["email"], "session_token": session_token, "next_url": row["next_url"]}


def revoke_web_session(token: Optional[str]) -> None:
    if not token:
        return
    conn = _db()
    conn.execute("UPDATE web_sessions SET revoked_at=? WHERE session_token=?", (iso_now(), token))
    conn.commit()
    conn.close()


def get_session_auth(session_token: Optional[str]) -> Dict[str, Any]:
    if not session_token:
        return {"authenticated": False}

    conn = _db()
    row = conn.execute("SELECT * FROM web_sessions WHERE session_token=?", (session_token,)).fetchone()

    if not row:
        conn.close()
        return {"authenticated": False}

    if row["revoked_at"]:
        conn.close()
        return {"authenticated": False}

    if not is_not_expired(row["expires_at"]):
        conn.close()
        return {"authenticated": False}

    email = row["email"]
    api_key = row["api_key"]

    conn.execute("UPDATE web_sessions SET last_seen_at=? WHERE session_token=?", (iso_now(), session_token))
    conn.commit()
    conn.close()

    return {
        "authenticated": True,
        "email": email,
        "api_key": api_key,
        "premium_active": email_has_active_premium(email),
        "via": "session",
    }


def resolve_auth_context(*, session_token: Optional[str], x_api_key: Optional[str]) -> Dict[str, Any]:
    session_auth = get_session_auth(session_token)

    if session_auth.get("authenticated"):
        return session_auth

    if x_api_key:
        conn = _db()
        row = conn.execute("SELECT * FROM api_keys WHERE api_key=?", (x_api_key,)).fetchone()
        conn.close()

        if row and str(row["status"]) == "active":
            return {
                "authenticated": True,
                "email": row["email"],
                "api_key": row["api_key"],
                "premium_active": True,
                "via": "api_key",
            }

    return {"authenticated": False}


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
    headers = {"User-Agent": "Mozilla/5.0 (compatible; Gullbrief/4.4)"}
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

    last = float(closes[-1])
    prev = float(closes[-2])
    change_pct = ((last - prev) / prev) * 100.0 if prev else None

    currency = None
    try:
        currency = chart["chart"]["result"][0]["meta"].get("currency")
    except Exception:
        pass

    return YahooPrice(symbol=symbol, last=last, prev=prev, change_pct=change_pct, currency=currency, ts=iso_now())


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
    s20 = sma(closes, 20)
    s50 = sma(closes, 50)
    rsi14v = rsi(closes, 14)
    tscore = trend_score_from_mas(last, s20, s50)

    if s20 is None or s50 is None:
        return "neutral", {"reason": "Kunne ikke beregne glidende snitt.", "rsi14": rsi14v, "trend_score": tscore}

    if last > s20 > s50:
        return "bullish", {"reason": "Pris over SMA20 og SMA50, med positiv trend.", "rsi14": rsi14v, "trend_score": tscore}

    if last < s20 < s50:
        return "bearish", {"reason": "Pris under SMA20 og SMA50, med negativ trend.", "rsi14": rsi14v, "trend_score": tscore}

    return "neutral", {"reason": "Blandet bilde mellom pris og glidende snitt.", "rsi14": rsi14v, "trend_score": tscore}


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

    headers = {"User-Agent": "Mozilla/5.0 (compatible; Gullbrief/4.4)"}
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

        seen.add(lk)
        title = (it.get("title") or "").strip()

        if is_gold_relevant_title(title):
            filtered.append(it)
        else:
            fallback.append(it)

    out = filtered[:limit]
    if len(out) < limit:
        out.extend(fallback[: limit - len(out)])

    return out[:limit]


# =============================================================================
# OpenAI bundle helpers
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
    out = {"analysis": "", "forecast": "", "forecast_en": "", "xauusd": "", "premium": ""}

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
        f"Du er {APP_NAME}. Du skriver korte, publiserbare markedstekster om gull for et finansnettsted.\n"
        "Viktig stil:\n"
        "- Skriv klart, konkret og nøkternt.\n"
        "- Skriv som en stram markedsredaktør, ikke som en meglerpresentasjon eller en AI-oppsummering.\n"
        "- Bruk korte og mellomlange setninger.\n"
        "- Ikke gjenta samme poeng med nye ord.\n"
        "- Hver tekst skal ha ett tydelig hovedpoeng.\n"
        "- Vær konkret om pris, signal, nivåer og drivere.\n"
        "- Ikke finn opp fakta. Hvis noe er uklart, si det tydelig.\n"
        "- forecast_en skal være på engelsk.\n"
        "- premium skal være klart mer innsiktsfull og mer verdifull enn gratisdelene.\n\n"

        "Unngå disse formuleringene:\n"
        "- 'markedet preges av'\n"
        "- 'alt i alt'\n"
        "- 'det er viktig å merke seg'\n"
        "- 'forblir en nøkkelressurs'\n"
        "- 'i et klima preget av'\n"
        "- 'kan indikere' når du heller kan skrive mer direkte\n"
        "- generelle og oppblåste formuleringer uten konkret innhold\n\n"

        "Svar KUN som gyldig JSON med nøyaktig disse nøklene:\n"
        '{"analysis":"...", "forecast":"...", "forecast_en":"...", "xauusd":"...", "premium":"..."}\n\n'

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

        "Skriv slik:\n"
        "- analysis: 9–12 linjer på norsk. Struktur: hva gjorde gull i dag, viktigste driver nå, teknisk bilde, hva må følges videre.\n"
        "- forecast: 9–12 linjer på norsk. Struktur: base, bull og bear, med tydelige drivere og nivåer.\n"
        "- forecast_en: 7–10 lines in English. Clear, specific and publication-ready.\n"
        "- xauusd: 7–10 linjer på norsk med tydelig fokus på USD, renter, nivåer og markedsdrivere.\n"
        "- premium: 20–30 linjer på norsk. Mer analytisk, mer konkret og mer nyttig enn gratistekstene. Bruk tydelige mellompoeng, men ikke overdriv formelt språk.\n"
    )

    try:
        from openai import OpenAI  # type: ignore

        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.responses.create(model=OPENAI_MODEL, input=prompt)
        txt = (resp.output_text or "").strip()

        i = txt.find("{")
        j = txt.rfind("}")
        if i >= 0 and j > i:
            txt = txt[i : j + 1]

        data = json.loads(txt)

        for k in ("analysis", "forecast", "forecast_en", "xauusd", "premium"):
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

    titles = [h.get("title", "").strip() for h in headlines if h.get("title")][:8]
    titles_block = "\n- ".join(titles) if titles else "(Ingen overskrifter tilgjengelig)"

    return (
        f"{APP_NAME} Premium ({datetime.now(timezone.utc).date().isoformat()})\n"
        f"Pris: {price_line} | Døgnendring: {chg_line} | RSI(14): {rsi_line} | Trend score: {ts_line}\n"
        f"Signal: {signal_state.upper()} ({signal_reason})\n"
        f"Støtte nær: {support_near} | Hovedstøtte: {support_major}\n"
        f"Motstand nær: {resistance_near} | Hovedmotstand: {resistance_major}\n"
        f"SMA20: {sma20_line} | SMA50: {sma50_line}\n\n"
        "Tittel:\n"
        "Utvidet premium-rapport\n\n"
        "Executive summary:\n"
        f"{analysis_text or 'Markedet fremstår blandet, og nyhetsbildet gir ikke alene grunnlag for et sterkt ensidig case akkurat nå.'}\n\n"
        "Marked akkurat nå:\n"
        f"{analysis_text or 'Markedet er avventende.'}\n\n"
        "Teknisk bilde:\n"
        f"Signalet står nå som {signal_state.upper()} basert på forholdet mellom pris, SMA20 og SMA50.\n"
        f"RSI(14) ligger på {rsi_line}, noe som gir en pekepinn på kortsiktig momentum.\n"
        f"Nær støtte ligger ved {support_near}, mens hovedstøtte ligger ved {support_major}.\n"
        f"Nær motstand ligger ved {resistance_near}, mens hovedmotstand ligger ved {resistance_major}.\n"
        f"SMA20 på {sma20_line} og SMA50 på {sma50_line} er sentrale nivåer for å vurdere om trenden holder eller svekkes.\n\n"
        "Makrodrivere:\n"
        f"{xauusd_text or 'USD, renter, realrenter og bred risk-on/off bør følges tett.'}\n\n"
        "Scenarier 24–72t:\n"
        f"{forecast_text or 'Base: videre konsolidering. Bull: svakere USD/renter og sterkere safe haven-etterspørsel. Bear: sterkere USD og høyere realrenter.'}\n\n"
        "Hva styrker signalet:\n"
        "- Pris holder seg over kortsiktig støtte og fortsetter å respektere SMA20\n"
        "- Ny makrostøy eller geopolitisk uro trekker kapital mot trygge havner\n\n"
        "Hva bryter signalet:\n"
        "- Pris klart under kortsiktig støtte og SMA20\n"
        "- Tydelig styrking i USD eller løft i renter og realrenter\n\n"
        "Watchlist neste 24–72t:\n"
        "- DXY\n"
        "- 10Y-renter / realrenter\n"
        "- Makrooverskrifter med direkte effekt på gull\n"
        "- Om pris nærmer seg eller avvises ved definerte motstandsnivåer\n\n"
        "Nyhetsdriver (utdrag):\n- "
        f"{titles_block}\n\n"
        "Konklusjon:\n"
        "Markedet måles best gjennom samspillet mellom teknisk struktur og makro. Når disse peker samme vei, øker kvaliteten i signalet. Når de spriker, stiger risikoen for støy og raske reverseringer."
    )


def fallback_analysis_text(signal_state: str) -> str:
    if signal_state == "bullish":
        return """Gullprisen holder seg i en positiv trend etter å ha etablert støtte over sentrale tekniske nivåer. Markedet støttes av etterspørsel etter sikre aktiva og et fortsatt usikkert makrobilde.

På kort sikt styres utviklingen særlig av renter, USD og nyhetsbildet. Dersom gull holder seg over støttenivåer kan markedet forsøke et nytt løft mot neste tekniske motstand.

Så lenge prisen ligger over viktige glidende gjennomsnitt vurderes den kortsiktige trenden fortsatt som moderat bullish."""

    if signal_state == "bearish":
        return """Gullprisen viser et svakere teknisk bilde etter å ha falt under viktige nivåer. Det bearish signalet reflekterer press fra renter, dollar og et mindre støttende makrobilde.

Videre utvikling vil i stor grad avhenge av makrotall fra USA og renteutviklingen. Dersom støttenivåer brytes kan markedet gå inn i en periode med svakere utvikling eller konsolidering."""

    return """Gullprisen beveger seg i et mer blandet kortsiktig bilde der markedet veier teknisk støtte mot makrodrivere som renter og dollar.

Uten et tydelig brudd opp eller ned er det mest sannsynlige scenarioet videre konsolidering mens markedet venter på nye makroimpulser."""


def fallback_forecast_text(signal_state: str, price_usd: Optional[float]) -> str:
    price_txt = f"{price_usd:.0f} USD" if isinstance(price_usd, (int, float)) else "dagens nivå"
    return f"""De neste 24–72 timene ventes gull å handle rundt {price_txt}. Basisscenarioet er videre konsolidering mens markedet reagerer på renter, USD og geopolitikk.

Et bullscenario kan oppstå dersom renter faller eller safe-haven-etterspørselen øker, mens et bearscenario kan oppstå dersom dollaren styrker seg og realrentene stiger."""


def fallback_forecast_en_text(signal_state: str, price_usd: Optional[float]) -> str:
    price_txt = f"${price_usd:,.0f}" if isinstance(price_usd, (int, float)) else "current levels"
    return f"""Gold is trading around {price_txt} in a mixed short-term environment as investors weigh macro data, yields and geopolitical headlines.

The most likely near-term scenario is continued consolidation unless a stronger macro catalyst moves the market."""


def fallback_xauusd_text(signal_state: str) -> str:
    return "Spot gold remains highly sensitive to movements in the US dollar, Treasury yields and global risk sentiment. Changes in real yields or the dollar index can quickly shift short-term momentum in XAUUSD."


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

    analysis_text = (bundle.get("analysis") or "").strip() or fallback_analysis_text(signal_state)
    forecast_text = (bundle.get("forecast") or "").strip() or fallback_forecast_text(signal_state, yp.last)
    forecast_en_text = (bundle.get("forecast_en") or "").strip() or fallback_forecast_en_text(signal_state, yp.last)
    xauusd_text = (bundle.get("xauusd") or "").strip() or fallback_xauusd_text(signal_state)
    premium_insight = (bundle.get("premium") or "").strip()

    return {
        "updated_at": yp.ts,
        "version": APP_VERSION,
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
        "forecast_en": forecast_en_text,
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
    if mode not in ("analysis", "analysis_en", "forecast", "forecast_en", "xauusd", "xauusd_en", "signal", "signal_en"):
        mode = "analysis"

    price_usd = safe_float(data.get("price_usd"))
    change_pct = safe_float(data.get("change_pct"))
    signal_state = str(data.get("signal") or "neutral").upper()
    signal_reason_no = str(data.get("signal_reason") or "")
    signal_reason_en = translate_signal_reason_to_english(signal_reason_no)
    forecast_en = str(data.get("forecast_en") or "").strip()
    xauusd_text = str(data.get("xauusd") or "").strip()
    analysis_text = str(data.get("analysis") or data.get("macro_summary") or "").strip()
    levels = data.get("levels") if isinstance(data.get("levels"), dict) else {}
    support_near = safe_float(levels.get("support_near"))
    resistance_near = safe_float(levels.get("resistance_near"))

    if mode == "forecast":
        summary = data.get("forecast") or data.get("macro_summary") or ""
    elif mode == "forecast_en":
        summary = forecast_en or data.get("forecast") or data.get("macro_summary") or ""
    elif mode == "analysis_en":
        price_txt = f"${price_usd:,.2f}" if price_usd is not None else "current levels"
        change_txt = f"{change_pct:+.2f}%" if change_pct is not None else "an unclear daily move"
        support_txt = f"${support_near:,.0f}" if support_near is not None else "near support"
        resistance_txt = f"${resistance_near:,.0f}" if resistance_near is not None else "near resistance"
        summary = (
            f"Gold is trading around {price_txt} with a daily move of {change_txt}. "
            f"The current signal is {signal_state}, with the technical read pointing to {signal_reason_en.lower() if signal_reason_en else 'a mixed near-term setup'}. "
            f"The market is watching support around {support_txt} and resistance near {resistance_txt}."
        )
    elif mode == "xauusd":
        summary = xauusd_text or analysis_text
    elif mode == "xauusd_en":
        summary = forecast_en or analysis_en or "Gold remains highly sensitive to the US dollar, Treasury yields and broader risk sentiment."
    elif mode == "signal_en":
        if signal_state == "BULLISH":
            base = "Today's signal is BULLISH. Gold remains in a constructive short-term technical structure."
        elif signal_state == "BEARISH":
            base = "Today's signal is BEARISH. Gold shows a weaker short-term technical structure."
        else:
            base = "Today's signal is NEUTRAL. Gold is trading in a mixed short-term setup without a clear directional edge."
        detail = forecast_en or xauusd_text
        summary = (base + (" " + detail if detail else "")).strip()
    elif mode == "signal":
        if signal_state == "BULLISH":
            base = "Dagens signal er BULLISH. Gullprisen ligger i en positiv teknisk struktur der prisbildet støttes av trend og glidende snitt."
        elif signal_state == "BEARISH":
            base = "Dagens signal er BEARISH. Gullprisen viser et svakere teknisk bilde med press på trend og glidende snitt."
        else:
            base = "Dagens signal er NEUTRAL. Markedet viser et blandet bilde uten et tydelig bullish eller bearish overtak akkurat nå."
        summary = (base + (" " + analysis_text if analysis_text else "")).strip()
    else:
        summary = analysis_text

    return {
        "updated_at": data.get("updated_at") or iso_now(),
        "version": data.get("version", APP_VERSION),
        "gold": {
            "price_usd": data.get("price_usd"),
            "change_pct": data.get("change_pct"),
        },
        "signal": {
            "state": data.get("signal", "neutral"),
            "reason_short": data.get("signal_reason", ""),
        },
        "macro": {
            "mode": mode,
            "summary_short": summary,
        },
        "headlines": (data.get("headlines") or [])[:FREE_HEADLINES_LIMIT],
        "headlines_total": len(data.get("headlines") or []),
        "headlines_free_limit": FREE_HEADLINES_LIMIT,
    }


def get_public_today_payload(mode: str = "analysis") -> Dict[str, Any]:
    data = get_public_brief(force_build=False)
    return map_to_public_today(data, mode)


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
        headers={"accept": "application/json", "content-type": "application/json", "api-key": BREVO_API_KEY},
        json=payload,
        timeout=20,
    )

    if r.status_code >= 400:
        raise RuntimeError(f"BREVO_HTTP_{r.status_code}: {r.text}")


def build_magic_link_url(request: Request, token: str) -> str:
    base = get_base_url(request)
    return f"{base}/auth/magic?t={quote(token)}"


def request_magic_link(email: str, request: Request, next_url: str = "/archive") -> Dict[str, Any]:
    email_n = normalize_email(email)
    if "@" not in email_n:
        raise RuntimeError("INVALID_EMAIL")

    ensure_user(email_n)

    token = build_signed_magic_token(email_n, generate_token_urlsafe(24))
    store_magic_link(email=email_n, token=token, next_url=next_url, request=request)

    link = build_magic_link_url(request, token)
    subject = f"{APP_NAME} – magic link"
    body = (
        f"Hei!\n\n"
        f"Klikk på denne lenken for å logge inn i {APP_NAME}:\n"
        f"{link}\n\n"
        f"Lenken er gyldig i cirka {MAGIC_LINK_TTL_MINUTES} minutter.\n\n"
        f"Hvis du har aktiv Premium på denne e-posten, får du tilgang automatisk etter innlogging.\n"
    )

    send_email(email_n, subject, body)

    conn = _db()
    conn.execute("UPDATE users SET last_magic_link_sent_at=?, updated_at=? WHERE email=?", (iso_now(), iso_now(), email_n))
    conn.commit()
    conn.close()

    return {"ok": True, "email": email_n, "link": link}


# =============================================================================
# Social / X
# =============================================================================

def build_daily_social_post(data: Dict[str, Any], request: Optional[Request] = None) -> Dict[str, Any]:
    signal_state = str(data.get("signal") or "neutral").upper()
    price = safe_float(data.get("price_usd"))
    change_pct = safe_float(data.get("change_pct"))
    link_base = get_base_url(request) if request else (BASE_URL or "https://gullbrief.no")
    link = f"{link_base}/gold-price-forecast"

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


def build_news_social_post(article: Dict[str, Any], request: Optional[Request] = None) -> Dict[str, Any]:
    article = normalize_article_for_display(article)

    base = get_base_url(request) if request else (BASE_URL or "https://gullbrief.no")
    path = str(article.get("path") or "/")
    url = absolute_url(base, path)

    title = str(article.get("title") or APP_NAME)
    summary = _clip_text(str(article.get("summary") or ""), 180)
    lang = str(article.get("lang") or "no")

    if lang == "en":
        text = (
            f"{title}\n\n"
            f"{summary}\n\n"
            f"{url}\n\n"
            f"Full analysis and signal update:\nhttps://gullbrief.no/premium\n\n"
            f"#gold #xauusd #macro #markets"
        )
    else:
        text = (
            f"{title}\n\n"
            f"{summary}\n\n"
            f"{url}\n\n"
            f"Full analyse og signaloppdatering:\nhttps://gullbrief.no/premium\n\n"
            f"#gull #gullpris #marked #økonomi"
        )

    return {"title": title, "url": url, "text": text, "lang": lang}


# =============================================================================
# Navigation / UI helpers
# =============================================================================

def is_english_active(active: str) -> bool:
    return active in {"gold_price", "gold_analysis", "gold_forecast", "gold_signal", "news", "trade_gold", "premium_en", "archive_en", "xauusd_en"}


def language_switch(active: str) -> str:
    current_is_en = is_english_active(active)
    mapping = {
        "analysis": "/gold-price-analysis",
        "forecast": "/gold-price-forecast",
        "gullpris": "/gold-price",
        "xauusd": "/xauusd-en",
        "signal": "/gold-signal",
        "nyheter": "/news",
        "trade_gull": "/trade-gold",
        "premium": "/premium-en",
        "archive": "/archive-en",
        "gold_price": "/gullpris",
        "gold_analysis": "/gullpris-analyse",
        "gold_forecast": "/gullpris-prognose",
        "gold_signal": "/gullpris-signal",
        "news": "/nyheter",
        "trade_gold": "/handle-gull",
        "premium_en": "/premium",
        "archive_en": "/archive",
        "xauusd_en": "/xauusd",
    }
    no_href = mapping.get(active, "/") if current_is_en else "/"
    en_href = mapping.get(active, "/gold-price") if not current_is_en else "/gold-price"
    no_cls = "lang-switch active" if not current_is_en else "lang-switch"
    en_cls = "lang-switch active" if current_is_en else "lang-switch"
    return (
        '<div class="lang-switches">'
        f'<a class="{no_cls}" href="{no_href}">NO</a>'
        '<span class="lang-sep">|</span>'
        f'<a class="{en_cls}" href="{en_href}">EN</a>'
        '</div>'
    )


def site_header(active: str) -> str:
    current_is_en = is_english_active(active)
    home_href = "/gold-price" if current_is_en else "/"
    return f'<header><div class="brand"><a href="{home_href}">{_escape_html(APP_NAME)}</a></div>{language_switch(active)}</header>'


def nav_tabs(active: str) -> str:
    if is_english_active(active):
        tabs = [
            ("/gold-price", "gold_price", "Gold price"),
            ("/gold-price-analysis", "gold_analysis", "Analysis"),
            ("/gold-price-forecast", "gold_forecast", "Forecast"),
            ("/xauusd-en", "xauusd_en", "XAUUSD"),
            ("/gold-signal", "gold_signal", "Signal"),
            ("/news", "news", "News"),
            ("/trade-gold", "trade_gold", "Trade gold"),
            ("/archive-en", "archive_en", "Archive"),
            ("/premium-en", "premium_en", "Premium"),
        ]
    else:
        tabs = [
            ("/gullpris", "gullpris", "Gullpris"),
            ("/gullpris-analyse", "analysis", "Analyse"),
            ("/gullpris-prognose", "forecast", "Prognose"),
            ("/xauusd", "xauusd", "XAUUSD"),
            ("/gullpris-signal", "signal", "Signal"),
            ("/nyheter", "nyheter", "Nyheter"),
            ("/handle-gull", "trade_gull", "Handle gull"),
            ("/archive", "archive", "Arkiv"),
            ("/premium", "premium", "Premium"),
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
        "logo": absolute_url(base, "/static/apple-touch-icon.png"),
        "inLanguage": "no",
        "potentialAction": {
            "@type": "SearchAction",
            "target": f"{base}/gullpris?q={{search_term_string}}",
            "query-input": "required name=search_term_string",
        },
    }
    return '<script type="application/ld+json">' + json.dumps(data, ensure_ascii=False) + "</script>"


def translate_headline_to_norwegian(title: str) -> str:
    if not title:
        return title

    replacements = [
        ("Gold price", "Gullpris"),
        ("gold price", "gullpris"),
        ("Gold", "Gull"),
        ("gold", "gull"),
        ("Oil", "Olje"),
        ("oil", "olje"),
        ("Market", "Marked"),
        ("market", "marked"),
        ("Markets", "Markeder"),
        ("markets", "markeder"),
        ("Inflation", "Inflasjon"),
        ("inflation", "inflasjon"),
        ("Dollar", "Dollar"),
        ("dollar", "dollar"),
        ("Energy", "Energi"),
        ("energy", "energi"),
        ("War", "krig"),
        ("war", "krig"),
        ("Rises", "stiger"),
        ("rises", "stiger"),
        ("Falls", "faller"),
        ("falls", "faller"),
        ("Forecast", "prognose"),
        ("forecast", "prognose"),
        ("Update", "oppdatering"),
        ("update", "oppdatering"),
    ]

    out = title
    for src, dst in replacements:
        out = out.replace(src, dst)

    return out


def jsonld_article(
    base: str,
    title: str,
    description: str,
    url_path: str,
    date_published: Optional[str] = None,
    lang: str = "no",
) -> str:
    data: Dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": "Article",
        "headline": title,
        "description": description,
        "inLanguage": lang,
        "mainEntityOfPage": {"@type": "WebPage", "@id": f"{base}{url_path}"},
        "publisher": {
            "@type": "Organization",
            "name": APP_NAME,
            "logo": {"@type": "ImageObject", "url": absolute_url(base, "/static/apple-touch-icon.png")},
        },
        "dateModified": iso_now(),
    }
    if date_published:
        data["datePublished"] = date_published
    return '<script type="application/ld+json">' + json.dumps(data, ensure_ascii=False) + "</script>"


def jsonld_news_article(base: str, article: Dict[str, Any]) -> str:
    lang = "en" if str(article.get("lang") or "") == "en" else "no"
    path = str(article.get("path") or "/")
    data = {
        "@context": "https://schema.org",
        "@type": "NewsArticle",
        "headline": str(article.get("title") or ""),
        "description": str(article.get("summary") or ""),
        "datePublished": str(article.get("published_at") or iso_now()),
        "dateModified": str(article.get("updated_at") or iso_now()),
        "inLanguage": lang,
        "mainEntityOfPage": {"@type": "WebPage", "@id": f"{base}{path}"},
        "author": {"@type": "Organization", "name": NEWS_DEFAULT_AUTHOR},
        "publisher": {
            "@type": "Organization",
            "name": NEWS_PUBLISHER_NAME,
            "logo": {"@type": "ImageObject", "url": absolute_url(base, NEWS_PUBLISHER_LOGO)},
        },
    }
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
  .brand a{color:var(--text);text-decoration:none}
  .brand a:hover{color:#fff0bf}
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
  input, textarea{
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
  pre{
    white-space:pre-wrap;
    font-family:inherit;
    line-height:1.6;
    margin:0;
  }
  footer{margin-top:22px;color:var(--muted);font-size:13px}
  .links{display:flex;gap:12px;flex-wrap:wrap;margin-top:6px}
  .links a{color:var(--muted)}
  .premiumhint{
    margin-top:12px;
    padding:14px 14px;
    border-radius:14px;
    background:rgba(212,175,55,.08);
    border:1px solid rgba(212,175,55,.18);
    color:#f1e2a7
  }
  .premiumbox{
    margin-top:14px;
    padding:16px;
    border-radius:16px;
    border:1px solid rgba(212,175,55,.24);
    background:
      radial-gradient(600px 240px at 0% 0%, rgba(212,175,55,.10), rgba(212,175,55,0) 60%),
      linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.02));
  }
  .premiumbox h3{
    margin:0 0 8px;
    font-size:18px;
    color:#f6e7ad;
  }
  .premiumbox p{
    margin:0;
    color:#d8d0b2;
  }
  .premiumbox-grid{
    display:grid;
    grid-template-columns:1fr;
    gap:10px;
    margin-top:12px;
  }
  @media (min-width:760px){
    .premiumbox-grid{grid-template-columns:repeat(2,1fr)}
  }
  .premiummini{
    padding:10px 12px;
    border-radius:12px;
    background:rgba(255,255,255,.04);
    border:1px solid rgba(255,255,255,.06);
    color:#efe7c5;
    font-size:14px;
  }
  .premiumcta{
    display:flex;
    gap:10px;
    flex-wrap:wrap;
    margin-top:14px;
  }
  .premiumcta a{
    display:inline-flex;
    align-items:center;
    justify-content:center;
    padding:10px 14px;
    border-radius:999px;
    font-weight:850;
  }
  .premiumcta .goldbtn{
    background:var(--gold);
    color:#10141b;
  }
  .premiumcta .ghostbtn{
    background:rgba(255,255,255,.08);
    color:var(--text);
  }
  .legal-card h2{margin-top:0}
  .legal-card h3{margin-top:22px;margin-bottom:8px;font-size:17px}
  .legal-card p, .legal-card li{color:var(--text)}
  .authbox{
    margin-top:16px;
    padding:16px;
    border-radius:16px;
    border:1px solid rgba(255,255,255,.08);
    background:rgba(255,255,255,.03);
  }
  .keypastebox{
    margin-top:18px;
    padding:16px;
    border-radius:16px;
    border:1px solid rgba(212,175,55,.18);
    background:rgba(212,175,55,.05);
  }
  .newslist h3{margin-top:0}
  .article-body p{margin:0 0 16px}
  .article-body h2{margin:26px 0 10px;font-size:22px;font-family:ui-serif,Georgia,Times}
  .content-block{margin-top:16px}
  .content-block h2{
    margin:0 0 10px;
    font-size:24px;
    font-family:ui-serif,Georgia,Times;
  }
  .content-block h3{
    margin:18px 0 8px;
    font-size:18px;
  }
  .content-block p{margin:0 0 14px;color:var(--text)}
  .content-block ul li{margin:8px 0}
  .affiliate-box{
    margin-top:16px;
    padding:18px;
    border-radius:16px;
    border:1px solid rgba(212,175,55,.16);
    background:
      radial-gradient(600px 220px at 0% 0%, rgba(212,175,55,.08), rgba(212,175,55,0) 60%),
      linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.02));
  }
  .affiliate-box h3{
    margin:0 0 8px;
    font-size:20px;
    color:#f2e2a6;
  }
  .affiliate-box p{
    margin:0;
    color:#d8d0b2;
  }
  .affiliate-actions{
    margin-top:12px;
    display:flex;
    gap:10px;
    flex-wrap:wrap;
  }
  .affiliate-btn{
    display:inline-flex;
    align-items:center;
    justify-content:center;
    padding:10px 14px;
    border-radius:999px;
    font-weight:850;
    background:var(--gold);
    color:#10141b;
  }
  .affiliate-disclaimer{
    margin-top:10px;
    font-size:12px;
    color:var(--muted);
  }
  .inline-guide-link{
    margin-top:14px;
    padding-top:14px;
    border-top:1px solid rgba(255,255,255,.06);
    color:var(--muted);
  }
  .inline-guide-link a{
    color:#f2e2a6;
    font-weight:700;
  }
  .archive-map{
    margin-bottom:16px;
  }
  .lang-switches{display:flex;align-items:center;gap:10px;color:var(--muted);font-size:13px;white-space:nowrap}
  .lang-switch{color:var(--muted);font-weight:700}
  .lang-switch.active{color:#fff}
  .lang-sep{color:var(--muted)}
  .mini-chart{margin-top:16px;height:84px;border-top:1px solid var(--line);padding-top:14px}
  .mini-chart svg{width:100%;height:68px;display:block}
  .mini-chart .chart-line{fill:none;stroke:var(--gold);stroke-width:2.4;stroke-linecap:round;stroke-linejoin:round}
  .mini-chart .chart-fill{fill:rgba(212,175,55,.10)}
  .mini-chart .chart-caption{margin-top:12px;padding-top:4px;font-size:12px;color:var(--muted)}
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
    lang: str = "no",
    extra_jsonld: str = "",
) -> str:
    base = get_base_url(request)
    canonical = f"{base}{path}"
    og_image = f"{base}/og.svg"

    robots = "index,follow,max-image-preview:large,max-snippet:-1,max-video-preview:-1"
    twitter_site_meta = f'<meta name="twitter:site" content="{_escape_html(TWITTER_SITE)}" />' if TWITTER_SITE else ""

    favicon_meta = '<link rel="icon" href="/static/favicon.ico" sizes="any" />' '<link rel="apple-touch-icon" href="/static/apple-touch-icon.png" />'

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
        f'<meta property="og:locale" content="{"en_US" if lang == "en" else "nb_NO"}" />'
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
        + jsonld_article(base, title, description, path, date_published=article_date, lang=lang)
        + extra_jsonld
    )

    return "<!doctype html>" f'<html lang="{lang}"><head>' + head + COMMON_STYLE + "</head><body>" + body_html + "</body></html>"


# =============================================================================
# Templates / boxes
# =============================================================================

def footer_links(is_en: bool = False) -> str:
    if is_en:
        return """
        <footer>
          <div class="links">
            <a href="/gold-price-analysis">Analysis</a>
            <a href="/gold-price-forecast">Forecast</a>
            <a href="/gold-price">Gold price</a>
            <a href="/xauusd-en">XAUUSD</a>
            <a href="/gold-signal">Signal</a>
            <a href="/news">News</a>
            <a href="/trade-gold">Trade gold</a>
            <a href="/premium-en">Premium</a>
            <a href="/archive-en">Archive</a>
            <a href="/kontakt">Contact</a>
            <a href="/terms">Terms</a>
            <a href="/privacy">Privacy</a>
          </div>
          <div style="margin-top:8px">© Gullbrief. Not investment advice.</div>
        </footer>
        """

    return """
    <footer>
      <div class="links">
        <a href="/gullpris-analyse">Analyse</a>
        <a href="/gullpris-prognose">Prognose</a>
        <a href="/gullpris">Gullpris</a>
        <a href="/xauusd">XAUUSD</a>
        <a href="/gullpris-signal">Signal</a>
        <a href="/nyheter">Nyheter</a>
        <a href="/handle-gull">Handle gull</a>
        <a href="/premium">Premium</a>
        <a href="/archive">Arkiv</a>
        <a href="/kontakt">Kontakt</a>
        <a href="/terms">Terms</a>
        <a href="/privacy">Privacy</a>
      </div>
      <div style="margin-top:8px">© Gullbrief. Ikke investeringsråd.</div>
    </footer>
    """


chart_html = """
      <div class="mini-chart">
        <svg id="priceChart" viewBox="0 0 400 68" preserveAspectRatio="none" aria-label="Price chart"></svg>
        <div class="chart-caption">7 dager / 7 days</div>
      </div>
"""

def premium_feature_box(resistance=None, support=None) -> str:
    return f"""
    <div class="premiumbox">
      <h3>⭐ Få hele analysen og neste markedsnivå</h3>
      <p>Få den utvidede rapporten med dypere markedskommentar, tekniske nivåer, scenarioarbeid, flere nyheter og signalhistorikk.</p>
      <p><strong>Neste tekniske nivåer </strong></p>
      <p>
      Motstandsnivå: <b>${resistance or "52•••"}</b><br>
      Støttenivå: <b>${support or "48•••"}</b><br>
      🔒 Kun tilgjengelig i Premium
      </p>
      <div class="premiumbox-grid">
        <div class="premiummini"><b>Utvidet premium-rapport</b><br/>Vesentlig lengre og mer utfyllende enn gratisanalysen.</div>
        <div class="premiummini"><b>Signalhistorikk</b><br/>Se hvordan tidligere bullish og bearish signaler utviklet seg etter 7 og 30 dager.</div>
        <div class="premiummini"><b>Flere markedssaker</b><br/>Gratis viser bare et utvalg. Premium gir bredere nyhetsbilde.</div>
        <div class="premiummini"><b>Arkiv + e-postvarsler</b><br/>Følg signalendringer og få den daglige rapporten sendt direkte.</div>
      </div>
      <div class="premiumcta">
        <a class="goldbtn" href="/premium">Åpne Premium</a>
        <a class="ghostbtn" href="/archive">Se arkiv</a>
      </div>
    </div>
    """


def premium_feature_box_en() -> str:
    return """
    <div class="premiumbox">
      <h3>⭐ Premium gives you more than a slightly longer note</h3>
      <p>Get the extended report with deeper market commentary, technical levels, scenario work, more headlines and signal history.</p>
      <div class="premiumbox-grid">
        <div class="premiummini"><b>Extended premium report</b><br/>Clearly longer and more detailed than the free analysis.</div>
        <div class="premiummini"><b>Signal history</b><br/>See how earlier bullish and bearish signals performed after 7 and 30 days.</div>
        <div class="premiummini"><b>More market headlines</b><br/>The free version only shows a smaller selection.</div>
        <div class="premiummini"><b>Archive + email alerts</b><br/>Follow signal changes and get the daily report delivered directly.</div>
      </div>
      <div class="premiumcta">
        <a class="goldbtn" href="/premium">Open Premium</a>
        <a class="ghostbtn" href="/archive">Open archive</a>
      </div>
    </div>
    """


def affiliate_box(lang: str = "no") -> str:
    is_en = lang == "en"
    title = "Want to trade gold yourself?" if is_en else "Vil du trade gull selv?"
    body = (
        "Gold can be traded directly in the market through XAUUSD using a trading platform."
        if is_en
        else "Gull kan handles direkte i markedet via XAUUSD gjennom en tradingplattform."
    )
    button = "Trade gold on eToro →" if is_en else "Handle gull hos eToro →"
    href = ETORO_AFFILIATE_EN if is_en else ETORO_AFFILIATE_NO
    disclaimer = AFFILIATE_DISCLAIMER_EN if is_en else AFFILIATE_DISCLAIMER_NO

    return f"""
    <div class="affiliate-box">
      <h3>{_escape_html(title)}</h3>
      <p>{_escape_html(body)}</p>
      <div class="affiliate-actions">
        <a class="affiliate-btn" href="{_escape_html(href)}" target="_blank" rel="nofollow sponsored">{_escape_html(button)}</a>
      </div>
      <div class="affiliate-disclaimer">{_escape_html(disclaimer)}</div>
    </div>
    """


def internal_trade_guide_link(lang: str = "no") -> str:
    if lang == "en":
        return """
        <div class="inline-guide-link">
          Want to learn how to trade gold? <a href="/trade-gold">Read our gold trading guide</a>.
        </div>
        """
    return """
    <div class="inline-guide-link">
      Vil du lære mer om hvordan man trader gull? <a href="/handle-gull">Se vår guide til gullhandel</a>.
    </div>
    """


def translate_signal_reason_to_english(reason: str) -> str:
    r = (reason or "").strip()
    mapping = {
        "Pris over SMA20 og SMA50, med positiv trend.": "Price above SMA20 and SMA50, with a positive trend.",
        "Pris under SMA20 og SMA50, med negativ trend.": "Price below SMA20 and SMA50, with a negative trend.",
        "Blandet bilde mellom pris og glidende snitt.": "Mixed picture between price and moving averages.",
        "For lite historikk til SMA20/SMA50. Setter nøytral.": "Too little history for SMA20/SMA50. Setting neutral.",
        "Kunne ikke beregne glidende snitt.": "Could not calculate moving averages.",
    }
    return mapping.get(r, r)


def format_article_date(value: str, lang: str = "no") -> str:
    dt = parse_iso_or_rss(value)
    if not dt:
        return value or ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    months_no = {
        1: "januar",
        2: "februar",
        3: "mars",
        4: "april",
        5: "mai",
        6: "juni",
        7: "juli",
        8: "august",
        9: "september",
        10: "oktober",
        11: "november",
        12: "desember",
    }
    months_en = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December",
    }

    if lang == "en":
        return f"{months_en[dt.month]} {dt.day}, {dt.year}"
    return f"{dt.day}. {months_no[dt.month]} {dt.year}"


def improve_generated_title(lang: str, article_type: str, day: str, summary: str = "") -> str:
    summary_l = (summary or "").lower()

    if lang == "en":
        if article_type == "analysis":
            return f"Gold price forecast: key levels to watch on {day}"
        if article_type == "market_driver":
            return "Gold market driver: what is moving the market now"
        if "usd" in summary_l or "yields" in summary_l:
            return "Gold market update: USD, yields and sentiment in focus"
        return f"Gold market update: macro drivers shaping gold on {day}"

    if article_type == "analysis":
        return f"Gullpris analyse: viktige nivåer å følge {day}"
    if article_type == "market_driver":
        return "Gull market driver: hva som flytter markedet nå"
    if "inflasjon" in summary_l or "renter" in summary_l:
        return "Gullpris i dag: renter, inflasjon og markedsstemning i fokus"
    return f"Gullmarkedet i dag: oppdatering og nøkkelnivåer {day}"


def normalize_article_for_display(article: Dict[str, Any]) -> Dict[str, Any]:
    a = dict(article)
    lang = str(a.get("lang") or "no")
    article_type = str(a.get("type") or "news")
    day = str(a.get("date") or "")
    title = str(a.get("title") or "").strip()
    summary = str(a.get("summary") or "").strip()
    content = str(a.get("content") or "")

    bad_prefixes = ["Gold market update ", "Gold price forecast ", "Gull marked oppdatering ", "Gullpris analyse "]

    if title.endswith(day) or any(title.startswith(p) for p in bad_prefixes):
        a["title"] = improve_generated_title(lang, article_type, day, summary)

    if lang == "no":
        content = content.replace("Full analysis and signal update:", "Full analyse og signaloppdatering:")
    else:
        content = content.replace("Full analyse og signaloppdatering:", "Full analysis and signal update:")

    a["content"] = content
    return a


def auth_login_box(next_url: str = "/archive", sent: bool = False, email: str = "", is_en: bool = False) -> str:
    sent_html = ""
    if sent:
        sent_text = (
            f"Magic link sent to {_escape_html(email)} if the address exists in the system."
            if is_en
            else f"Magic link sendt til {_escape_html(email)} dersom adressen finnes i systemet."
        )
        sent_html = f'<p class="small" style="margin-top:10px">{sent_text}</p>'

    title = "Sign in with magic link" if is_en else "Logg inn med magic link"
    desc = (
        "If you purchased Premium, get a login link sent to your email. No password required."
        if is_en
        else "Har du kjøpt Premium? Få innloggingslenke på e-post, uten passord."
    )
    placeholder = "Your email" if is_en else "Din e-post"
    button = "Send magic link" if is_en else "Send magic link"

    return f"""
    <div class="authbox">
      <h3 style="margin:0 0 8px">{title}</h3>
      <p class="muted" style="margin:0 0 10px">{desc}</p>
      <form method="post" action="/auth/request-link">
        <input name="email" type="email" placeholder="{placeholder}" autocomplete="email" />
        <input type="hidden" name="next_url" value="{_escape_html(next_url)}" />
        <div class="btnrow">
          <button type="submit">{button}</button>
        </div>
      </form>
      {sent_html}
    </div>
    """


def key_fallback_box(is_en: bool = False) -> str:
    title = "Have a premium key?" if is_en else "Har du premium-nøkkel?"
    desc = "You can paste your premium key here." if is_en else "Du kan lime inn premium-nøkkelen din her."
    placeholder = "Premium key" if is_en else "Premium-nøkkel"
    button = "Save key" if is_en else "Lagre nøkkel"
    saved = "Saved" if is_en else "Lagret"
    error = "Error" if is_en else "Feil"

    return f"""
<div class="keypastebox">
    <h3 style="margin:0 0 8px">{title}</h3>
    <p class="muted" style="margin:0 0 10px">{desc}</p>

    <div class="btnrow">
        <input id="key" placeholder="{placeholder}" autocomplete="off" />
        <button id="btnSave">{button}</button>
    </div>
</div>

<script>
(function(){{
  const LS_KEY = "gullbrief_premium_key";
  const key = document.getElementById("key");
  const btn = document.getElementById("btnSave");

  if(key){{
    try{{
      key.value = localStorage.getItem(LS_KEY) || "";
    }}catch(e){{}}
  }}

  if(btn && key){{
    btn.onclick = function(){{
      try{{
        localStorage.setItem(LS_KEY, key.value.trim());
        btn.innerText = "{saved}";
        setTimeout(()=>{{ btn.innerText = "{button}"; }}, 1000);
      }}catch(e){{
        btn.innerText = "{error}";
      }}
    }}
  }}
}})();
</script>
"""

INDEX_BODY_TEMPLATE = """
<div class="wrap">
  __SITE_HEADER__

  <section class="hero">
    <h1>Gullpris i dag 📈 analyse, prognose og signal for gull (XAUUSD)</h1>
    <p>__DESC__</p>
  </section>

  __NAV_TABS__

  <section class="grid">
    <div class="card">
      <div class="title"><h2>__CARD_TITLE__</h2><div class="muted" id="updatedAt">__UPDATED_LOADING__</div></div>
      <div class="big" id="price">$–</div>
      <div class="sub" id="change">__CHANGE_LOADING__</div>
      <div class="pill neutral" id="signalPill"><span class="dot"></span><span id="signalText">Signal: –</span></div>
      <p class="muted" style="margin-top:12px" id="reason">–</p>
      __CHART_HTML__

      <h2 style="margin-top:14px">Analyse</h2>
      <p class="muted" id="macro"></p>

      __GUIDE_LINK__
      __AFFILIATE_BOX__
      __PREMIUM_BOX__
      __AUTH_BOX__
      __KEY_BOX__

      <div class="btnrow">
        <button id="btnReload">Oppdater</button>
        <button onclick="location.href='/premium'">Premium</button>
        <button onclick="location.href='/archive'">Arkiv</button>
      </div>

      <div class="muted" id="status" style="margin-top:8px">Status: …</div>
    </div>

    <div class="card">
      <div class="title"><h2>__HEADLINES_TITLE__</h2><div class="muted">__HEADLINES_SUB__</div></div>
      <ul id="headlines"></ul>
      <div id="premiumNewsHint" class="premiumhint" style="display:none"></div>
    </div>
  </section>

  __LATEST_NEWS__
  __FOOTER__
</div>

<script id="initialTodayData" type="application/json">__INITIAL_JSON__</script>
<script>
  const MODE = "analysis";
  const $ = (id) => document.getElementById(id);
  const fmtPct = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ((Number(x)>0?"+":"") + Number(x).toFixed(2) + "%");
  const fmtPrice = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ("$" + Number(x).toLocaleString(undefined,{maximumFractionDigits:2}));
  const pillClass = (s) => (s||"").toLowerCase().includes("bull") ? "bullish" : ((s||"").toLowerCase().includes("bear") ? "bearish" : "neutral");
  const UPDATED_LABEL = "__UPDATED_LABEL__";
  const CHANGE_LABEL = "__CHANGE_LABEL__";
  const PREMIUM_NEWS_HINT = "__PREMIUM_NEWS_HINT__";
  const formatUpdatedAt = (value) => {
    if(!value) return "–";
    try{
      const d = new Date(value);
      if(Number.isNaN(d.getTime())) return value;
      return d.toLocaleDateString("__DATE_LOCALE__", { day:"numeric", month:"long", year:"numeric" });
    }catch(e){
      return value;
    }
  };


  function renderChart(points){
    const svg = $("priceChart");
    if(!svg || !Array.isArray(points) || points.length < 2) return;
    const vals = points.map(p=>Number(p.close)).filter(v=>Number.isFinite(v));
    if(vals.length < 2) return;
    const min = Math.min(...vals), max = Math.max(...vals);
    const span = Math.max(max-min, 1e-9);
    const width = 400, height = 68;
    const coords = vals.map((v,i)=>{
      const x = (i/(vals.length-1))*width;
      const y = height - (((v-min)/span)*(height-8)+4);
      return [x,y];
    });
    const line = coords.map((c,i)=>(i?"L":"M") + c[0].toFixed(2)+" "+c[1].toFixed(2)).join(" ");
    const area = line + ` L ${width} ${height} L 0 ${height} Z`;
    svg.innerHTML = `<path class="chart-fill" d="${area}"></path><path class="chart-line" d="${line}"></path>`;
  }

  async function loadChart(){
    try{
      const res = await fetch("/api/public/chart?days=7", {cache:"no-store"});
      const data = await res.json();
      if(res.ok) renderChart(data.points || []);
    }catch(e){}
  }

  function renderChart(points){
    const svg = $("priceChart");
    if(!svg || !Array.isArray(points) || points.length < 2) return;
    const vals = points.map(p=>Number(p.close)).filter(v=>Number.isFinite(v));
    if(vals.length < 2) return;
    const min = Math.min(...vals), max = Math.max(...vals);
    const span = Math.max(max-min, 1e-9);
    const width = 400, height = 68;
    const coords = vals.map((v,i)=>{
      const x = (i/(vals.length-1))*width;
      const y = height - (((v-min)/span)*(height-8)+4);
      return [x,y];
    });
    const line = coords.map((c,i)=>(i?"L":"M") + c[0].toFixed(2)+" "+c[1].toFixed(2)).join(" ");
    const area = line + ` L ${width} ${height} L 0 ${height} Z`;
    svg.innerHTML = `<path class="chart-fill" d="${area}"></path><path class="chart-line" d="${line}"></path>`;
  }

  async function loadChart(){
    try{
      const res = await fetch("/api/public/chart?days=7", {cache:"no-store"});
      const data = await res.json();
      if(res.ok) renderChart(data.points || []);
    }catch(e){}
  }
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
      hint.innerHTML = PREMIUM_NEWS_HINT.replace("__FREE_LIMIT__", String(freeLimit));
    }else{
      hint.style.display = "none";
      hint.textContent = "";
    }
  }

  function renderToday(data){
    $("updatedAt").textContent = UPDATED_LABEL + formatUpdatedAt(data.updated_at);
    $("price").textContent = fmtPrice(data?.gold?.price_usd);
    $("change").textContent = CHANGE_LABEL + fmtPct(data?.gold?.change_pct);
    const state = data?.signal?.state || "neutral";
    $("signalText").textContent = "Signal: " + state;
    $("signalPill").className = "pill " + pillClass(state);
    $("reason").textContent = data?.signal?.reason_short || "";
    $("macro").textContent = data?.macro?.summary_short || "";
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

  if (typeof loadChart === "function") loadChart();
  if(!renderInitial()){
    loadToday();
  } else {
    setTimeout(loadToday, 500);
  }
</script>
"""


PREMIUM_BODY_TEMPLATE = """
<div class="wrap">
  __SITE_HEADER__

  <section class="hero">
    <h1>Premium</h1>
    <p>Mer data, mindre støy. Daglig premium-rapport, signalhistorikk, flere nyheter og arkiv.</p>
  </section>

  __NAV_TABS__

  <section class="grid">
    <div class="card">
      <div class="title"><h2>Dette får du</h2><div class="muted">Premium</div></div>
      <ul>
        <li><b>Signal history (last 30)</b> + hit rate</li>
        <li><b>Archive</b> with 7d/30d after signal</li>
        <li><b>Extended daily premium report</b> clearly longer than the free analysis</li>
        <li><b>More headlines</b> than the free version</li>
        <li><b>Email alerts</b> for signal changes and daily delivery</li>
      </ul>

      <h2 style="margin-top:14px">Kjøp Premium</h2>
      <p class="muted">Skriv e-post og gå til Stripe checkout.</p>
      <div class="btnrow">
        <input id="payEmail" placeholder="E-post for kjøp" autocomplete="email" />
        <button class="cta" id="btnPay" style="border:0">Kjøp premium</button>
      </div>
      <div class="small" id="status" style="margin-top:10px"></div>

      __AUTH_BOX__
      __KEY_BOX__
    </div>

    <div class="card">
      <div class="title"><h2>Hva rapporten inneholder</h2><div class="muted">Daglig</div></div>
      <ul>
        <li>Executive summary og marked akkurat nå</li>
        <li>Teknisk bilde med støtte, motstand, SMA og momentum</li>
        <li>Makrodrivere og XAUUSD-vinkel</li>
        <li>Base / Bull / Bear-scenario</li>
        <li>Hva som styrker og hva som bryter signalet</li>
        <li>Watchlist neste 24–72t</li>
        <li>Konklusjon med samlet vurdering</li>
      </ul>
    </div>
  </section>
  __LATEST_NEWS__
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
  __SITE_HEADER__

  <section class="hero">
    <h1>__H1__</h1>
    <p>__INTRO__</p>
  </section>

  __NAV_TABS__

  <section class="grid">
    <div class="card">
      <div class="title"><h2>__CARD_TITLE__</h2><div class="muted" id="updatedAt">__UPDATED_LOADING__</div></div>
      <div class="big" id="price">$–</div>
      <div class="sub" id="change">__CHANGE_LOADING__</div>
      <div class="pill neutral" id="signalPill"><span class="dot"></span><span id="signalText">Signal: –</span></div>
      <p class="muted" style="margin-top:12px" id="reason">–</p>
      __CHART_HTML__
      <p class="muted" id="macro"></p>

      __GUIDE_LINK__
      __AFFILIATE_BOX__
      __PREMIUM_BOX__
      __AUTH_BOX__
      __KEY_BOX__

      <div class="btnrow">
        <button id="btnReload">Oppdater</button>
        <button onclick="location.href='/premium'">Premium</button>
        <button onclick="location.href='/archive'">Arkiv</button>
      </div>
      <div class="muted" id="status" style="margin-top:8px">Status: …</div>
    </div>

    <div class="card">
      <div class="title"><h2>__HEADLINES_TITLE__</h2><div class="muted">__HEADLINES_SUB__</div></div>
      <ul id="headlines"></ul>
      <div id="premiumNewsHint" class="premiumhint" style="display:none"></div>
    </div>
  </section>

  __SEO_TEXT__
  __LATEST_NEWS__
  __FOOTER__
</div>

<script id="initialTodayData" type="application/json">__INITIAL_JSON__</script>
<script>
  const MODE = "__MODE__";
  const $ = (id) => document.getElementById(id);
  const fmtPct = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ((Number(x)>0?"+":"") + Number(x).toFixed(2) + "%");
  const fmtPrice = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ("$" + Number(x).toLocaleString(undefined,{maximumFractionDigits:2}));
  const pillClass = (s) => (s||"").toLowerCase().includes("bull") ? "bullish" : ((s||"").toLowerCase().includes("bear") ? "bearish" : "neutral");
  const UPDATED_LABEL = "__UPDATED_LABEL__";
  const CHANGE_LABEL = "__CHANGE_LABEL__";
  const PREMIUM_NEWS_HINT = "__PREMIUM_NEWS_HINT__";
  const formatUpdatedAt = (value) => {
    if(!value) return "–";
    try{
      const d = new Date(value);
      if(Number.isNaN(d.getTime())) return value;
      return d.toLocaleDateString("__DATE_LOCALE__", { day:"numeric", month:"long", year:"numeric" });
    }catch(e){
      return value;
    }
  };

  function renderChart(points){
    const svg = $("priceChart");
    if(!svg || !Array.isArray(points) || points.length < 2) return;
    const vals = points.map(p=>Number(p.close)).filter(v=>Number.isFinite(v));
    if(vals.length < 2) return;
    const min = Math.min(...vals), max = Math.max(...vals);
    const span = Math.max(max-min, 1e-9);
    const width = 400, height = 68;
    const coords = vals.map((v,i)=>{
      const x = (i/(vals.length-1))*width;
      const y = height - (((v-min)/span)*(height-8)+4);
      return [x,y];
    });
    const line = coords.map((c,i)=>(i?"L":"M") + c[0].toFixed(2)+" "+c[1].toFixed(2)).join(" ");
    const area = line + ` L ${width} ${height} L 0 ${height} Z`;
    svg.innerHTML = `<path class="chart-fill" d="${area}"></path><path class="chart-line" d="${line}"></path>`;
  }

  async function loadChart(){
    try{
      const res = await fetch("/api/public/chart?days=7", {cache:"no-store"});
      const data = await res.json();
      if(res.ok) renderChart(data.points || []);
    }catch(e){}
  }

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
      hint.innerHTML = PREMIUM_NEWS_HINT.replace("__FREE_LIMIT__", String(freeLimit));
    }else{
      hint.style.display = "none";
      hint.textContent = "";
    }
  }

  function renderToday(data){
    $("updatedAt").textContent = UPDATED_LABEL + formatUpdatedAt(data.updated_at);
    $("price").textContent = fmtPrice(data?.gold?.price_usd);
    $("change").textContent = CHANGE_LABEL + fmtPct(data?.gold?.change_pct);
    const state = data?.signal?.state || "neutral";
    $("signalText").textContent = "Signal: " + state;
    $("signalPill").className = "pill " + pillClass(state);
    $("reason").textContent = data?.signal?.reason_short || "";
    $("macro").textContent = data?.macro?.summary_short || "";
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

  if (typeof loadChart === "function") loadChart();
  if(!renderInitial()){
    loadToday();
  }
</script>
"""


TRADE_GUIDE_TEMPLATE = """
<div class="wrap">
  __SITE_HEADER__

  <section class="hero">
    <h1>__H1__</h1>
    <p>__INTRO__</p>
  </section>

  __NAV_TABS__

  <section class="grid">
    <div class="card">
      <div class="title"><h2>__CARD_TITLE__</h2><div class="muted" id="updatedAt">__UPDATED_LOADING__</div></div>
      <div class="big" id="price">$–</div>
      <div class="sub" id="change">__CHANGE_LOADING__</div>
      <div class="pill neutral" id="signalPill"><span class="dot"></span><span id="signalText">Signal: –</span></div>
      <p class="muted" style="margin-top:12px" id="reason">–</p>
      __CHART_HTML__
      <p class="muted" id="macro"></p>

      __AFFILIATE_BOX__

      <div class="btnrow">
        <button id="btnReload">Oppdater</button>
        <button onclick="location.href='__ANALYSIS_LINK__'">__ANALYSIS_BTN__</button>
        <button onclick="location.href='/premium'">Premium</button>
      </div>
      <div class="muted" id="status" style="margin-top:8px">Status: …</div>
    </div>

    <div class="card content-block">
      __CONTENT_HTML__
    </div>
  </section>

  __LATEST_NEWS__
  __FOOTER__
</div>

<script id="initialTodayData" type="application/json">__INITIAL_JSON__</script>
<script>
  const MODE = "__MODE__";
  const $ = (id) => document.getElementById(id);
  const fmtPct = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ((Number(x)>0?"+":"") + Number(x).toFixed(2) + "%");
  const fmtPrice = (x) => (x==null||Number.isNaN(Number(x))) ? "–" : ("$" + Number(x).toLocaleString(undefined,{maximumFractionDigits:2}));
  const pillClass = (s) => (s||"").toLowerCase().includes("bull") ? "bullish" : ((s||"").toLowerCase().includes("bear") ? "bearish" : "neutral");
  const UPDATED_LABEL = "__UPDATED_LABEL__";
  const CHANGE_LABEL = "__CHANGE_LABEL__";
  const formatUpdatedAt = (value) => {
    if(!value) return "–";
    try{
      const d = new Date(value);
      if(Number.isNaN(d.getTime())) return value;
      return d.toLocaleDateString("__DATE_LOCALE__", { day:"numeric", month:"long", year:"numeric" });
    }catch(e){
      return value;
    }
  };

  function renderChart(points){
    const svg = $("priceChart");
    if(!svg || !Array.isArray(points) || points.length < 2) return;
    const vals = points.map(p=>Number(p.close)).filter(v=>Number.isFinite(v));
    if(vals.length < 2) return;
    const min = Math.min(...vals), max = Math.max(...vals);
    const span = Math.max(max-min, 1e-9);
    const width = 400, height = 68;
    const coords = vals.map((v,i)=>{
      const x = (i/(vals.length-1))*width;
      const y = height - (((v-min)/span)*(height-8)+4);
      return [x,y];
    });
    const line = coords.map((c,i)=>(i?"L":"M") + c[0].toFixed(2)+" "+c[1].toFixed(2)).join(" ");
    const area = line + ` L ${width} ${height} L 0 ${height} Z`;
    svg.innerHTML = `<path class="chart-fill" d="${area}"></path><path class="chart-line" d="${line}"></path>`;
  }

  async function loadChart(){
    try{
      const res = await fetch("/api/public/chart?days=7", {cache:"no-store"});
      const data = await res.json();
      if(res.ok) renderChart(data.points || []);
    }catch(e){}
  }

  function renderToday(data){
    $("updatedAt").textContent = UPDATED_LABEL + formatUpdatedAt(data.updated_at);
    $("price").textContent = fmtPrice(data?.gold?.price_usd);
    $("change").textContent = CHANGE_LABEL + fmtPct(data?.gold?.change_pct);
    const state = data?.signal?.state || "neutral";
    $("signalText").textContent = "Signal: " + state;
    $("signalPill").className = "pill " + pillClass(state);
    $("reason").textContent = data?.signal?.reason_short || "";
    $("macro").textContent = data?.macro?.summary_short || "";
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

  if (typeof loadChart === "function") loadChart();
  if(!renderInitial()){
    loadToday();
  }
</script>
"""

ARCHIVE_BODY_INNER = """
<div class="wrap">
  __SITE_HEADER__

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
      <div class="muted">Logg inn med magic link, eller bruk premium-nøkkel som fallback.</div>

      __AUTH_BOX__

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
        API: <code>/api/history</code> med header <code>x-api-key</code> eller aktiv session-cookie.
      </div>

      __KEY_BOX__
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
    try{ $("key").value = localStorage.getItem(LS_KEY) || ""; }catch(e){}
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
        setTeaser("Ingen snapshots ennå.");
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
    setStatus("Laster…");
    $("tbl").style.display="none";
    $("body").innerHTML="";
    try{
      const headers = {};
      if(k){ headers["x-api-key"] = k; }
      const res = await fetch("/api/history?limit=200", {headers, cache:"no-store", credentials:"same-origin"});
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
    if(!email.includes("@")){ setStatus("Skriv inn gyldig e-post."); return; }
    try{
      setStatus("Lagrer e-post…");
      const headers = {"Content-Type":"application/json"};
      if(k){ headers["x-api-key"] = k; }
      const res = await fetch("/api/premium/subscribe-email", {
        method:"POST",
        headers,
        credentials:"same-origin",
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
    try{
      localStorage.setItem(LS_KEY, $("key").value.trim());
      setStatus("Nøkkel lagret lokalt ✅");
    }catch(e){ setStatus("Kunne ikke lagre nøkkel."); }
  });
  $("btnClear").addEventListener("click", ()=>{
    try{
      localStorage.removeItem(LS_KEY);
      $("key").value="";
      setStatus("Nøkkel fjernet.");
      $("tbl").style.display="none";
      $("body").innerHTML="";
    }catch(e){ setStatus("Kunne ikke fjerne nøkkel."); }
  });
  $("btnLoad").addEventListener("click", loadArchive);
  $("btnEmail").addEventListener("click", subscribeEmail);
  $("btnPay").addEventListener("click", startCheckout);

  loadSavedKey();
  loadTeaser();
  loadArchive();
</script>
"""


SUCCESS_TEMPLATE = """
<div class="wrap">
  __SITE_HEADER__

  <section class="hero">
    <h1>Betaling registrert</h1>
    <p>Hvis Stripe-webhooken har rukket å kjøre, ligger premium-nøkkelen din klar under. Magic-link kan brukes videre for enkel innlogging.</p>
  </section>

  __NAV_TABS__

  <section class="grid" style="grid-template-columns:1fr">
    <div class="card">
      <div class="title"><h2>Premium-nøkkel</h2><div class="muted">Aktivering</div></div>
      <div class="big" style="font-size:24px;word-break:break-word">__KEY__</div>
      <p class="muted" style="margin-top:12px">__STATUS__</p>
      __AUTH_BOX__
      <div class="btnrow">
        <button onclick="location.href='/archive'">Åpne arkiv</button>
        <button onclick="navigator.clipboard.writeText('__KEY_RAW__').catch(()=>{})">Kopier nøkkel</button>
      </div>
      __KEY_BOX__
    </div>
  </section>

  __FOOTER__
</div>
"""


LEGAL_PAGE_TEMPLATE = """
<div class="wrap">
  __SITE_HEADER__

  <section class="hero">
    <h1>__TITLE__</h1>
    <p>__INTRO__</p>
  </section>

  <section class="grid" style="grid-template-columns:1fr">
    <div class="card legal-card">
      __CONTENT__
    </div>
  </section>

  __KEY_BOX__
  __FOOTER__
</div>
"""


def get_recent_news_articles(lang: str, limit: int = 3, exclude_slug: Optional[str] = None) -> List[Dict[str, Any]]:
    items = []
    for article in get_news_articles_by_lang(lang):
        if exclude_slug and str(article.get("slug") or "") == exclude_slug:
            continue
        items.append(normalize_article_for_display(article))
    return items[:limit]


def get_latest_articles(limit: int = 3, lang: str = "no") -> List[Dict[str, Any]]:
    articles = get_recent_news_articles(lang=lang, limit=limit)
    out = []
    for article in articles:
        out.append({"date": str(article.get("date") or ""), "title": str(article.get("title") or ""), "url": str(article.get("path") or "#")})
    return out


def render_recent_articles_box(lang: str, exclude_slug: Optional[str] = None) -> str:
    articles = get_recent_news_articles(lang=lang, limit=3, exclude_slug=exclude_slug)
    if not articles:
        return ""

    title = "Latest articles" if lang == "en" else "Siste artikler"
    count_label = "items" if lang == "en" else "artikler"

    items = []
    for article in articles:
        path = str(article.get("path") or "#")
        title_txt = str(article.get("title") or "")
        summary = str(article.get("summary") or "")
        published = format_article_date(str(article.get("published_at") or article.get("date") or ""), lang=lang)
        items.append(
            "<li>"
            f'<a href="{_escape_html(path)}"><b>{_escape_html(title_txt)}</b></a><br/>'
            f'<span class="muted">{_escape_html(summary)}</span><br/>'
            f'<span class="small">{_escape_html(published)}</span>'
            "</li>"
        )

    return f"""
    <section class="card" style="margin-top:16px">
      <div class="title"><h2>{title}</h2><div class="muted">{len(articles)} {count_label}</div></div>
      <ul>{''.join(items)}</ul>
    </section>
    """


def ensure_news_seeded() -> None:
    articles = get_all_news_articles()
    if articles:
        return
    try:
        if NEWS_DAILY_ENABLED:
            generate_and_store_daily_news()
    except Exception:
        pass


def trade_guide_content_html(lang: str = "no") -> str:
    if lang == "en":
        return """
        <h2>What is XAUUSD?</h2>
        <p>XAUUSD is the common market symbol for gold priced in US dollars. When people trade spot gold through many brokers and CFD platforms, they often do it through an instrument linked to XAUUSD.</p>

        <h2>How can you trade gold?</h2>
        <p>Gold can be traded in several ways, including spot exposure, CFDs, futures, mining shares, gold ETFs and physical bullion. The most accessible route for many retail users is often a platform that offers gold exposure through XAUUSD.</p>

        <h3>Typical process</h3>
        <ul>
          <li>Open an account with a trading platform</li>
          <li>Search for gold or XAUUSD</li>
          <li>Choose position size and risk level</li>
          <li>Follow price, trend and macro drivers such as USD, yields and inflation</li>
        </ul>

        <h2>Trading vs investing in gold</h2>
        <p>Trading is usually more short term and focused on price movements over hours, days or weeks. Investing is often longer term and may involve holding gold-related exposure for diversification, inflation hedging or macro protection.</p>

        <h2>Is eToro one option?</h2>
        <p>Yes. eToro is one possible platform for gaining gold exposure, and it is relevant here because many readers looking at gold forecasts also want a straightforward way to follow or trade XAUUSD. There are also other platforms in the market.</p>

        <h2>What should you watch before trading?</h2>
        <ul>
          <li>US dollar strength</li>
          <li>Treasury yields and real yields</li>
          <li>Inflation releases</li>
          <li>Central bank communication</li>
          <li>Geopolitical stress and safe-haven demand</li>
          <li>Technical levels such as support, resistance and moving averages</li>
        </ul>

        <h2>Related reading on Gullbrief</h2>
        <p>Before trading gold, it helps to read the daily <a href="/gold-price-forecast">gold price forecast</a>, the Norwegian <a href="/gullpris-analyse">gullpris analyse</a> and the <a href="/xauusd">XAUUSD page</a> to understand the live market picture.</p>
        """
    return """
    <h2>Hva er XAUUSD?</h2>
    <p>XAUUSD er det vanlige markedssymbolet for gull priset i amerikanske dollar. Når mange trader gull via meglere og tradingplattformer, skjer det ofte gjennom et instrument knyttet til XAUUSD.</p>

    <h2>Hvordan kan man trade gull?</h2>
    <p>Gull kan handles på flere måter, blant annet via spot-eksponering, CFD-er, futures, gruveaksjer, gull-ETF-er og fysisk gull. For mange private brukere er en plattform med tilgang til XAUUSD ofte den enkleste inngangen.</p>

    <h3>Typisk prosess</h3>
    <ul>
      <li>Opprett konto hos en tradingplattform</li>
      <li>Søk opp gull eller XAUUSD</li>
      <li>Velg posisjonsstørrelse og risikonivå</li>
      <li>Følg pris, trend og makrodrivere som USD, renter og inflasjon</li>
    </ul>

    <h2>Forskjellen på trading og investering</h2>
    <p>Trading er ofte mer kortsiktig og handler om prisbevegelser over timer, dager eller uker. Investering er gjerne mer langsiktig og kan handle om diversifisering, inflasjonssikring eller generell eksponering mot gull over tid.</p>

    <h2>Er eToro en mulig plattform?</h2>
    <p>Ja. eToro er en mulig plattform for å få eksponering mot gull, og er relevant her fordi mange som leser gullprognoser også ønsker en enkel vei til å følge eller trade XAUUSD. Det finnes også andre plattformer i markedet.</p>

    <h2>Hva bør du følge med på før du trader gull?</h2>
    <ul>
      <li>Utviklingen i amerikansk dollar</li>
      <li>Renter og realrenter</li>
      <li>Inflasjonstall</li>
      <li>Sentralbankkommunikasjon</li>
      <li>Geopolitisk uro og safe haven-etterspørsel</li>
      <li>Tekniske nivåer som støtte, motstand og glidende snitt</li>
    </ul>

    <h2>Relatert lesning på Gullbrief</h2>
    <p>Før du trader gull kan det være nyttig å lese daglig <a href="/gullpris-analyse">gullpris analyse</a>, <a href="/gullpris-prognose">gullpris prognose</a> og <a href="/xauusd">XAUUSD-siden</a> for å forstå markedsbildet akkurat nå.</p>
    """


def seo_landing(
    request: Request,
    path: str,
    title: str,
    desc: str,
    h1: str,
    intro: str,
    mode: str,
    nav_active: str,
    lang: str = "no",
    seo_text_html: str = "",
    sent_magic_link: bool = False,
    sent_email: str = "",
    include_affiliate: bool = False,
    include_trade_link: bool = False,
) -> HTMLResponse:
    ensure_news_seeded()

    initial_payload = get_public_today_payload(mode)

    is_en = lang == "en"
    articles_lang = "en" if mode == "forecast_en" else "no"

    if is_en and isinstance(initial_payload.get("signal"), dict):
        initial_payload["signal"]["reason_short"] = translate_signal_reason_to_english(
            str(initial_payload["signal"].get("reason_short") or "")
        )

    premium_box_html = premium_feature_box_en() if is_en else premium_feature_box("52•••", "48•••")

    auth_box_html = auth_login_box(
        next_url=path,
        sent=sent_magic_link,
        email=sent_email,
        is_en=is_en
    )

    key_box_html = key_fallback_box(is_en=is_en)
    latest_news_html = render_recent_articles_box(articles_lang)
    affiliate_html = affiliate_box(lang=lang) if include_affiliate else ""
    guide_link_html = internal_trade_guide_link(lang=lang) if include_trade_link else ""

    body = _replace_many(
        SEO_LANDING_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__SITE_HEADER__": site_header(nav_active),
            "__CHART_HTML__": chart_html,
            "__H1__": _escape_html(h1),
            "__INTRO__": _escape_html(intro),
            "__FOOTER__": footer_links(is_en=is_en),
            "__MODE__": _escape_html(mode),
            "__NAV_TABS__": nav_tabs(nav_active),
            "__INITIAL_JSON__": json_for_html(initial_payload),
            "__PREMIUM_BOX__": premium_box_html,
            "__AUTH_BOX__": auth_box_html,
            "__KEY_BOX__": key_box_html,
            "__SEO_TEXT__": seo_text_html,
            "__LATEST_NEWS__": latest_news_html,
            "__AFFILIATE_BOX__": affiliate_html,
            "__GUIDE_LINK__": guide_link_html,
            "__CARD_TITLE__": "Gold price today" if is_en else "Gullpris i dag",
            "__UPDATED_LOADING__": "Updating…" if is_en else "Oppdaterer…",
            "__CHANGE_LOADING__": "Change:" if is_en else "Endring:",
            "__UPDATED_LABEL__": "Updated:" if is_en else "Oppdatert:",
            "__CHANGE_LABEL__": "Change:" if is_en else "Endring:",
            "__DATE_LOCALE__": "en-US" if is_en else "nb-NO",
            "__HEADLINES_TITLE__": "Relevant headlines" if is_en else "Relevante nyheter",
            "__HEADLINES_SUB__": "Direct sources" if is_en else "Direkte kilder",
            "__PREMIUM_NEWS_HINT__": (
                "Showing __FREE_LIMIT__ recent articles. Premium gives access to more market headlines, the longer report and the archive. "
                "<a href=&quot;/premium&quot;>Open Premium</a>"
                if is_en
                else
                "Viser __FREE_LIMIT__ nylige artikler. Premium gir tilgang til flere markedssaker, lengre rapport og arkiv. <a href=&quot;/premium&quot;>Åpne Premium</a>"
            ),
        },
    )

    return HTMLResponse(
        html_shell(
            request,
            title=title,
            description=desc,
            path=path,
            body_html=body,
            lang=lang,
        )
    )


def trade_guide_page(
    request: Request,
    *,
    path: str,
    title: str,
    desc: str,
    h1: str,
    intro: str,
    lang: str,
    nav_active: str,
) -> HTMLResponse:
    ensure_news_seeded()
    mode = "forecast_en" if lang == "en" else "analysis"
    initial_payload = get_public_today_payload(mode)

    if lang == "en" and isinstance(initial_payload.get("signal"), dict):
        initial_payload["signal"]["reason_short"] = translate_signal_reason_to_english(
            str(initial_payload["signal"].get("reason_short") or "")
        )

    body = _replace_many(
        TRADE_GUIDE_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__SITE_HEADER__": site_header(nav_active),
            "__CHART_HTML__": chart_html,
            "__H1__": _escape_html(h1),
            "__INTRO__": _escape_html(intro),
            "__NAV_TABS__": nav_tabs(nav_active),
            "__CARD_TITLE__": "Gold price snapshot" if lang == "en" else "Markedssnapshot",
            "__UPDATED_LOADING__": "Updating…" if lang == "en" else "Oppdaterer…",
            "__CHANGE_LOADING__": "Change:" if lang == "en" else "Endring:",
            "__UPDATED_LABEL__": "Updated:" if lang == "en" else "Oppdatert:",
            "__CHANGE_LABEL__": "Change:" if lang == "en" else "Endring:",
            "__DATE_LOCALE__": "en-US" if lang == "en" else "nb-NO",
            "__MODE__": mode,
            "__INITIAL_JSON__": json_for_html(initial_payload),
            "__AFFILIATE_BOX__": affiliate_box(lang=lang),
            "__CONTENT_HTML__": trade_guide_content_html(lang=lang),
            "__ANALYSIS_LINK__": "/gold-price-forecast" if lang == "en" else "/gullpris-analyse",
            "__ANALYSIS_BTN__": "Read forecast" if lang == "en" else "Les analyse",
            "__LATEST_NEWS__": render_recent_articles_box("en" if lang == "en" else "no"),
            "__FOOTER__": footer_links(is_en=(lang == "en")),
        },
    )

    return HTMLResponse(
        html_shell(
            request,
            title=title,
            description=desc,
            path=path,
            body_html=body,
            lang=lang,
        )
    )


def legal_page(request: Request, path: str, title: str, intro: str, content_html: str) -> HTMLResponse:
    body = _replace_many(
        LEGAL_PAGE_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__SITE_HEADER__": site_header("premium"),
            "__TITLE__": _escape_html(title),
            "__INTRO__": _escape_html(intro),
            "__CONTENT__": content_html,
            "__KEY_BOX__": key_fallback_box(),
            "__FOOTER__": footer_links(),
        },
    )
    return HTMLResponse(html_shell(request, title=title, description=intro, path=path, body_html=body))

# =============================================================================
# News engine
# =============================================================================

def dedupe_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for article in sorted(articles, key=lambda x: str(x.get("published_at") or ""), reverse=True):
        key = (
            str(article.get("id") or ""),
            str(article.get("lang") or ""),
            str(article.get("slug") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(article)
    out.sort(key=lambda x: str(x.get("published_at") or ""), reverse=True)
    return out


def get_news_articles() -> List[Dict[str, Any]]:
    store = read_news_store()
    articles = store.get("articles") or []
    out: List[Dict[str, Any]] = []
    for article in articles:
        if isinstance(article, dict):
            out.append(article)
    out = dedupe_articles(out)
    out.sort(key=lambda x: str(x.get("published_at") or ""), reverse=True)
    return out


def get_all_news_articles() -> List[Dict[str, Any]]:
    current = get_news_articles()
    archived = load_news_archive()
    merged = dedupe_articles(current + archived)
    merged.sort(key=lambda x: str(x.get("published_at") or ""), reverse=True)
    return merged


def save_news_articles(articles: List[Dict[str, Any]]) -> None:
    write_news_store({"articles": dedupe_articles(articles)})


def append_news_archive(articles: List[Dict[str, Any]]) -> None:
    path = pathlib.Path(NEWS_ARCHIVE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)

    existing_ids = set()

    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    existing_ids.add(str(obj.get("id") or ""))
                except Exception:
                    pass

    with path.open("a", encoding="utf-8") as f:
        for article in articles:
            article_id = str(article.get("id") or "")
            if article_id and article_id not in existing_ids:
                f.write(json.dumps(article, ensure_ascii=False) + "\n")


def get_news_article_by_slug(lang: str, slug: str) -> Optional[Dict[str, Any]]:
    articles = get_news_articles_by_lang(lang)
    for article in articles:
        if str(article.get("slug") or "") == slug:
            return normalize_article_for_display(article)
    return None


def get_news_articles_by_lang(lang: str) -> List[Dict[str, Any]]:
    articles = [a for a in get_all_news_articles() if str(a.get("lang") or "") == lang]
    articles.sort(key=lambda x: str(x.get("published_at") or ""), reverse=True)
    return articles


def filter_articles_by_year(articles: List[Dict[str, Any]], year: str) -> List[Dict[str, Any]]:
    return [a for a in articles if str(a.get("date") or "").startswith(f"{year}-")]


def filter_articles_by_month(articles: List[Dict[str, Any]], year: str, month: str) -> List[Dict[str, Any]]:
    return [a for a in articles if str(a.get("date") or "").startswith(f"{year}-{month}-")]


def filter_articles_by_day(articles: List[Dict[str, Any]], year: str, month: str, day: str) -> List[Dict[str, Any]]:
    return [a for a in articles if str(a.get("date") or "") == f"{year}-{month}-{day}"]


def unique_news_years(lang: str) -> List[str]:
    years = set()
    for a in get_news_articles_by_lang(lang):
        d = str(a.get("date") or "")
        if len(d) >= 4:
            years.add(d[:4])
    return sorted(years, reverse=True)


def unique_news_months(lang: str, year: str) -> List[str]:
    months = set()
    for a in get_news_articles_by_lang(lang):
        d = str(a.get("date") or "")
        if d.startswith(f"{year}-") and len(d) >= 7:
            months.add(d[:7])
    return sorted(months, reverse=True)


def unique_news_days(lang: str, year: str, month: str) -> List[str]:
    days = set()
    prefix = f"{year}-{month}-"
    for a in get_news_articles_by_lang(lang):
        d = str(a.get("date") or "")
        if d.startswith(prefix) and len(d) == 10:
            days.add(d)
    return sorted(days, reverse=True)


def _headline_titles(headlines: List[Dict[str, str]], limit: int = 8) -> List[str]:
    out = []
    for h in headlines[:limit]:
        title = str(h.get("title") or "").strip()
        if title:
            out.append(title)
    return out


def _fallback_news_summary(lang: str, article_type: str) -> str:
    if lang == "en":
        if article_type == "analysis":
            return "Daily gold price forecast and XAUUSD analysis for the next 24 to 72 hours."
        if article_type == "market_driver":
            return "Extra gold market driver article triggered by larger market moves or unusually important macro headlines."
        return "Daily gold market update covering macro drivers, USD, yields, inflation and sentiment."
    if article_type == "analysis":
        return "Daglig gullpris-analyse og scenario for de neste 24 til 72 timene."
    if article_type == "market_driver":
        return "Ekstra market driver-sak som bare publiseres når markedet beveger seg mer enn normalt eller nyhetsbildet er uvanlig sterkt."
    return "Daglig markedssak om gullpris, renter, inflasjon og stemning i markedet."


def _fallback_article_body(
    *,
    lang: str,
    article_type: str,
    title: str,
    snapshot: Dict[str, Any],
    headlines: List[Dict[str, str]],
) -> str:
    price = safe_float(snapshot.get("price_usd"))
    change_pct = safe_float(snapshot.get("change_pct"))
    signal = str(snapshot.get("signal") or "neutral").upper()
    analysis = str(snapshot.get("analysis") or snapshot.get("macro_summary") or "").strip()
    forecast = str(snapshot.get("forecast") or "").strip()
    forecast_en = str(snapshot.get("forecast_en") or "").strip()
    xauusd = str(snapshot.get("xauusd") or "").strip()
    titles = _headline_titles(headlines, 6)
    headline_block = "\n".join([f"- {t}" for t in titles]) if titles else "- Ingen støttende overskrifter tilgjengelig"

    price_txt_en = f"${price:,.2f}" if price is not None else "N/A"
    price_txt_no = f"${price:,.2f}" if price is not None else "ukjent"
    chg_txt = f"{change_pct:+.2f}%" if change_pct is not None else "ukjent"

    if lang == "en":
        headline_block = "\n".join([f"- {t}" for t in titles]) if titles else "- No supporting headlines available"
        if article_type == "analysis":
            return (
                f"{title}\n\n"
                f"Gold is trading around {price_txt_en} with a daily move of {chg_txt} where data is available, while the internal signal stands at {signal}.\n\n"
                f"Near-term price action remains tied to the interaction between the US dollar, Treasury yields, inflation expectations and broader risk sentiment. When real yields rise and the dollar strengthens, gold often faces resistance. When markets rotate into defensive positioning, gold can regain support rather quickly.\n\n"
                f"Recent context:\n{headline_block}\n\n"
                f"{forecast_en or 'The most likely short-term scenario is continued consolidation unless a stronger macro catalyst shifts sentiment.'}\n\n"
                f"{xauusd or 'XAUUSD remains highly sensitive to changes in yields, DXY and safe-haven demand.'}\n\n"
                f"{analysis or 'The current setup suggests that traders should watch momentum, macro releases and cross-asset sentiment closely.'}\n\n"
                f"Full analysis and signal update:\nhttps://gullbrief.no/premium"
            )

        return (
            f"{title}\n\n"
            f"Gold markets are being shaped by inflation data, rate expectations, the US dollar, oil and geopolitical headlines. Today gold is trading around {price_txt_en} with a daily move of {chg_txt} where data is available, while the internal signal stands at {signal}.\n\n"
            f"Headlines influencing sentiment right now:\n{headline_block}\n\n"
            f"These drivers matter because gold tends to react quickly when markets reassess real rates and the path for policy. Oil can matter when it feeds inflation expectations, while geopolitical risk can amplify safe-haven demand.\n\n"
            f"The near-term tone remains reactive rather than settled. Gold can stay firm in periods of uncertainty, but sustained upside often becomes more credible when the dollar softens or yields stop climbing.\n\n"
            f"Full analysis and signal update:\nhttps://gullbrief.no/premium"
        )

    if article_type == "analysis":
        return (
            f"{title}\n\n"
            f"Gull handles rundt {price_txt_no} med en dagsendring på {chg_txt} der data er tilgjengelig, mens det interne signalet nå står i {signal}.\n\n"
            f"På kort sikt styres gullprisen i stor grad av samspillet mellom renter, dollar, inflasjonsforventninger og generell uro i markedet. Når realrentene stiger og dollaren styrker seg, blir det ofte tyngre for gull. Når investorer søker tryggere plasseringer, øker derimot interessen for gull som safe haven.\n\n"
            f"Aktuell markedskontekst:\n{headline_block}\n\n"
            f"{forecast or 'Basisscenarioet er videre konsolidering de neste 24 til 72 timene, med mindre et tydelig makrosignal endrer stemningen.'}\n\n"
            f"{xauusd or 'XAUUSD påvirkes særlig av DXY, renter og endringer i risk-on/risk-off.'}\n\n"
            f"{analysis or 'Oppsettet tilsier at investorer bør følge både makrodata, momentum og tverrmarkedssignaler tett.'}\n\n"
            f"Full analyse og signaloppdatering:\nhttps://gullbrief.no/premium"
        )

    return (
        f"{title}\n\n"
        f"Gullmarkedet påvirkes nå av inflasjonstall, renteutsikter, dollar, olje og geopolitisk uro. I dag ligger gull rundt {price_txt_no} med en dagsbevegelse på {chg_txt} der data er tilgjengelig, mens det interne signalet står i {signal}.\n\n"
        f"Markedspunkter som preger bildet akkurat nå:\n{headline_block}\n\n"
        f"Dette betyr noe fordi gull ofte reagerer raskt når markedet justerer forventningene til renter og realrenter. Også olje og geopolitikk kan spille inn når det påvirker inflasjon eller øker etterspørselen etter tryggere plasseringer.\n\n"
        f"Det kortsiktige bildet er mer reaktivt enn avklart. Gull kan holde seg sterkt ved økt uro, men videre oppgang får ofte bedre fotfeste dersom dollaren roer seg eller rentene faller tilbake.\n\n"
        f"Full analyse og signaloppdatering:\nhttps://gullbrief.no/premium"
    )


def generate_article_content(
    *,
    lang: str,
    article_type: str,
    title: str,
    snapshot: Dict[str, Any],
    headlines: List[Dict[str, str]],
) -> str:
    if not OPENAI_API_KEY:
        return _fallback_article_body(lang=lang, article_type=article_type, title=title, snapshot=snapshot, headlines=headlines)

    titles = _headline_titles(headlines, 8)
    titles_block = "\n".join([f"- {t}" for t in titles]) if titles else "- None"
    price = safe_float(snapshot.get("price_usd")) or 0.0
    change_pct = safe_float(snapshot.get("change_pct")) or 0.0
    signal = str(snapshot.get("signal") or "neutral")
    analysis = str(snapshot.get("analysis") or snapshot.get("macro_summary") or "").strip()
    forecast = str(snapshot.get("forecast") or "").strip()
    forecast_en = str(snapshot.get("forecast_en") or "").strip()
    xauusd = str(snapshot.get("xauusd") or "").strip()

    if lang == "en":
        language_instruction = "Write in English."
        if article_type == "analysis":
            style_instruction = (
                "Write a sharp, publication-ready gold market analysis. "
                "Explain what changed today, why it matters, and what traders should watch next."
            )
        else:
            style_instruction = (
                "Write a news-driven gold market article. "
                "Focus on today's most important development and explain why it matters for gold."
            )
        extra_context = f"Forecast: {forecast_en}\nXAUUSD context: {xauusd}\n"
        cta = "Full analysis and signal update:\nhttps://gullbrief.no/premium"
    else:
        language_instruction = "Skriv på norsk bokmål."
        if article_type == "analysis":
            style_instruction = (
                "Skriv en skarp og publiserbar gullanalyse. "
                "Forklar hva som har skjedd i dag, hvorfor det betyr noe, og hva markedet bør følge videre."
            )
        else:
            style_instruction = (
                "Skriv en nyhetsdrevet markedssak om gull. "
                "Ta utgangspunkt i dagens viktigste utvikling og forklar hvorfor den betyr noe for gullprisen."
            )
        extra_context = f"Forecast: {forecast}\nXAUUSD context: {xauusd}\n"
        cta = "Full analyse og signaloppdatering:\nhttps://gullbrief.no/premium"

    prompt = (
        f"{language_instruction}\n"
        f"{style_instruction}\n"
        f"Tittel: {title}\n"
        f"Pris: {price:.2f} USD\n"
        f"Dagsendring: {change_pct:+.2f}%\n"
        f"Signal: {signal.upper()}\n"
        f"Analysegrunnlag: {analysis or 'No extra analysis'}\n"
        f"{extra_context}\n"
        f"Overskrifter:\n{titles_block}\n\n"

        "Skriv som en markedsredaktør for et finansnettsted.\n"
        "Teksten skal være publiserbar, konkret og lett å lese.\n"
        "Ikke skriv som en AI-oppsummering, meglertekst eller generell markedsrapport.\n"
        "Bruk korte og mellomlange setninger.\n"
        "Bygg teksten rundt ett tydelig hovedpoeng.\n"
        "Bruk 2 til 4 konkrete observasjoner fra dagens pris, signal, nivåer eller overskrifter.\n"
        "Ikke gjenta samme idé med nye ord.\n"
        "Ikke bruk generiske formuleringer som:\n"
        "- markedet preges av\n"
        "- alt i alt\n"
        "- det er viktig å merke seg\n"
        "- forblir en nøkkelressurs\n"
        "- i et klima preget av\n\n"

        "Krav:\n"
        "- 500 til 800 ord\n"
        "- bruk mellomtitler\n"
        "- vær tydelig på hvorfor dagens utvikling betyr noe for gull\n"
        "- hold deg til tilgjengelig kontekst\n"
        "- ikke skriv investeringsråd\n"
        "- unngå oppstyltet språk\n"
        "- teksten skal føles skrevet av et menneske med markedsforståelse\n"
        "- avslutt med en kort, konkret konklusjon\n"
        "- avslutt alltid med nøyaktig denne CTA-en:\n"
        f"{cta}\n"
    )

    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.responses.create(model=OPENAI_MODEL, input=prompt)
        text = (resp.output_text or "").strip()
        return text or _fallback_article_body(lang=lang, article_type=article_type, title=title, snapshot=snapshot, headlines=headlines)
    except Exception:
        return _fallback_article_body(lang=lang, article_type=article_type, title=title, snapshot=snapshot, headlines=headlines)


def build_daily_news_articles(force_date: Optional[str] = None) -> List[Dict[str, Any]]:
    day = force_date or utc_now().date().isoformat()
    snapshot = get_cached_brief(force_refresh=False)
    headlines = snapshot.get("headlines") or fetch_headlines(limit=FULL_HEADLINES_LIMIT)
    published_at = iso_now()

    out: List[Dict[str, Any]] = []

    extra_en = [("market_driver", "gold-market-driver", "Gold market driver")] if should_generate_market_driver(snapshot, headlines) else []
    for article_type, slug_base, _title_base in EN_NEWS_TOPICS + extra_en:
        slug = slugify(f"{slug_base}-{day}")
        summary = _fallback_news_summary("en", article_type)
        title = improve_generated_title("en", article_type, day, summary)
        out.append(
            {
                "id": f"en-{article_type}-{day}",
                "slug": slug,
                "lang": "en",
                "type": article_type,
                "title": title,
                "summary": summary,
                "content": generate_article_content(lang="en", article_type=article_type, title=title, snapshot=snapshot, headlines=headlines),
                "date": day,
                "published_at": published_at,
                "updated_at": published_at,
                "path": f"/news/{slug}",
                "source_count": len(headlines[:8]),
            }
        )

    extra_no = [("market_driver", "gull-market-driver", "Gull market driver")] if should_generate_market_driver(snapshot, headlines) else []
    for article_type, slug_base, _title_base in NO_NEWS_TOPICS + extra_no:
        slug = slugify(f"{slug_base}-{day}")
        summary = _fallback_news_summary("no", article_type)
        title = improve_generated_title("no", article_type, day, summary)
        out.append(
            {
                "id": f"no-{article_type}-{day}",
                "slug": slug,
                "lang": "no",
                "type": article_type,
                "title": title,
                "summary": summary,
                "content": generate_article_content(lang="no", article_type=article_type, title=title, snapshot=snapshot, headlines=headlines),
                "date": day,
                "published_at": published_at,
                "updated_at": published_at,
                "path": f"/nyheter/{slug}",
                "source_count": len(headlines[:8]),
            }
        )

    return out


def generate_and_store_daily_news(force_date: Optional[str] = None) -> Dict[str, Any]:
    day = force_date or utc_now().date().isoformat()
    existing = get_all_news_articles()

    existing_same_day = [a for a in existing if str(a.get("date") or "") == day and str(a.get("lang") or "") in ("en", "no")]
    if len(existing_same_day) >= 4:
        return {"ok": True, "generated": 0, "articles": existing_same_day, "message": "ALREADY_GENERATED"}

    new_articles = build_daily_news_articles(force_date=day)
    current_store_articles = get_news_articles()
    merged = dedupe_articles(current_store_articles + new_articles)
    save_news_articles(merged)
    append_news_archive(new_articles)

    return {"ok": True, "generated": len(new_articles), "articles": new_articles, "message": "GENERATED"}


def generate_news_range(days: int = 7, end_date: Optional[str] = None) -> Dict[str, Any]:
    days = max(1, min(days, 30))
    if end_date:
        try:
            end = date.fromisoformat(end_date)
        except Exception:
            raise RuntimeError("INVALID_END_DATE")
    else:
        end = utc_now().date()

    generated_total = 0
    created_articles: List[Dict[str, Any]] = []

    for offset in range(days):
        day = (end - timedelta(days=offset)).isoformat()
        try:
            result = generate_and_store_daily_news(force_date=day)
            generated_total += int(result.get("generated") or 0)
            created_articles.extend(result.get("articles") or [])
        except Exception:
            continue

    return {
        "ok": True,
        "days": days,
        "end_date": end.isoformat(),
        "generated": generated_total,
        "articles": created_articles,
    }


def render_news_index_page(request: Request, lang: str) -> HTMLResponse:
    articles = [normalize_article_for_display(a) for a in get_news_articles_by_lang(lang)]

    title = "Gold News and Market Updates" if lang == "en" else "Gullnyheter og markedsoppdateringer"
    desc = (
        "English gold market updates, forecasts and macro-driven news articles from Gullbrief."
        if lang == "en"
        else "Norske nyheter og analyser om gullpris, renter, inflasjon og markedet fra Gullbrief."
    )
    h1 = "Gold News" if lang == "en" else "Gullnyheter"
    intro = (
        "Daily English articles about gold price forecasts, macro drivers, inflation, USD and market sentiment."
        if lang == "en"
        else "Daglige norske saker om gullpris, gullmarkedet, renter, inflasjon og markedsstemning."
    )

    items = []
    for article in articles[:80]:
        path = str(article.get("path") or "#")
        published = format_article_date(str(article.get("published_at") or article.get("date") or ""), lang=lang)
        items.append(
            "<li>"
            f'<a href="{_escape_html(path)}"><b>{_escape_html(str(article.get("title") or ""))}</b></a><br/>'
            f'<span class="muted">{_escape_html(str(article.get("summary") or ""))}</span><br/>'
            f'<span class="small">{_escape_html(published)}</span>'
            "</li>"
        )

    recent_box = render_recent_articles_box(lang=lang)
    is_en = lang == "en"

    body = f"""
    <div class="wrap">
      {site_header("news" if lang == "en" else "nyheter")}

      <section class="hero">
        <h1>{_escape_html(h1)}</h1>
        <p>{_escape_html(intro)}</p>
      </section>

      {nav_tabs("news" if lang == "en" else "nyheter")}

      <section class="grid" style="grid-template-columns:1fr">
        <div class="card">
          <div class="title"><h2>{'Latest articles' if lang == 'en' else 'Siste artikler'}</h2><div class="muted">{len(articles)} {'items' if lang == 'en' else 'artikler'}</div></div>
          <ul>{''.join(items) if items else ('<li>No articles yet.</li>' if lang == 'en' else '<li>Ingen artikler ennå.</li>')}</ul>
        </div>
      </section>

      {auth_login_box(next_url='/news' if lang == 'en' else '/nyheter', is_en=is_en)}
      {key_fallback_box(is_en=is_en)}
      {recent_box}
      {footer_links(is_en=is_en)}
    </div>
    """

    return HTMLResponse(html_shell(request, title=title, description=desc, path="/news" if lang == "en" else "/nyheter", body_html=body, lang=lang))


def render_news_archive_list(
    *,
    request: Request,
    lang: str,
    title: str,
    intro: str,
    items_html: str,
    path: str,
) -> HTMLResponse:
    is_en = lang == "en"

    body = f"""
    <div class="wrap">
      {site_header("news" if lang == "en" else "nyheter")}

      <section class="hero">
        <h1>{_escape_html(title)}</h1>
        <p>{_escape_html(intro)}</p>
      </section>

      {nav_tabs("news" if is_en else "nyheter")}

      <section class="grid" style="grid-template-columns:1fr">
        <div class="card">
          <ul>{items_html}</ul>
        </div>
      </section>

      {footer_links(is_en=is_en)}
    </div>
    """

    return HTMLResponse(
        html_shell(
            request,
            title=title,
            description=intro,
            path=path,
            body_html=body,
            lang=lang,
        )
    )


def _article_content_to_html(text: str) -> str:
    lines = [line.rstrip() for line in (text or "").splitlines()]
    chunks: List[str] = []
    current: List[str] = []

    def _linkify_html(s: str) -> str:
        pattern = re.compile(r"(https?://[^\s<]+)")

        def repl(match: re.Match[str]) -> str:
            url = match.group(1).rstrip('.,);:!?')
            trailing = match.group(1)[len(url):]
            return f'<a href="{url}" target="_blank" rel="noopener noreferrer">{url}</a>{trailing}'

        return pattern.sub(repl, s)

    def _format_inline(s: str) -> str:
        escaped = _escape_html(s)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
        return _linkify_html(escaped)

    def flush_paragraph() -> None:
        nonlocal current
        if current:
            paragraph = " ".join([x for x in current if x.strip()])
            if paragraph.strip():
                chunks.append(f"<p>{_format_inline(paragraph)}</p>")
            current = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            flush_paragraph()
            continue

        if stripped.startswith("###"):
            flush_paragraph()
            heading = stripped.lstrip('#').strip()
            if heading:
                chunks.append(f"<h2>{_format_inline(heading)}</h2>")
            continue

        if stripped.startswith("##"):
            flush_paragraph()
            heading = stripped.lstrip('#').strip()
            if heading:
                chunks.append(f"<h2>{_format_inline(heading)}</h2>")
            continue

        if len(stripped) < 90 and not stripped.endswith(".") and not stripped.startswith("-"):
            flush_paragraph()
            chunks.append(f"<h2>{_format_inline(stripped)}</h2>")
            continue

        if stripped.startswith("- "):
            flush_paragraph()
            chunks.append(f"<p>{_format_inline(stripped)}</p>")
            continue

        if stripped.startswith("http://") or stripped.startswith("https://"):
            flush_paragraph()
            chunks.append(f"<p>{_format_inline(stripped)}</p>")
            continue

        current.append(stripped)

    flush_paragraph()
    return '<div class="article-body">' + "".join(chunks) + "</div>"


def render_news_article_page(request: Request, article: Dict[str, Any]) -> HTMLResponse:
    article = normalize_article_for_display(article)

    lang = "en" if str(article.get("lang") or "") == "en" else "no"
    title = str(article.get("title") or APP_NAME)
    summary = str(article.get("summary") or "")
    path = str(article.get("path") or "/")
    published_at_raw = str(article.get("published_at") or iso_now())
    published_at_display = format_article_date(published_at_raw, lang=lang)
    content_html = _article_content_to_html(str(article.get("content") or ""))
    recent_box = render_recent_articles_box(lang=lang, exclude_slug=str(article.get("slug") or ""))
    is_en = lang == "en"

    body = f"""
    <div class="wrap">
      {site_header("news" if lang == "en" else "nyheter")}

      <section class="hero">
        <h1>{_escape_html(title)}</h1>
        <p>{_escape_html(summary)}</p>
      </section>

      {nav_tabs("news" if lang == "en" else "nyheter")}

      <section class="grid" style="grid-template-columns:1fr">
        <div class="card">
          <div class="title"><h2>{'Article' if lang == 'en' else 'Artikkel'}</h2><div class="muted">{_escape_html(published_at_display)}</div></div>
          {content_html}
        </div>
      </section>

      <section class="grid" style="grid-template-columns:1fr">
        <div class="card">
          <div class="title"><h2>{'Archive' if lang == 'en' else 'Arkiv'}</h2><div class="muted">{'News article' if lang == 'en' else 'Nyhetsartikkel'}</div></div>
          <ul><li><a href="/{'news' if lang == 'en' else 'nyheter'}">{'Back to news' if lang == 'en' else 'Tilbake til nyheter'}</a></li></ul>
        </div>
      </section>

      {auth_login_box(next_url=path, is_en=is_en)}
      {key_fallback_box(is_en=is_en)}
      {recent_box}
      {footer_links(is_en=is_en)}
    </div>
    """

    return HTMLResponse(
        html_shell(
            request,
            title=title,
            description=summary or title,
            path=path,
            body_html=body,
            article_date=published_at_raw[:10] if published_at_raw else None,
            lang=lang,
            extra_jsonld=jsonld_news_article(get_base_url(request), article),
        )
    )

# =============================================================================
# Pages
# =============================================================================

@app.get("/analysis")
def analysis_redirect():
    return RedirectResponse(url="/", status_code=302)


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    title = "Gullpris i dag – analyse, prognose og signal for gull (XAUUSD)"
    desc = "Gullpris i dag med daglig analyse, prognose og signal for gull (XAUUSD). Følg trend, makro og markedssignal."

    initial_payload = get_public_today_payload("analysis")

    sent = request.query_params.get("sent") == "1"
    sent_email = str(request.query_params.get("email") or "")

    latest_news_html = render_recent_articles_box("no")

    body = _replace_many(
        INDEX_BODY_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__DESC__": _escape_html(desc),
            "__FOOTER__": footer_links(),
            "__SITE_HEADER__": site_header("analysis"),
            "__CHART_HTML__": chart_html,
            "__NAV_TABS__": nav_tabs("analysis"),
            "__INITIAL_JSON__": json_for_html(initial_payload),
            "__LATEST_NEWS__": latest_news_html,
            "__PREMIUM_BOX__": premium_feature_box(),
            "__AUTH_BOX__": auth_login_box(next_url="/", sent=sent, email=sent_email),
            "__KEY_BOX__": key_fallback_box(),
            "__GUIDE_LINK__": internal_trade_guide_link("no"),
            "__AFFILIATE_BOX__": affiliate_box("no"),
            "__CARD_TITLE__": "Gullpris i dag",
            "__UPDATED_LOADING__": "Oppdaterer…",
            "__CHANGE_LOADING__": "Endring: ⏳",
            "__UPDATED_LABEL__": "Oppdatert: ",
            "__CHANGE_LABEL__": "Endring: ",
            "__DATE_LOCALE__": "nb-NO",
            "__HEADLINES_TITLE__": "Relevante nyheter",
            "__HEADLINES_SUB__": "Direkte kilder",
            "__PREMIUM_NEWS_HINT__": "Viser __FREE_LIMIT__ nylige artikler. Premium gir tilgang til flere markedssaker, lengre rapport og arkiv. <a href=&quot;/premium&quot;>Åpne Premium</a>",
        },
    )

    return HTMLResponse(html_shell(request, title=title, description=desc, path="/", body_html=body))


@app.get("/premium", response_class=HTMLResponse)
def premium_page(request: Request, session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME)) -> HTMLResponse:
    title = "Gullbrief Premium – gullpris analyse, signalhistorikk og arkiv"
    desc = "Premium: daglig rapport, signalhistorikk, flere nyheter, arkiv med 7d/30d etter signal, og e-postvarsler."

    auth = resolve_auth_context(session_token=session_token, x_api_key=None)
    sent = request.query_params.get("sent") == "1"
    sent_email = str(request.query_params.get("email") or "")

    latest_news_html = render_recent_articles_box("no")

    extra_top = ""
    if auth["authenticated"] and auth["premium_active"]:
        latest = None
        history_rows = get_history_rows_resilient(limit=1)
        if history_rows:
            latest = history_rows[-1]
        if not latest:
            latest = read_public_snapshot()

        premium_report = ""
        if latest:
            premium_report = str(latest.get("premium_report") or "").strip()

        premium_report_html = ""
        if premium_report:
            premium_report_html = f"""
        <div class="card" style="margin-bottom:16px">
          <div class="title"><h2>Dagens premium-rapport</h2><div class="muted">Live</div></div>
          <pre>{_escape_html(premium_report)}</pre>
        </div>
        """

        extra_top = f"""
    <div class="card" style="margin-bottom:16px">
      <div class="title"><h2>Innlogget</h2><div class="muted">Magic link</div></div>
      <p class="muted">Innlogget som <b>{_escape_html(str(auth.get("email") or ""))}</b>.</p>
      <div class="btnrow">
        <button onclick="location.href='/archive'">Åpne arkiv</button>
        <button onclick="location.href='/auth/logout'">Logg ut</button>
      </div>
    </div>
    {premium_report_html}
    """

    body = _replace_many(
        PREMIUM_BODY_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__FOOTER__": footer_links(),
            "__SITE_HEADER__": site_header("premium"),
            "__NAV_TABS__": nav_tabs("premium"),
            "__AUTH_BOX__": auth_login_box(next_url="/premium", sent=sent, email=sent_email),
            "__KEY_BOX__": key_fallback_box(),
            "__LATEST_NEWS__": latest_news_html,
        },
    )

    body = body.replace('<section class="grid">', extra_top + '<section class="grid">', 1)

    return HTMLResponse(html_shell(request, title=title, description=desc, path="/premium", body_html=body))


@app.get("/archive", response_class=HTMLResponse)
def archive_page(request: Request) -> HTMLResponse:
    title = "Gullbrief arkiv – signalhistorikk og avkastning etter signal"
    desc = "Se siste snapshots gratis. Premium gir full historikk, signalhistorikk og 7d/30d etter signal."

    ensure_snapshot_persisted_from_public()
    dates = get_archive_dates(last_n_days=SITEMAP_ARCHIVE_DAYS)

    links = []
    for d in dates[:60]:
        links.append(f'<li><a href="/archive/{_escape_html(d)}">Arkiv {_escape_html(d)}</a></li>')

    sent = request.query_params.get("sent") == "1"
    sent_email = str(request.query_params.get("email") or "")

    archive_map_html = (
        "<div class='wrap archive-map'><div class='card' style='margin-top:12px'>"
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
            "__AUTH_BOX__": auth_login_box(next_url="/archive", sent=sent, email=sent_email),
            "__KEY_BOX__": key_fallback_box(),
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
        <div class="brand"><a href="/">__APP_NAME__</a></div>
        <div class="nav">
          <a href="/">Analyse</a>
          <a href="/archive">Arkiv</a>
          <a href="/news">News</a>
          <a href="/nyheter">Nyheter</a>
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
      __KEY_BOX__
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
            "__KEY_BOX__": key_fallback_box(),
            "__FOOTER__": footer_links(),
            "__NAV_TABS__": nav_tabs("premium"),
        },
    )

    title = f"{APP_NAME} arkiv {day} – {sig} | gold price analysis {day}"
    desc = f"{APP_NAME} snapshot {day}: {sig}. {header}. Gullpris analyse og gold price analysis for {day}."
    return HTMLResponse(html_shell(request, title=title, description=desc, path=f"/archive/{day}", body_html=body, article_date=day))


@app.get("/success", response_class=HTMLResponse)
def success_page(request: Request, session_id: Optional[str] = None) -> HTMLResponse:
    key = "Nøkkel opprettes..."
    status_text = "Vent noen sekunder og oppdater siden hvis nøkkelen ikke vises med en gang."
    email = ""

    if session_id and stripe_ready():
        try:
            require_stripe(request)
            sess = stripe.checkout.Session.retrieve(session_id)
            customer_id = getattr(sess, "customer", None)
            subscription_id = getattr(sess, "subscription", None)
            try:
                email = normalize_email(sess.get("customer_details", {}).get("email", ""))  # type: ignore
            except Exception:
                email = ""

            if customer_id or subscription_id or email:
                conn = _db()
                row = conn.execute(
                    "SELECT api_key,status FROM api_keys WHERE stripe_customer_id=? OR stripe_subscription_id=? OR email=? ORDER BY created_at DESC LIMIT 1",
                    (customer_id or "", subscription_id or "", email or ""),
                ).fetchone()
                conn.close()
                if row:
                    key = row["api_key"]
                    status_text = f"Status: {row['status']}. Lagre nøkkelen og bruk den på arkivsiden, eller logg inn med magic link."
        except Exception:
            pass

    body = _replace_many(
        SUCCESS_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__SITE_HEADER__": site_header("premium"),
            "__KEY__": _escape_html(key),
            "__KEY_RAW__": _escape_html(key),
            "__STATUS__": _escape_html(status_text),
            "__AUTH_BOX__": auth_login_box(next_url="/archive", email=email),
            "__KEY_BOX__": key_fallback_box(),
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
        include_trade_link=True,
    )


@app.get("/gold-price-forecast", response_class=HTMLResponse)
def page_gold_price_forecast(request: Request) -> HTMLResponse:
    return seo_landing(
        request,
        path="/gold-price-forecast",
        title="Gold Price Forecast | XAUUSD outlook and daily scenario",
        desc="Daily gold price forecast for XAUUSD based on trend, signal and macro drivers such as USD, rates and geopolitics.",
        h1="Gold Price Forecast – Short Term Outlook for XAUUSD",
        intro="Daily gold price forecast for the next 24–72 hours based on trend, signal, technical levels and macro developments. See also Gullpris i dag for the main Norwegian overview.",
        mode="forecast_en",
        nav_active="gold_forecast",
        lang="en",
        include_affiliate=True,
        include_trade_link=True,
    )


@app.get("/gullpris-analyse", response_class=HTMLResponse)
def page_gullpris_analyse(request: Request) -> HTMLResponse:
    return seo_landing(
        request,
        path="/gullpris-analyse",
        title="Gullpris analyse | Gold price analysis | daglig signal og makro",
        desc="Daglig gullpris analyse og gold price analysis: signal, trend og makrodrivere. Se Gullpris i dag og oppdateringer.",
        h1="Gullpris analyse",
        intro="Nøktern daglig analyse av gull. Fokus på trend, signal og makro. Gold price analysis og XAUUSD signal.",
        mode="analysis",
        nav_active="analysis",
        include_affiliate=True,
        include_trade_link=True,
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
        mode="signal",
        nav_active="signal",
    )


@app.get("/gullpris", response_class=HTMLResponse)
def page_gullpris(request: Request) -> HTMLResponse:
    seo_text_html = """
    <section class="wrap" style="padding-top:0">
      <div class="card">
        <h2>Om gullpris i dag</h2>
        <p>
          Gullpris i dag påvirkes av en kombinasjon av renter, inflasjon, dollarkurs, geopolitisk uro og generell
          risikovilje i markedene. Når investorer søker tryggere plasseringer, får gull ofte økt oppmerksomhet som
          en klassisk safe haven. Samtidig kan høyere realrenter og en sterkere amerikansk dollar legge press på
          gullprisen, siden gull ikke gir løpende rente. Derfor er det nyttig å følge både XAUUSD, sentralbank-signaler,
          inflasjonstall og bred markedsstemning når man vurderer gullmarkedet.
        </p>
        <p>
          På Gullbrief finner du daglig oppdatert gullpris, kort analyse, relevante nyheter og signalvurdering på ett sted.
          Målet er å gi et raskt og oversiktlig bilde av hva som driver markedet akkurat nå, uten unødvendig støy.
          For tradere og investorer som ønsker mer dybde, gir Premium tilgang til lengre analyser, signalhistorikk,
          arkiv og flere markedssaker. Siden er bygget for både lesbarhet, crawling og søkesynlighet, og oppdateres
          fortløpende med nye markedssignaler og nyhetsdrevne artikler på norsk og engelsk.
        </p>
      </div>
    </section>
    """
    return seo_landing(
        request,
        path="/gullpris",
        title="Gullpris i dag | Gold price today | pris, signal og nyheter",
        desc="Gullpris i dag med pris i USD, daglig analyse, prognose, signal og relevante nyheter om gull og XAUUSD. Følg gullmarkedet løpende.",
        h1="Gullpris i dag",
        intro="Dagens pris og signal, med korte drivere og relevante nyheter.",
        mode="analysis",
        nav_active="analysis",
        seo_text_html=seo_text_html,
        include_trade_link=True,
    )


@app.get("/handle-gull", response_class=HTMLResponse)
def page_trade_gull(request: Request) -> HTMLResponse:
    return trade_guide_page(
        request,
        path="/handle-gull",
        title="Hvordan trade gull | XAUUSD, trading og investering",
        desc="Guide til hvordan man kan trade gull, hva XAUUSD er, forskjellen på trading og investering, og hvordan en plattform som eToro kan brukes.",
        h1="Hvordan trade gull",
        intro="En enkel guide til gullhandel, XAUUSD og hva du bør vite før du trader gull.",
        lang="no",
        nav_active="trade_gull",
    )


@app.get("/hvordan-trade-gull", response_class=HTMLResponse)
def page_hvordan_trade_gull():
    return RedirectResponse(url="/handle-gull", status_code=301)


@app.get("/trade-gold", response_class=HTMLResponse)
def page_trade_gold(request: Request) -> HTMLResponse:
    return trade_guide_page(
        request,
        path="/trade-gold",
        title="How to Trade Gold | XAUUSD, trading vs investing and platforms",
        desc="Guide to how to trade gold, what XAUUSD is, the difference between trading and investing, and how a platform like eToro can be used.",
        h1="How to Trade Gold",
        intro="A practical guide to gold trading, XAUUSD and what to watch before trading gold.",
        lang="en",
        nav_active="trade_gold",
    )


@app.get("/how-to-trade-gold", response_class=HTMLResponse)
def page_how_to_trade_gold():
    return RedirectResponse(url="/trade-gold", status_code=301)


@app.head("/gullpris")
def page_gullpris_head() -> Response:
    return Response(status_code=200)




@app.get("/en", response_class=HTMLResponse)
def page_en_home():
    return RedirectResponse(url="/gold-price", status_code=302)


@app.get("/gold-price", response_class=HTMLResponse)
def page_gold_price(request: Request) -> HTMLResponse:
    seo_text_html = """
    <section class="wrap" style="padding-top:0">
      <div class="card">
        <h2>Gold price today</h2>
        <p>Follow the live gold price, daily analysis, forecast, signal and market drivers in one place. This English surface mirrors the Norwegian core pages so the same functions are available in both languages.</p>
      </div>
    </section>
    """
    return seo_landing(
        request,
        path="/gold-price",
        title="Gold price today | daily gold price, signal and news",
        desc="Gold price today with daily analysis, forecast, signal and relevant gold news in English.",
        h1="Gold price today",
        intro="Live gold price, signal and the main market drivers in English.",
        mode="analysis_en",
        nav_active="gold_price",
        lang="en",
        seo_text_html=seo_text_html,
        include_affiliate=True,
        include_trade_link=True,
    )


@app.get("/gold-price-analysis", response_class=HTMLResponse)
def page_gold_price_analysis(request: Request) -> HTMLResponse:
    return seo_landing(
        request,
        path="/gold-price-analysis",
        title="Gold price analysis | daily gold analysis and macro drivers",
        desc="Daily gold price analysis in English with signal, trend and macro drivers.",
        h1="Gold price analysis",
        intro="Daily English analysis of gold with focus on trend, signal and macro.",
        mode="analysis_en",
        nav_active="gold_analysis",
        lang="en",
        include_affiliate=True,
        include_trade_link=True,
    )


@app.get("/gold-signal", response_class=HTMLResponse)
def page_gold_signal(request: Request) -> HTMLResponse:
    return seo_landing(
        request,
        path="/gold-signal",
        title="Gold signal | bullish, bearish or neutral",
        desc="Gold signal with explanation in English. Premium includes signal history and archive.",
        h1="Gold signal",
        intro="See today's signal and why it is set.",
        mode="signal_en",
        nav_active="gold_signal",
        lang="en",
    )


@app.get("/xauusd-en", response_class=HTMLResponse)
def page_xauusd_en(request: Request) -> HTMLResponse:
    return seo_landing(
        request,
        path="/xauusd-en",
        title="XAUUSD analysis | gold vs USD",
        desc="English XAUUSD analysis: gold versus USD, rates, dollar and market drivers.",
        h1="XAUUSD",
        intro="Spot gold versus USD with focus on yields, the dollar and risk sentiment.",
        mode="xauusd_en",
        nav_active="xauusd_en",
        lang="en",
    )


@app.get("/handle-gull", response_class=HTMLResponse)
def page_handle_gull(request: Request) -> HTMLResponse:
    return trade_guide_page(
        request,
        path="/handle-gull",
        title="Hvordan handle gull | XAUUSD, plattform og risiko",
        desc="Guide til hvordan du kan handle gull, hva XAUUSD er og hva du bør vite før du starter.",
        h1="Hvordan handle gull",
        intro="En enkel guide til gullhandel, XAUUSD og hva du bør vite før du handler gull.",
        lang="no",
        nav_active="trade_gull",
    )


@app.get("/premium-en", response_class=HTMLResponse)
def premium_page_en(request: Request, session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME)) -> HTMLResponse:
    auth = resolve_auth_context(session_token=session_token, x_api_key=None)
    sent = request.query_params.get("sent") == "1"
    sent_email = str(request.query_params.get("email") or "")
    latest_news_html = render_recent_articles_box("en")
    body = _replace_many(
        PREMIUM_BODY_TEMPLATE,
        {
            "__APP_NAME__": _escape_html(APP_NAME),
            "__SITE_HEADER__": site_header("premium_en"),
            "__FOOTER__": footer_links(is_en=True),
            "__NAV_TABS__": nav_tabs("premium_en"),
            "__AUTH_BOX__": auth_login_box(next_url="/premium-en", sent=sent, email=sent_email, is_en=True),
            "__KEY_BOX__": key_fallback_box(is_en=True),
            "__LATEST_NEWS__": latest_news_html,
        },
    )
    body = body.replace("Mer data, mindre støy. Daglig premium-rapport, signalhistorikk, flere nyheter og arkiv.", "More data, less noise. Daily premium report, signal history, more headlines and archive.")
    body = body.replace("Dette får du", "What you get")
    body = body.replace("Kjøp Premium", "Buy Premium")
    body = body.replace("Skriv e-post og gå til Stripe checkout.", "Enter your email and continue to Stripe checkout.")
    body = body.replace("Hva rapporten inneholder", "What the report includes")
    body = body.replace("Daglig", "Daily")
    body = body.replace("Kjøp premium", "Buy premium")
    body = body.replace("Signalhistorikk (siste 30) + treffsikkerhet", "Signal history (last 30) + hit rate")
    body = body.replace("Arkiv med 7d/30d etter signal", "Archive with 7d/30d after signal")
    body = body.replace("Daglig premium-rapport på norsk, vesentlig lengre enn gratisanalyse", "Extended daily premium report")
    body = body.replace("Flere nyheter enn gratisversjonen", "More headlines than the free version")
    body = body.replace("E-postvarsler ved signalendring og daglig utsendelse", "Email alerts for signal changes and daily delivery")
    body = body.replace("E-post for kjøp", "Email for purchase")
    body = body.replace("Executive summary og marked akkurat nå", "Executive summary and current market picture")
    body = body.replace("Teknisk bilde med støtte, motstand, SMA og momentum", "Technical picture with support, resistance, SMA and momentum")
    body = body.replace("Makrodrivere og XAUUSD-vinkel", "Macro drivers and XAUUSD angle")
    body = body.replace("Hva som styrker og hva som bryter signalet", "What strengthens and what breaks the signal")
    body = body.replace("Watchlist neste 24–72t", "Watchlist for the next 24–72h")
    body = body.replace("Konklusjon med samlet vurdering", "Conclusion with overall assessment")
    body = body.replace("Skriv inn gyldig e-post.", "Enter a valid email.")
    body = body.replace("Åpner Stripe checkout…", "Opening Stripe checkout…")
    body = body.replace("Feil: ", "Error: ")
    if auth.get("authenticated") and auth.get("premium_active"):
        body = body.replace("<section class=\"grid\">", '<div class="card" style="margin-bottom:16px"><div class="title"><h2>Signed in</h2><div class="muted">Magic link</div></div><p class="muted">Signed in as <b>' + _escape_html(str(auth.get("email") or "")) + '</b>.</p><div class="btnrow"><button onclick="location.href=\'/archive-en\'">Open archive</button><button onclick="location.href=\'/auth/logout\'">Log out</button></div></div><section class="grid">', 1)
    return HTMLResponse(html_shell(request, title="Gullbrief Premium – gold price analysis, signal history and archive", description="Premium in English: archive, signal history, more market headlines and the longer daily report.", path="/premium-en", body_html=body, lang="en"))


@app.get("/archive-en", response_class=HTMLResponse)
def archive_page_en(request: Request) -> HTMLResponse:
    title = "Gullbrief archive – signal history and post-signal returns"
    desc = "See the latest snapshots for free. Premium includes full history, signal history and 7d/30d performance after signals."
    ensure_snapshot_persisted_from_public()
    dates = get_archive_dates(last_n_days=SITEMAP_ARCHIVE_DAYS)
    links = [f'<li><a href="/archive/{_escape_html(d)}">Archive {_escape_html(d)}</a></li>' for d in dates[:60]]
    sent = request.query_params.get("sent") == "1"
    sent_email = str(request.query_params.get("email") or "")
    archive_map_html = ("<div class='wrap archive-map'><div class='card' style='margin-top:12px'><div style='font-size:18px;font-weight:900'>Archive map</div><div class='muted'>Links to the latest days.</div>" + f"<ul>{''.join(links) if links else '<li class=\"muted\">No archive days yet.</li>'}</ul></div></div>")
    body = _replace_many(ARCHIVE_BODY_INNER, {"__APP_NAME__": _escape_html(APP_NAME), "__SITE_HEADER__": site_header("archive_en"), "__FOOTER__": footer_links(is_en=True), "__NAV_TABS__": nav_tabs("archive_en"), "__AUTH_BOX__": auth_login_box(next_url="/archive-en", sent=sent, email=sent_email, is_en=True), "__KEY_BOX__": key_fallback_box(is_en=True)})
    body = archive_map_html + body
    body = body.replace("Teaser (gratis)", "Teaser (free)").replace("Siste 3 snapshots. Full historikk ligger bak premium.", "Latest 3 snapshots. Full history is inside Premium.").replace("Premium", "Premium").replace("Logg inn med magic link, eller bruk premium-nøkkel som fallback.", "Sign in with a magic link, or use a premium key as fallback.")
    return HTMLResponse(html_shell(request, title=title, description=desc, path="/archive-en", body_html=body, lang="en"))

@app.get("/news", response_class=HTMLResponse)
def news_index_page(request: Request) -> HTMLResponse:
    return render_news_index_page(request, "en")


@app.get("/nyheter", response_class=HTMLResponse)
def nyheter_index_page(request: Request) -> HTMLResponse:
    return render_news_index_page(request, "no")


@app.get("/news/{slug}", response_class=HTMLResponse)
def news_article_page(request: Request, slug: str) -> HTMLResponse:
    article = get_news_article_by_slug("en", slug)
    if not article:
        return HTMLResponse(
            html_shell(
                request,
                title=f"{APP_NAME} – News",
                description="Article not found.",
                path=f"/news/{slug}",
                body_html="<div class='wrap'><div class='card'>Article not found.</div></div>",
                lang="en",
            ),
            status_code=404,
        )
    return render_news_article_page(request, article)


@app.get("/nyheter/{slug}", response_class=HTMLResponse)
def nyheter_article_page(request: Request, slug: str) -> HTMLResponse:
    article = get_news_article_by_slug("no", slug)
    if not article:
        return HTMLResponse(
            html_shell(
                request,
                title=f"{APP_NAME} – Nyheter",
                description="Artikkel ikke funnet.",
                path=f"/nyheter/{slug}",
                body_html="<div class='wrap'><div class='card'>Artikkel ikke funnet.</div></div>",
            ),
            status_code=404,
        )
    return render_news_article_page(request, article)


@app.get("/news/{year}", response_class=HTMLResponse)
def news_year_page(request: Request, year: str) -> HTMLResponse:
    if not (len(year) == 4 and year.isdigit()):
        return HTMLResponse("Not found", status_code=404)

    months = unique_news_months("en", year)
    if not months:
        return HTMLResponse("Not found", status_code=404)

    items = []
    for ym in months:
        y, m = ym.split("-")
        items.append(f'<li><a href="/news/{y}/{m}">{_escape_html(ym)}</a></li>')

    return render_news_archive_list(
        request=request,
        lang="en",
        title=f"Gold News Archive {year}",
        intro=f"Monthly archive for gold news and analysis published in {year}.",
        items_html="".join(items),
        path=f"/news/{year}",
    )


@app.get("/news/{year}/{month}", response_class=HTMLResponse)
def news_month_page(request: Request, year: str, month: str) -> HTMLResponse:
    if not (len(year) == 4 and year.isdigit() and len(month) == 2 and month.isdigit()):
        return HTMLResponse("Not found", status_code=404)

    days = unique_news_days("en", year, month)
    if not days:
        return HTMLResponse("Not found", status_code=404)

    items = []
    for d in days:
        y, m, day = d.split("-")
        items.append(f'<li><a href="/news/{y}/{m}/{day}">{_escape_html(d)}</a></li>')

    return render_news_archive_list(
        request=request,
        lang="en",
        title=f"Gold News Archive {year}-{month}",
        intro=f"Daily archive for gold news and analysis published in {year}-{month}.",
        items_html="".join(items),
        path=f"/news/{year}/{month}",
    )


@app.get("/news/{year}/{month}/{day}", response_class=HTMLResponse)
def news_day_page(request: Request, year: str, month: str, day: str) -> HTMLResponse:
    if not (
        len(year) == 4 and year.isdigit() and
        len(month) == 2 and month.isdigit() and
        len(day) == 2 and day.isdigit()
    ):
        return HTMLResponse("Not found", status_code=404)

    articles = [normalize_article_for_display(a) for a in filter_articles_by_day(get_news_articles_by_lang("en"), year, month, day)]
    if not articles:
        return HTMLResponse("Not found", status_code=404)

    items = []
    for article in articles:
        path = str(article.get("path") or "#")
        title_txt = str(article.get("title") or "")
        summary = str(article.get("summary") or "")
        items.append(
            "<li>"
            f'<a href="{_escape_html(path)}"><b>{_escape_html(title_txt)}</b></a><br/>'
            f'<span class="muted">{_escape_html(summary)}</span>'
            "</li>"
        )

    return render_news_archive_list(
        request=request,
        lang="en",
        title=f"Gold News for {year}-{month}-{day}",
        intro=f"Gold news and analysis published on {year}-{month}-{day}.",
        items_html="".join(items),
        path=f"/news/{year}/{month}/{day}",
    )

@app.get("/nyheter/{year}", response_class=HTMLResponse)
def nyheter_year_page(request: Request, year: str) -> HTMLResponse:
    if not (len(year) == 4 and year.isdigit()):
        return HTMLResponse("Not found", status_code=404)

    months = unique_news_months("no", year)
    if not months:
        return HTMLResponse("Not found", status_code=404)

    items = []
    for ym in months:
        y, m = ym.split("-")
        items.append(f'<li><a href="/nyheter/{y}/{m}">{_escape_html(ym)}</a></li>')

    return render_news_archive_list(
        request=request,
        lang="no",
        title=f"Gullnyheter arkiv {year}",
        intro=f"Månedsarkiv for gullnyheter og analyser publisert i {year}.",
        items_html="".join(items),
        path=f"/nyheter/{year}",
    )


@app.get("/nyheter/{year}/{month}", response_class=HTMLResponse)
def nyheter_month_page(request: Request, year: str, month: str) -> HTMLResponse:
    if not (len(year) == 4 and year.isdigit() and len(month) == 2 and month.isdigit()):
        return HTMLResponse("Not found", status_code=404)

    days = unique_news_days("no", year, month)
    if not days:
        return HTMLResponse("Not found", status_code=404)

    items = []
    for d in days:
        y, m, day = d.split("-")
        items.append(f'<li><a href="/nyheter/{y}/{m}/{day}">{_escape_html(d)}</a></li>')

    return render_news_archive_list(
        request=request,
        lang="no",
        title=f"Gullnyheter arkiv {year}-{month}",
        intro=f"Dagsarkiv for gullnyheter og analyser publisert i {year}-{month}.",
        items_html="".join(items),
        path=f"/nyheter/{year}/{month}",
    )


@app.get("/nyheter/{year}/{month}/{day}", response_class=HTMLResponse)
def nyheter_day_page(request: Request, year: str, month: str, day: str) -> HTMLResponse:
    if not (
        len(year) == 4 and year.isdigit() and
        len(month) == 2 and month.isdigit() and
        len(day) == 2 and day.isdigit()
    ):
        return HTMLResponse("Not found", status_code=404)

    articles = [normalize_article_for_display(a) for a in filter_articles_by_day(get_news_articles_by_lang("no"), year, month, day)]
    if not articles:
        return HTMLResponse("Not found", status_code=404)

    items = []
    for article in articles:
        path = str(article.get("path") or "#")
        title_txt = str(article.get("title") or "")
        summary = str(article.get("summary") or "")
        items.append(
            "<li>"
            f'<a href="{_escape_html(path)}"><b>{_escape_html(title_txt)}</b></a><br/>'
            f'<span class="muted">{_escape_html(summary)}</span>'
            "</li>"
        )

    return render_news_archive_list(
        request=request,
        lang="no",
        title=f"Gullnyheter for {year}-{month}-{day}",
        intro=f"Gullnyheter og analyser publisert {year}-{month}-{day}.",
        items_html="".join(items),
        path=f"/nyheter/{year}/{month}/{day}",
    )


@app.post("/auth/request-link")
async def auth_request_link(request: Request, email: str = Form(...), next_url: str = Form("/archive")):
    email_n = normalize_email(email)
    if "@" not in email_n:
        return HTMLResponse("Ugyldig e-post.", status_code=400)

    try:
        request_magic_link(email_n, request, next_url=next_url)
        return RedirectResponse(url=f"{next_url}?sent=1&email={quote(email_n)}", status_code=303)
    except Exception as e:
        return HTMLResponse(f"Kunne ikke sende magic link: {_escape_html(str(e))}", status_code=500)


@app.get("/auth/magic")
def auth_magic_link(request: Request, t: str):
    consumed = consume_magic_link(t, request=request)
    if not consumed:
        return HTMLResponse("Magic link er ugyldig eller utløpt.", status_code=400)

    target = str(consumed.get("next_url") or "/archive")
    resp = RedirectResponse(url=target, status_code=303)
    resp.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=str(consumed.get("session_token") or ""),
        max_age=SESSION_TTL_DAYS * 24 * 60 * 60,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite="lax",
        path="/",
    )
    return resp


@app.get("/auth/logout")
def auth_logout(session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME)):
    revoke_web_session(session_token)
    resp = RedirectResponse(url="/premium", status_code=303)
    resp.delete_cookie(SESSION_COOKIE_NAME, path="/")
    return resp


@app.get("/kontakt", response_class=HTMLResponse)
def kontakt_page(request: Request) -> HTMLResponse:
    content = f"""
    <h2>Kontakt</h2>
    <p>Har du spørsmål om Premium, betaling, tilgang, samarbeid eller tekniske problemer, kan du kontakte oss på e-post.</p>

    <h3>E-post</h3>
    <p><a href="mailto:{_escape_html(CONTACT_EMAIL)}">{_escape_html(CONTACT_EMAIL)}</a></p>

    <h3>Om tjenesten</h3>
    <p>{_escape_html(APP_NAME)} publiserer daglige markedskommentarer om gullpris, XAUUSD, signaler og relaterte nyhetsdrivere.</p>

    <h3>Viktig</h3>
    <p>Innholdet er kun ment som informasjon og markedskommentar. Det er ikke investeringsrådgivning, personlig rådgivning eller en oppfordring til kjøp eller salg av finansielle instrumenter.</p>
    """
    return legal_page(request, path="/kontakt", title="Kontakt", intro="Kontaktinformasjon for spørsmål om Gullbrief, Premium og tilgang.", content_html=content)


@app.get("/terms", response_class=HTMLResponse)
def terms_page(request: Request) -> HTMLResponse:
    org_line = f"<p><b>Leverandør:</b> {_escape_html(LEGAL_COMPANY_NAME)}</p>"
    if LEGAL_ORGNO:
        org_line += f"<p><b>Org.nr:</b> {_escape_html(LEGAL_ORGNO)}</p>"
    if LEGAL_ADDRESS:
        org_line += f"<p><b>Adresse:</b> {_escape_html(LEGAL_ADDRESS)}</p>"

    content = f"""
    <h2>Vilkår</h2>
    {org_line}
    <p><b>Kontakt:</b> <a href="mailto:{_escape_html(CONTACT_EMAIL)}">{_escape_html(CONTACT_EMAIL)}</a></p>

    <h3>1. Om tjenesten</h3>
    <p>{_escape_html(APP_NAME)} leverer informasjon, markedskommentarer, signaler og analyser relatert til gullpris og XAUUSD. Tjenesten leveres som den er, og innhold kan endres uten varsel.</p>

    <h3>2. Ikke investeringsråd</h3>
    <p>Alt innhold er kun ment som generell informasjon. Innholdet utgjør ikke investeringsråd, finansiell rådgivning eller personlig anbefaling. Du er selv ansvarlig for egne beslutninger.</p>

    <h3>3. Premium og betaling</h3>
    <p>Premium gir tilgang til utvidet innhold som arkiv, signalhistorikk, flere nyheter og lengre rapporter. Betaling håndteres via Stripe. Ved tekniske problemer med aktivering kan du kontakte oss.</p>

    <h3>4. Tilgang</h3>
    <p>Premium-tilgang er personlig og skal ikke deles videre. Misbruk, automatisert uthenting eller forsøk på å omgå tilgangskontroll kan føre til stenging av tilgang.</p>

    <h3>5. Ansvarsbegrensning</h3>
    <p>Vi forsøker å holde informasjonen oppdatert, men garanterer ikke for fullstendighet, korrekthet eller tilgjengelighet til enhver tid. Vi er ikke ansvarlige for tap, direkte eller indirekte, som følge av bruk av tjenesten.</p>

    <h3>6. Endringer</h3>
    <p>Disse vilkårene kan oppdateres. Den til enhver tid publiserte versjonen på nettstedet gjelder.</p>

    <h3>Financial disclaimer</h3>
    <p>Innholdet på Gullbrief er kun ment som generell informasjon og markedskommentar. Det utgjør ikke investeringsråd, finansiell rådgivning eller en anbefaling om å kjøpe eller selge finansielle instrumenter.</p>

    <p>Forfatteren gir ingen garanti for nøyaktighet eller fullstendighet. All bruk av informasjon fra nettstedet skjer på eget ansvar.</p>

    <h3>Market commentary</h3>
    <p>Analyser, signaler og prognoser er basert på tilgjengelige data, tekniske indikatorer og offentlige nyhetskilder. Disse kan endre seg raskt og skal ikke tolkes som garantier for fremtidig utvikling.</p>
    """
    return legal_page(request, path="/terms", title="Terms", intro="Vilkår for bruk av Gullbrief og Premium.", content_html=content)


@app.get("/privacy", response_class=HTMLResponse)
def privacy_page(request: Request) -> HTMLResponse:
    content = f"""
    <h2>Personvern</h2>
    <p>Denne siden beskriver hvordan {_escape_html(APP_NAME)} behandler personopplysninger.</p>

    <h3>1. Hvilke opplysninger som kan behandles</h3>
    <ul>
      <li>E-postadresse ved kjøp eller påmelding til varsler</li>
      <li>Premium-nøkkel og tilhørende abonnementsstatus</li>
      <li>Tekniske data som kan oppstå i forbindelse med bruk av nettstedet og betaling</li>
    </ul>

    <h3>2. Formål</h3>
    <ul>
      <li>Levere Premium-tilgang</li>
      <li>Sende daglige rapporter eller signalvarsler hvis du har meldt deg på</li>
      <li>Håndtere betaling og kundeservice</li>
      <li>Forebygge misbruk og sikre stabil drift</li>
    </ul>

    <h3>3. Betaling</h3>
    <p>Betaling behandles av Stripe. Kortdata håndteres ikke direkte av {_escape_html(APP_NAME)}.</p>

    <h3>4. Lagring</h3>
    <p>Vi lagrer bare opplysninger som er nødvendige for å levere tjenesten. E-post og abonnementsstatus kan lagres så lenge det er nødvendig for aktiv tilgang, varslinger eller oppfølging.</p>

    <h3>5. Deling</h3>
    <p>Opplysninger deles ikke med uvedkommende, med unntak av nødvendige tredjepartsleverandører for betaling og utsendelse, som Stripe og Brevo, når dette brukes.</p>

    <h3>6. Dine rettigheter</h3>
    <p>Du kan be om innsyn, retting eller sletting av opplysninger ved å kontakte oss på <a href="mailto:{_escape_html(CONTACT_EMAIL)}">{_escape_html(CONTACT_EMAIL)}</a>.</p>
    """
    return legal_page(request, path="/privacy", title="Privacy", intro="Informasjon om hvordan Gullbrief behandler personopplysninger.", content_html=content)


# =============================================================================
# Public chart / rebuild helpers
# =============================================================================

def public_chart_points(days: int = 7) -> List[Dict[str, Any]]:
    days = max(2, min(days, 30))
    rows = get_history_rows_resilient(limit=max(days, 60))
    points: List[Dict[str, Any]] = []
    for r in rows[-days:]:
        close = safe_float(r.get("price_usd"))
        ts = str(r.get("updated_at") or "")
        if close is not None and ts:
            points.append({"ts": ts, "close": close})
    if len(points) >= 2:
        return points
    try:
        chart = fetch_yahoo_chart(YAHOO_SYMBOL, range_="1mo", interval="1d")
        result = chart["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        stamps = result.get("timestamp") or []
        for stamp, close in zip(stamps, closes):
            v = safe_float(close)
            if v is None:
                continue
            points.append({"ts": datetime.fromtimestamp(int(stamp), tz=timezone.utc).isoformat(), "close": v})
    except Exception:
        pass
    return points[-days:]


def rebuild_history_from_yahoo(days: int = 7) -> Dict[str, Any]:
    days = max(1, min(days, 30))
    existing = {date_yyyy_mm_dd_from_iso_or_rss(str(r.get("updated_at") or "")) for r in read_history(limit=5000)}
    chart = fetch_yahoo_chart(YAHOO_SYMBOL, range_="3mo", interval="1d")
    result = chart["chart"]["result"][0]
    closes = result["indicators"]["quote"][0]["close"]
    stamps = result.get("timestamp") or []
    inserted = 0
    points = []
    for stamp, close in zip(stamps, closes):
        v = safe_float(close)
        if v is None:
            continue
        ts = datetime.fromtimestamp(int(stamp), tz=timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0).isoformat()
        day = date_yyyy_mm_dd_from_iso_or_rss(ts)
        if not day:
            continue
        points.append((day, ts, v))
    for day, ts, v in points[-days:]:
        if day in existing:
            continue
        rec = {
            "updated_at": ts,
            "version": APP_VERSION,
            "symbol": YAHOO_SYMBOL,
            "price_usd": v,
            "change_pct": None,
            "signal": "neutral",
            "signal_reason": "Rebuilt from Yahoo daily history after deploy.",
            "rsi14": None,
            "trend_score": None,
            "levels": {},
            "macro_summary": "Historical snapshot rebuilt after deploy.",
            "analysis": "Historical snapshot rebuilt after deploy.",
            "forecast": "",
            "forecast_en": "",
            "xauusd": "",
            "premium_insight": "",
            "headlines": [],
        }
        store_snapshot_if_needed(rec)
        inserted += 1
    return {"ok": True, "inserted": inserted, "days": days}


def should_generate_market_driver(snapshot: Dict[str, Any], headlines: List[Dict[str, str]]) -> bool:
    change_pct = safe_float(snapshot.get("change_pct"))
    signal = str(snapshot.get("signal") or "neutral").lower()

    if change_pct is not None and abs(change_pct) >= 1.2:
        return True

    blob = " ".join(_headline_titles(headlines, 8)).lower()
    trigger_words = ["fed", "inflation", "cpi", "pce", "yields", "treasury", "war", "geopolitical", "central bank"]
    hits = sum(1 for word in trigger_words if word in blob)
    move_ok = change_pct is not None and abs(change_pct) >= 0.8

    return (
        hits >= 2
        or (
            signal in ("bullish", "bearish")
            and (move_ok or hits >= 1)
        )
    )


@app.get("/api/public/chart")
def api_public_chart(days: int = 7):
    return JSONResponse({"points": public_chart_points(days), "days": max(2, min(days, 30))})


@app.post("/api/tasks/rebuild-history")
def api_rebuild_history(days: int = 7, x_api_key: Optional[str] = Header(default=None)):
    if x_api_key != ADMIN_API_KEY:
        return JSONResponse({"message": "UNAUTHORIZED"}, status_code=401)
    try:
        return JSONResponse(rebuild_history_from_yahoo(days=days))
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


@app.post("/api/tasks/rebuild-last-week")
def api_rebuild_last_week(x_api_key: Optional[str] = Header(default=None)):
    if x_api_key != ADMIN_API_KEY:
        return JSONResponse({"message": "UNAUTHORIZED"}, status_code=401)
    try:
        hist = rebuild_history_from_yahoo(days=7)
        news = generate_news_range(days=7)
        return JSONResponse({"ok": True, "history": hist, "news": news})
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


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
    rows = get_history_rows_resilient(limit=50)
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
def api_history(
    limit: int = 200,
    x_api_key: Optional[str] = Header(default=None),
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    auth = resolve_auth_context(session_token=session_token, x_api_key=x_api_key)
    if not auth["authenticated"] or not auth["premium_active"]:
        return JSONResponse({"message": "UNAUTHORIZED"}, status_code=401)

    limit = max(1, min(limit, 1000))
    rows = get_history_rows_resilient(limit=limit)
    rows = add_forward_returns(rows)
    rows_out = list(reversed(rows))
    stats = signal_stats_last30(rows_out)
    return JSONResponse({"items": rows, "count": len(rows), "stats": stats, "auth_via": auth.get("via")})


@app.post("/api/premium/subscribe-email")
async def api_subscribe_email(
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
    session_token: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE_NAME),
):
    auth = resolve_auth_context(session_token=session_token, x_api_key=x_api_key)
    if not auth["authenticated"] or not auth["premium_active"]:
        return JSONResponse({"message": "UNAUTHORIZED"}, status_code=401)

    try:
        body = await request.json()
    except Exception:
        body = {}
    email = normalize_email(str(body.get("email") or auth.get("email") or ""))
    if "@" not in email:
        return JSONResponse({"message": "Ugyldig e-post."}, status_code=400)

    api_key = str(auth.get("api_key") or get_active_api_key_for_email(email) or "")
    if not api_key:
        return JSONResponse({"message": "NO_ACTIVE_API_KEY"}, status_code=400)

    conn = _db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO email_subscriptions(api_key,email,created_at,last_notified_signal,last_daily_sent_date,last_macro_sent_date) VALUES(?,?,?,?,?,?)",
            (api_key, email, iso_now(), None, None, None),
        )
        conn.commit()
    finally:
        conn.close()

    return JSONResponse({"ok": True, "email": email})


@app.post("/api/tasks/send-premium-daily")
def api_send_premium_daily(
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
):
    if x_api_key != "gb_test_12345":
        return JSONResponse({"message": "UNAUTHORIZED"}, status_code=401)

    today = datetime.now(timezone.utc).date().isoformat()
    base = get_base_url(request)

    latest = None
    rows = get_history_rows_resilient(limit=1)
    if rows:
        latest = rows[-1]
    if not latest:
        latest = read_public_snapshot() or {}

    price_usd = safe_float(latest.get("price_usd"))
    change_pct = safe_float(latest.get("change_pct"))
    signal = str(latest.get("signal") or "neutral").upper()

    price_txt = f"${price_usd:,.2f}" if price_usd is not None else "ukjent"
    chg_txt = f"{change_pct:+.2f}%" if change_pct is not None else "ukjent"

    subject = f"{APP_NAME} Premium – dagens rapport er klar"
    body = (
        f"Hei!\n\n"
        f"Dagens premium-rapport er nå publisert.\n\n"
        f"Gull: {price_txt}\n"
        f"Døgnendring: {chg_txt}\n"
        f"Signal: {signal}\n\n"
        f"Åpne rapporten her:\n"
        f"{base}/premium\n\n"
        f"Hvis du ikke er innlogget, kan du be om magic link på premium-siden.\n\n"
        f"Hilsen\n"
        f"{APP_NAME}"
    )

    conn = _db()
    sent = 0
    skipped = 0
    errors = []

    try:
        subs = conn.execute(
            "SELECT email, last_daily_sent_date FROM email_subscriptions ORDER BY created_at DESC"
        ).fetchall()

        for row in subs:
            email = normalize_email(str(row["email"] or ""))
            last_sent = str(row["last_daily_sent_date"] or "")

            if "@" not in email:
                skipped += 1
                continue

            if last_sent == today:
                skipped += 1
                continue

            if not email_has_active_premium(email):
                skipped += 1
                continue

            try:
                send_email(email, subject, body)
                conn.execute(
                    "UPDATE email_subscriptions SET last_daily_sent_date=? WHERE email=?",
                    (today, email),
                )
                sent += 1
            except Exception as e:
                errors.append({"email": email, "message": str(e)})

        conn.commit()
    finally:
        conn.close()

    return JSONResponse(
        {
            "ok": True,
            "date": today,
            "sent": sent,
            "skipped": skipped,
            "errors": errors[:10],
        }
    )


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

        result = {"enabled": SOCIAL_DAILY_ENABLED, "configured": x_configured(), "text": post["text"]}

        if SOCIAL_DAILY_ENABLED and x_configured():
            send_result = send_social_post(post["text"])
            result["send_result"] = send_result
        else:
            result["send_result"] = {"ok": False, "message": "SOCIAL_DISABLED_OR_NOT_CONFIGURED"}

        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


# =============================================================================
# News task API
# =============================================================================

@app.get("/api/tasks/generate-news")
def api_generate_news(x_api_key: Optional[str] = Header(default=None), force_date: Optional[str] = None):
    if x_api_key != ADMIN_API_KEY:
        return JSONResponse({"message": "UNAUTHORIZED"}, status_code=401)
    if not NEWS_DAILY_ENABLED:
        return JSONResponse({"ok": False, "message": "NEWS_DAILY_DISABLED"}, status_code=400)
    try:
        result = generate_and_store_daily_news(force_date=force_date)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)


@app.get("/api/tasks/regenerate-news")
def api_regenerate_news(
    x_api_key: Optional[str] = Header(default=None),
    days: int = 7,
    end_date: Optional[str] = None,
):
    if x_api_key != ADMIN_API_KEY:
        return JSONResponse({"message": "UNAUTHORIZED"}, status_code=401)
    try:
        result = generate_news_range(days=days, end_date=end_date)
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
    email = normalize_email(str(body.get("email") or "").strip())
    if "@" not in email:
        return JSONResponse({"message": "Ugyldig e-post."}, status_code=400)

    try:
        ensure_user(email)
        env = require_stripe(request)
        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            customer_email=email,
            line_items=[{"price": env["price_id"], "quantity": 1}],
            success_url=env["success_url"],
            cancel_url=env["cancel_url"],
            allow_promotion_codes=True,
            metadata={"app": APP_NAME, "email_hash": _hash_email(email), "email": email},
        )
        return JSONResponse({"ok": True, "url": session.url})
    except Exception as e:
        return JSONResponse({"message": str(e)}, status_code=500)


def _already_processed(event_id: str) -> bool:
    conn = _db()
    row = conn.execute("SELECT 1 FROM stripe_events WHERE event_id=?", (event_id,)).fetchone()
    conn.close()
    return bool(row)


def _mark_processed(event_id: str, event_type: str) -> None:
    conn = _db()
    conn.execute("INSERT OR IGNORE INTO stripe_events(event_id, event_type, created_at) VALUES(?,?,?)", (event_id, event_type, iso_now()))
    conn.commit()
    conn.close()


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
                or str(data_obj.get("metadata", {}).get("email") or "")
            )
            if customer_id or subscription_id or email:
                sync_premium_from_stripe(email=email, customer_id=customer_id, subscription_id=subscription_id, status="active")

        elif event_type in ("customer.subscription.created", "customer.subscription.updated"):
            customer_id = str(data_obj.get("customer") or "")
            subscription_id = str(data_obj.get("id") or "")
            status = str(data_obj.get("status") or "")
            mapped = "active" if status in ("active", "trialing") else "inactive"
            email = ""
            try:
                customer = stripe.Customer.retrieve(customer_id) if customer_id else None
                if customer:
                    email = normalize_email(str(getattr(customer, "email", "") or customer.get("email", "")))  # type: ignore
            except Exception:
                email = ""
            if customer_id or subscription_id or email:
                sync_premium_from_stripe(email=email, customer_id=customer_id, subscription_id=subscription_id, status=mapped)

        elif event_type in ("customer.subscription.deleted",):
            customer_id = str(data_obj.get("customer") or "")
            subscription_id = str(data_obj.get("id") or "")
            email = ""
            try:
                customer = stripe.Customer.retrieve(customer_id) if customer_id else None
                if customer:
                    email = normalize_email(str(getattr(customer, "email", "") or customer.get("email", "")))  # type: ignore
            except Exception:
                email = ""

            if customer_id:
                _set_key_status_for_customer(customer_id, "inactive")
            if subscription_id:
                _set_key_status_for_subscription(subscription_id, "inactive")
            if email:
                update_user_premium_state(
                    email=email,
                    premium_status="inactive",
                    stripe_customer_id=customer_id,
                    stripe_subscription_id=subscription_id,
                )

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
    news_store = read_news_store()
    archive_count = len(load_news_archive())
    history_exists = bool(get_history_rows_resilient(limit=1))
    return JSONResponse(
        {
            "status": "ok",
            "ts": iso_now(),
            "yahoo_symbol": YAHOO_SYMBOL,
            "cache_ttl_seconds": CACHE_TTL_SECONDS,
            "openai_enabled": bool(OPENAI_API_KEY),
            "rss_feeds": RSS_FEEDS,
            "history_path": HISTORY_PATH,
            "history_available": history_exists,
            "db_path": DB_PATH,
            "public_snapshot_path": PUBLIC_SNAPSHOT_PATH,
            "public_snapshot_exists": bool(snapshot),
            "news_path": NEWS_PATH,
            "news_count": len(news_store.get("articles") or []),
            "news_archive_path": NEWS_ARCHIVE_PATH,
            "news_archive_count": archive_count,
            "admin_key_configured": bool(ADMIN_API_KEY),
            "stripe_enabled": stripe_ready(),
            "stripe_secret_len": len(stripe_env()["secret_key"]),
            "stripe_price_id_prefix": stripe_env()["price_id"][:10] + "..." if stripe_env()["price_id"] else "",
            "stripe_webhook_secret_set": bool(stripe_env()["webhook_secret"]),
            "smtp_enabled": brevo_configured(),
            "social_daily_enabled": SOCIAL_DAILY_ENABLED,
            "social_configured": x_configured(),
            "news_daily_enabled": NEWS_DAILY_ENABLED,
            "session_cookie_name": SESSION_COOKIE_NAME,
            "etoro_no_configured": bool(ETORO_AFFILIATE_NO),
            "etoro_en_configured": bool(ETORO_AFFILIATE_EN),
            "english_surface_enabled": True,
            "version": APP_VERSION,
        }
    )


@app.get("/robots.txt")
def robots_txt(request: Request):
    base = get_base_url(request)
    txt = f"User-agent: *\nAllow: /\n\nSitemap: {base}/sitemap.xml\nSitemap: {base}/news-sitemap.xml\n"
    return PlainTextResponse(txt)


@app.get("/feed.xml")
def feed_xml(request: Request):
    base = get_base_url(request)
    rows = list(reversed(get_history_rows_resilient(limit=max(FEED_ITEMS, 5))))
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
        "/gold-price",
        "/gold-price-analysis",
        "/gold-price-forecast",
        "/gold-signal",
        "/xauusd",
        "/xauusd-en",
        "/handle-gull",
        "/trade-gold",
        "/news",
        "/nyheter",
        "/premium",
        "/premium-en",
        "/archive-en",
        "/archive",
        "/kontakt",
        "/terms",
        "/privacy",
    ]

    news_urls = [str(a.get("path") or "") for a in get_all_news_articles() if str(a.get("path") or "")]

    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

    for p in static_urls + news_urls:
        if not p:
            continue
        changefreq = "daily" if p not in ("/premium", "/premium-en", "/archive", "/archive-en", "/terms", "/privacy", "/kontakt") else "weekly"
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
    cutoff = utc_now() - timedelta(days=2)

    articles: List[Dict[str, Any]] = []
    for article in get_all_news_articles():
        published_at = str(article.get("published_at") or "")
        dt = parse_iso_or_rss(published_at)
        if not published_at or not dt:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt >= cutoff:
            articles.append(article)

    articles.sort(key=lambda a: str(a.get("published_at") or ""), reverse=True)

    parts = ['<?xml version="1.0" encoding="UTF-8"?>']
    parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">')

    for article in articles[:200]:
        lang = "en" if str(article.get("lang") or "") == "en" else "no"
        path = str(article.get("path") or "")
        if not path:
            continue
        title = str(normalize_article_for_display(article).get("title") or APP_NAME)
        published_at = str(article.get("published_at") or "")
        if not published_at:
            continue

        parts.append(
            "<url>"
            f"<loc>{_escape_html(base + path)}</loc>"
            "<news:news>"
            "<news:publication>"
            f"<news:name>{_escape_html(NEWS_PUBLISHER_NAME)}</news:name>"
            f"<news:language>{_escape_html(lang)}</news:language>"
            "</news:publication>"
            f"<news:publication_date>{_escape_html(published_at)}</news:publication_date>"
            f"<news:title>{_escape_html(title)}</news:title>"
            "</news:news>"
            "</url>"
        )

    parts.append("</urlset>")
    return Response("".join(parts), media_type="application/xml")


@app.get(f"/{GOOGLE_SITE_VERIFICATION}")
def google_site_verification():
    return PlainTextResponse(GOOGLE_SITE_VERIFICATION)


# =============================================================================
# Optional local run
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)     
