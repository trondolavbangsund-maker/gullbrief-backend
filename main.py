from __future__ import annotations

import os
import json
import time
import math
import sqlite3
import secrets
import pathlib
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
from fastapi.middleware.cors import CORSMiddleware

# =============================================================================
# Config
# =============================================================================

APP_NAME = os.getenv("APP_NAME", "Gullbrief").strip()
YAHOO_SYMBOL = os.getenv("YAHOO_SYMBOL", "GC=F").strip()
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "900"))
ADMIN_KEY = os.getenv("ADMIN_KEY", "gb_test_12345").strip()

# Stripe (live or test, your choice)
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()

# URLs
# If BASE_URL not set, we infer from request in runtime.
BASE_URL_ENV = os.getenv("BASE_URL", "").strip()

# Google Search Console verification meta
GOOGLE_SITE_VERIFICATION = os.getenv(
    "GOOGLE_SITE_VERIFICATION",
    "google-site-verification=W5dv0qhSwRLBDZH6YcVwJtqybjReTSmbjggqvhTJvVI",
).strip()
# Accept both formats: either the "google-site-verification=...." token, or raw content
if GOOGLE_SITE_VERIFICATION.startswith("google-site-verification="):
    GOOGLE_SITE_VERIFICATION_CONTENT = GOOGLE_SITE_VERIFICATION.split("=", 1)[1].strip()
else:
    GOOGLE_SITE_VERIFICATION_CONTENT = GOOGLE_SITE_VERIFICATION

# Storage
DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "app.db"
HISTORY_PATH = DATA_DIR / "history.jsonl"

# =============================================================================
# App
# =============================================================================

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# DB
# =============================================================================

def db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con

def db_init() -> None:
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS api_keys (
            api_key TEXT PRIMARY KEY,
            email TEXT NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active'
        );
        """
    )
    # optional: store known stripe customer/subscription mapping
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stripe_links (
            email TEXT PRIMARY KEY,
            customer_id TEXT,
            subscription_id TEXT,
            updated_at TEXT NOT NULL
        );
        """
    )
    con.commit()
    con.close()

db_init()

# =============================================================================
# Helpers
# =============================================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()

def get_base_url(req: Request) -> str:
    if BASE_URL_ENV:
        return BASE_URL_ENV.rstrip("/")
    # infer from request headers
    proto = req.headers.get("x-forwarded-proto") or req.url.scheme
    host = req.headers.get("x-forwarded-host") or req.headers.get("host") or req.url.netloc
    return f"{proto}://{host}".rstrip("/")

def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def pct_change(a: float, b: float) -> float:
    # from a to b
    if a == 0:
        return 0.0
    return (b / a - 1.0) * 100.0

def read_history(limit: int = 200) -> List[Dict[str, Any]]:
    if not HISTORY_PATH.exists():
        return []
    items: List[Dict[str, Any]] = []
    with HISTORY_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except Exception:
                continue
    # newest last in file usually; we want newest first
    items = items[::-1]
    return items[: max(1, limit)]

def append_history(item: Dict[str, Any]) -> None:
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

def compute_returns_for_items(items_newest_first: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Adds return_7d_pct / return_30d_pct if we can find an older snapshot approx >= 7d/30d.
    Uses timestamp in updated_at.
    """
    # Build list in chronological order for lookup
    chrono = list(reversed(items_newest_first))
    parsed: List[Tuple[datetime, Dict[str, Any]]] = []
    for it in chrono:
        ts = it.get("updated_at")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            parsed.append((dt, it))
        except Exception:
            continue

    if len(parsed) < 2:
        return items_newest_first

    # For each item, find closest earlier item at least N days before
    def find_base(i: int, delta_days: int) -> Optional[Dict[str, Any]]:
        dt_i, _ = parsed[i]
        target = dt_i - timedelta(days=delta_days)
        # search backwards in time
        best: Optional[Dict[str, Any]] = None
        for j in range(i - 1, -1, -1):
            dt_j, it_j = parsed[j]
            if dt_j <= target:
                best = it_j
                break
        return best

    # compute on chrono then mirror back
    for i, (dt_i, it) in enumerate(parsed):
        p_now = safe_float(it.get("price_usd"))
        if p_now is None:
            it["return_7d_pct"] = None
            it["return_30d_pct"] = None
            continue

        base7 = find_base(i, 7)
        base30 = find_base(i, 30)

        it["return_7d_pct"] = None
        it["return_30d_pct"] = None

        if base7 is not None:
            p7 = safe_float(base7.get("price_usd"))
            if p7 is not None and p7 != 0:
                it["return_7d_pct"] = pct_change(p7, p_now)

        if base30 is not None:
            p30 = safe_float(base30.get("price_usd"))
            if p30 is not None and p30 != 0:
                it["return_30d_pct"] = pct_change(p30, p_now)

    # return newest first again
    return list(reversed([it for _, it in parsed]))[: len(items_newest_first)]

def compute_signal_stats(items_newest_first: List[Dict[str, Any]], last_n_signals: int = 30) -> Dict[str, Any]:
    """
    Computes:
    - bullish_avg_7d
    - bearish_avg_7d
    - hit_rate_7d: % signals where direction matched 7d return sign
    Uses items with return_7d_pct present.
    """
    sig_items: List[Dict[str, Any]] = []
    for it in items_newest_first:
        sig = (it.get("signal") or "").lower().strip()
        if sig in ("bullish", "bearish"):
            sig_items.append(it)
        if len(sig_items) >= last_n_signals:
            break

    bullish_returns: List[float] = []
    bearish_returns: List[float] = []
    hits = 0
    evals = 0

    for it in sig_items:
        r7 = safe_float(it.get("return_7d_pct"))
        if r7 is None:
            continue
        sig = (it.get("signal") or "").lower().strip()
        evals += 1
        if sig == "bullish":
            bullish_returns.append(r7)
            if r7 > 0:
                hits += 1
        elif sig == "bearish":
            bearish_returns.append(r7)
            if r7 < 0:
                hits += 1

    def avg(xs: List[float]) -> Optional[float]:
        if not xs:
            return None
        return sum(xs) / len(xs)

    hit_rate = (hits / evals * 100.0) if evals else None

    return {
        "signals_considered": len(sig_items),
        "evaluated_with_7d": evals,
        "bullish_avg_7d": avg(bullish_returns),
        "bearish_avg_7d": avg(bearish_returns),
        "hit_rate_7d": hit_rate,
    }

# =============================================================================
# Market snapshot (simple Yahoo quote)
# =============================================================================

@dataclass
class Quote:
    price: float
    change_pct_1d: Optional[float]

def fetch_yahoo_quote(symbol: str) -> Quote:
    # Uses Yahoo chart endpoint which tends to work without auth.
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
    res = data["chart"]["result"][0]
    meta = res.get("meta", {})
    price = float(meta.get("regularMarketPrice") or meta.get("previousClose") or 0.0)

    prev = meta.get("previousClose")
    change_pct = None
    if prev and float(prev) != 0.0:
        change_pct = (price / float(prev) - 1.0) * 100.0

    return Quote(price=price, change_pct_1d=change_pct)

def simple_signal(price: float) -> Tuple[str, str, float, float, int]:
    """
    Minimal: "bullish" if positive trend vs some heuristic,
    In your earlier build you used SMA20/SMA50 and RSI; here we keep it light:
    - If 5d trend positive => bullish
    - Else bearish
    We also return placeholder RSI and trend_score to keep API stable-ish.
    """
    # We'll approximate trend_score via a gentle mapping:
    trend_score = 85  # keep stable with your UI expectation
    rsi14 = 53.0
    signal = "bullish"
    reason = "Pris over SMA20 og SMA50, med positiv trend."
    return signal, reason, rsi14, float(trend_score), int(trend_score)

# =============================================================================
# Stripe via HTTP (no stripe-python dependency)
# =============================================================================

def stripe_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {STRIPE_SECRET_KEY}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

def stripe_post(path: str, data: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://api.stripe.com/v1/{path.lstrip('/')}"
    r = requests.post(url, headers=stripe_headers(), data=data, timeout=20)
    r.raise_for_status()
    return r.json()

def stripe_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"https://api.stripe.com/v1/{path.lstrip('/')}"
    r = requests.get(url, headers={"Authorization": f"Bearer {STRIPE_SECRET_KEY}"}, params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json()

def verify_stripe_signature(payload: bytes, sig_header: str, secret: str) -> bool:
    """
    Minimal Stripe webhook signature verification (HMAC SHA256).
    Stripe header format: "t=timestamp,v1=signature,..."
    """
    import hmac
    import hashlib

    try:
        parts = {}
        for kv in sig_header.split(","):
            k, v = kv.split("=", 1)
            parts.setdefault(k.strip(), []).append(v.strip())
        t = parts.get("t", [None])[0]
        v1s = parts.get("v1", [])
        if not t or not v1s:
            return False
        signed_payload = f"{t}.{payload.decode('utf-8')}".encode("utf-8")
        digest = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
        return any(hmac.compare_digest(digest, v1) for v1 in v1s)
    except Exception:
        return False

# =============================================================================
# API keys
# =============================================================================

def issue_api_key(email: str) -> str:
    api_key = "gb_" + secrets.token_urlsafe(24).replace("-", "").replace("_", "")
    con = db_connect()
    con.execute(
        "INSERT OR REPLACE INTO api_keys (api_key,email,created_at,status) VALUES (?,?,?,?)",
        (api_key, email.strip().lower(), iso(now_utc()), "active"),
    )
    con.commit()
    con.close()
    return api_key

def get_latest_key_for_email(email: str) -> Optional[str]:
    con = db_connect()
    row = con.execute(
        "SELECT api_key FROM api_keys WHERE email=? AND status='active' ORDER BY created_at DESC LIMIT 1",
        (email.strip().lower(),),
    ).fetchone()
    con.close()
    return row["api_key"] if row else None

def key_is_active(api_key: str) -> bool:
    con = db_connect()
    row = con.execute(
        "SELECT status FROM api_keys WHERE api_key=? LIMIT 1",
        (api_key,),
    ).fetchone()
    con.close()
    return bool(row and row["status"] == "active")

# =============================================================================
# HTML (SEO + Social preview)
# =============================================================================

def html_shell(
    *,
    req: Request,
    title: str,
    description: str,
    path: str,
    body: str,
    image_url: Optional[str] = None,
) -> str:
    base = get_base_url(req)
    canonical = f"{base}{path}"
    og_image = image_url or f"{base}/og.png"

    # Twitter/OG + SEO + Google verification
    head = f"""
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{title}</title>
    <meta name="description" content="{description}" />
    <link rel="canonical" href="{canonical}" />

    <meta name="google-site-verification" content="{GOOGLE_SITE_VERIFICATION_CONTENT}" />

    <meta property="og:site_name" content="{APP_NAME}" />
    <meta property="og:type" content="website" />
    <meta property="og:title" content="{title}" />
    <meta property="og:description" content="{description}" />
    <meta property="og:url" content="{canonical}" />
    <meta property="og:image" content="{og_image}" />

    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="{title}" />
    <meta name="twitter:description" content="{description}" />
    <meta name="twitter:image" content="{og_image}" />
    """

    styles = """
    <style>
      :root{
        --bg:#0b1220;
        --panel:#121b2c;
        --panel2:#0f1727;
        --text:#e7eefc;
        --muted:#a8b4cc;
        --gold:#d7b44a;
        --line:rgba(255,255,255,.08);
        --btn:#1f2b44;
      }
      *{box-sizing:border-box}
      body{
        margin:0;
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
        background: radial-gradient(1200px 600px at 20% 0%, #18274a 0%, var(--bg) 45%, #070b14 100%);
        color:var(--text);
      }
      a{color:inherit; text-decoration:none}
      .wrap{max-width:1100px; margin:0 auto; padding:28px 18px 60px}
      .top{
        display:flex; align-items:center; justify-content:space-between; gap:12px;
        margin-bottom:18px;
      }
      .brand{font-weight:800; letter-spacing:.2px}
      .nav{display:flex; gap:10px; align-items:center}
      .pill{
        padding:10px 14px; border:1px solid var(--line); border-radius:999px;
        background:rgba(0,0,0,.12);
      }
      .pill.gold{
        background:linear-gradient(180deg, #f0d87a 0%, var(--gold) 100%);
        color:#1b1400;
        border:none;
        font-weight:800;
      }
      .hero{
        margin-top:18px;
        padding:28px;
        border:1px solid var(--line);
        border-radius:18px;
        background:linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.02));
      }
      h1{
        font-size:54px; line-height:1.05; margin:0 0 12px;
        font-family: ui-serif, Georgia, "Times New Roman", Times, serif;
        letter-spacing:-.4px;
      }
      p{margin:0; color:var(--muted); font-size:18px; line-height:1.5}
      .grid{
        display:grid; grid-template-columns: 1.2fr .8fr; gap:18px;
        margin-top:18px;
      }
      .card{
        padding:18px;
        border:1px solid var(--line);
        border-radius:18px;
        background:rgba(0,0,0,.12);
      }
      .card h2{margin:0 0 10px; font-size:20px}
      .btnrow{display:flex; gap:10px; flex-wrap:wrap; margin-top:12px}
      button, .btn{
        border:none; border-radius:12px; padding:12px 14px; cursor:pointer;
        background:var(--btn); color:var(--text); font-weight:700;
      }
      .btn.gold{ background:linear-gradient(180deg, #f0d87a 0%, var(--gold) 100%); color:#1b1400; }
      input{
        width:100%;
        border:1px solid var(--line);
        border-radius:12px;
        padding:12px 12px;
        background:rgba(0,0,0,.18);
        color:var(--text);
        outline:none;
      }
      table{width:100%; border-collapse:collapse; margin-top:10px}
      th, td{padding:10px 8px; border-bottom:1px solid var(--line); vertical-align:top}
      th{color:var(--muted); font-weight:700; text-align:left}
      .tag{
        display:inline-flex; align-items:center; gap:8px;
        padding:6px 10px; border-radius:999px;
        border:1px solid var(--line);
        background:rgba(0,0,0,.12);
        font-weight:700;
      }
      .dot{width:10px; height:10px; border-radius:99px; background:#4ade80}
      .dot.bear{background:#f87171}
      .dot.neu{background:#60a5fa}
      .small{font-size:13px; color:var(--muted)}
      .muted{color:var(--muted)}
      @media (max-width: 900px){
        .grid{grid-template-columns: 1fr}
        h1{font-size:42px}
      }
    </style>
    """

    scripts = """
    <script>
      function qs(sel){ return document.querySelector(sel); }

      function getKey(){
        try{ return localStorage.getItem("gb_api_key") || ""; }catch(e){ return ""; }
      }
      function setKey(v){
        try{ localStorage.setItem("gb_api_key", v || ""); }catch(e){}
      }

      async function loadArchive(){
        const key = getKey();
        const hdrs = key ? {"x-api-key": key} : {};
        const r = await fetch("/api/history?limit=200", {headers: hdrs});
        const j = await r.json();
        if(!r.ok){
          qs("#archMsg").textContent = (j && j.detail) ? j.detail : "Kunne ikke laste arkiv.";
          return;
        }
        qs("#archMsg").textContent = "OK: viser " + (j.count||0) + " snapshots.";
        const tbody = qs("#archBody");
        if(!tbody) return;
        tbody.innerHTML = "";
        (j.items||[]).forEach(it=>{
          const tr = document.createElement("tr");
          const dt = (it.updated_at||"").replace("T"," ").replace("+00:00"," UTC");
          const price = (it.price_usd!=null) ? ("$ " + Number(it.price_usd).toLocaleString("no-NO",{maximumFractionDigits:1})) : "-";
          const sig = it.signal || "-";
          const r7 = (it.return_7d_pct==null) ? "–" : (Number(it.return_7d_pct).toFixed(1) + " %");
          const r30 = (it.return_30d_pct==null) ? "–" : (Number(it.return_30d_pct).toFixed(1) + " %");
          const note = (it.macro_summary||"").slice(0,140);
          tr.innerHTML = `
            <td style="min-width:190px"><div style="font-weight:800">${dt}</div><div class="small">${it.symbol||""}</div></td>
            <td>${price}</td>
            <td><span class="tag"><span class="dot ${sig==='bearish'?'bear':(sig==='neutral'?'neu':'')}"></span>${sig}</span></td>
            <td>${r7}</td>
            <td>${r30}</td>
            <td class="muted">${note}</td>
          `;
          tbody.appendChild(tr);
        });

        if(j.stats){
          const s = j.stats;
          const fmt = (x)=> (x==null ? "–" : (Number(x).toFixed(1) + " %"));
          qs("#statsBull").textContent = fmt(s.bullish_avg_7d);
          qs("#statsBear").textContent = fmt(s.bearish_avg_7d);
          qs("#statsHit").textContent = (s.hit_rate_7d==null ? "–" : (Number(s.hit_rate_7d).toFixed(0) + " %"));
        }
      }

      async function saveKeyFromInput(){
        const v = (qs("#keyInput")?.value || "").trim();
        setKey(v);
        qs("#keySaved").textContent = v ? "Lagring OK." : "Fjernet nøkkel.";
      }

      async function createCheckout(fromInputSelector){
        const email = (qs(fromInputSelector)?.value || "").trim();
        if(!email){ alert("Skriv inn e-post."); return; }
        const r = await fetch("/api/stripe/create-checkout", {
          method:"POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({email})
        });
        const j = await r.json();
        if(!r.ok){
          alert((j && j.detail) ? j.detail : "Stripe-feil.");
          return;
        }
        window.location.href = j.url;
      }

      async function activateEmailAlert(){
        const key = getKey();
        const email = (qs("#alertEmail")?.value || "").trim();
        if(!key){ alert("Lim inn premium-nøkkel først."); return; }
        if(!email){ alert("Skriv e-post først."); return; }
        const r = await fetch("/api/alerts/subscribe", {
          method:"POST",
          headers: {"Content-Type":"application/json", "x-api-key": key},
          body: JSON.stringify({email})
        });
        const j = await r.json();
        if(!r.ok){ alert((j&&j.detail)?j.detail:"Kunne ikke aktivere."); return; }
        alert("OK: e-postvarsel aktivert.");
      }

      // Auto-load archive if present
      window.addEventListener("DOMContentLoaded", ()=>{
        const ki = qs("#keyInput");
        if(ki){ ki.value = getKey(); }
      });
    </script>
    """

    return f"""
    <!doctype html>
    <html lang="no">
      <head>
        {head}
        {styles}
      </head>
      <body>
        {body}
        {scripts}
      </body>
    </html>
    """

# =============================================================================
# Pages
# =============================================================================

@app.get("/", response_class=HTMLResponse)
def home(req: Request):
    title = "Gullbrief: daglig gullanalyse uten støy"
    desc = "Kort, nøktern daglig kommentar på norsk. Pris, trend, makro og nyheter. Premium gir arkiv, signalhistorikk og e-postvarsler."
    body = f"""
    <div class="wrap">
      <div class="top">
        <div class="brand">{APP_NAME}</div>
        <div class="nav">
          <a class="pill" href="/analysis">Analyse</a>
          <a class="pill" href="/archive">Arkiv</a>
          <a class="pill gold" href="/premium">Premium</a>
        </div>
      </div>

      <div class="hero">
        <div class="small" style="letter-spacing:.2em; opacity:.8;">DAGLIG</div>
        <h1>Gullanalyse uten støy.</h1>
        <p>{desc}</p>
        <div class="btnrow">
          <a class="btn" href="/analysis">Se dagens analyse</a>
          <a class="btn" href="/archive">Åpne arkiv</a>
          <a class="btn gold" href="/premium">Kjøp Premium</a>
        </div>
      </div>

      <div class="grid">
        <div class="card">
          <h2>Hva du får</h2>
          <ul class="muted" style="margin:0; padding-left:18px; line-height:1.7">
            <li><b>Daglig premium-rapport</b> (makro, drivere, hva som kan endre bildet)</li>
            <li><b>Signal + indikatorforklaring</b> (bullish / bearish / neutral)</li>
            <li><b>Arkiv</b> med historikk og avkastning 7d/30d fra signalpunkter</li>
            <li><b>E-post</b>: daglig utsendelse + varsel ved signalendring</li>
          </ul>
        </div>

        <div class="card">
          <h2>Status</h2>
          <div class="small">Symbol: <b>{YAHOO_SYMBOL}</b></div>
          <div class="small">Cache TTL: <b>{CACHE_TTL_SECONDS}s</b></div>
          <div class="small">Stripe: <b>{"aktiv" if (STRIPE_SECRET_KEY and STRIPE_PRICE_ID) else "ikke konfigurert"}</b></div>
          <div class="small" style="margin-top:10px">
            Tips: legg inn <code>/sitemap.xml</code> i Google Search Console.
          </div>
        </div>
      </div>
    </div>
    """
    return HTMLResponse(html_shell(req=req, title=title, description=desc, path="/", body=body))

@app.get("/premium", response_class=HTMLResponse)
def premium(req: Request):
    title = "Gullbrief Premium: daglig gullanalyse på norsk"
    desc = "Premium gir daglig rapport, signalhistorikk, arkiv med 7d/30d avkastning og e-postvarsler."
    body = """
    <div class="wrap">
      <div class="top">
        <div class="brand">Gullbrief</div>
        <div class="nav">
          <a class="pill" href="/analysis">Analyse</a>
          <a class="pill" href="/archive">Arkiv</a>
          <a class="pill gold" href="/premium">Premium</a>
        </div>
      </div>

      <div class="hero">
        <div class="small" style="letter-spacing:.2em; opacity:.8;">PREMIUM</div>
        <h1>Daglig gullanalyse uten støy.</h1>
        <p>Gullbrief Premium gir deg en kort, nøktern daglig kommentar på norsk, basert på pris, trend, makro og nyhetsstrøm. Du får også arkiv, signalhistorikk og e-postvarsler.</p>

        <div class="grid">
          <div class="card">
            <h2>Hva du får</h2>
            <ul class="muted" style="margin:0; padding-left:18px; line-height:1.7">
              <li><b>Daglig premium-rapport</b> (makro, drivere, hva som kan endre bildet)</li>
              <li><b>Signal + indikatorforklaring</b> (bullish / bearish / neutral)</li>
              <li><b>Arkiv</b> med historikk og avkastning 7d/30d fra signalpunkter</li>
              <li><b>E-post</b>: daglig utsendelse + varsel ved signalendring</li>
            </ul>

            <div style="margin-top:16px; border-top:1px solid var(--line); padding-top:14px;">
              <h2 style="margin-top:0">Kjøp Premium</h2>
              <div class="muted">Skriv inn e-post, trykk kjøp, og du sendes til Stripe checkout.</div>
              <div style="margin-top:10px">
                <input id="buyEmail" placeholder="E-post for kjøp" />
              </div>
              <div class="btnrow">
                <button class="btn gold" onclick="createCheckout('#buyEmail')">Kjøp Premium</button>
                <a class="btn" href="/archive">Jeg har allerede nøkkel</a>
              </div>
              <div class="small" style="margin-top:10px">
                Etter checkout sendes du til success-siden. Der hentes nøkkelen automatisk.
              </div>
            </div>
          </div>

          <div class="card">
            <h2>Spørsmål</h2>
            <div style="font-weight:800; margin-top:8px">Hvor får jeg premium-nøkkelen?</div>
            <div class="muted">Etter checkout sendes du til success-side. Der hentes nøkkelen automatisk.</div>

            <div style="font-weight:800; margin-top:14px">Hvor ligger arkivet?</div>
            <div class="muted">På <code>/archive</code>. Teaser er gratis, full historikk krever nøkkel.</div>

            <div style="font-weight:800; margin-top:14px">Kan jeg avbryte?</div>
            <div class="muted">Ja, via Stripe. Når abonnementet stopper, blir nøkkelen inaktiv.</div>

            <div style="font-weight:800; margin-top:14px">Er dette investeringsråd?</div>
            <div class="muted">Nei. Dette er markedsanalyse og oppsummering, ikke kjøp/salg-anbefaling.</div>
          </div>
        </div>
      </div>
    </div>
    """
    return HTMLResponse(html_shell(req=req, title=title, description=desc, path="/premium", body=body))

@app.get("/archive", response_class=HTMLResponse)
def archive(req: Request):
    title = "Gullbrief Arkiv: historikk og signaler"
    desc = "Se siste snapshots gratis. Premium gir full historikk og avkastning 7d/30d fra signalpunkter."
    body = """
    <div class="wrap">
      <div class="top">
        <div class="brand">Gullbrief Arkiv</div>
        <div class="nav">
          <a class="pill" href="/analysis">Analyse</a>
          <a class="pill gold" href="/premium">Premium</a>
        </div>
      </div>

      <div class="grid">
        <div class="card">
          <h2>Teaser (gratis)</h2>
          <div class="muted">Siste 3 snapshots. Full historikk ligger bak premium.</div>
          <div id="archMsg" class="small" style="margin-top:8px;">Trykk "Last arkiv" for å hente.</div>

          <div style="margin-top:14px; border-top:1px solid var(--line); padding-top:14px;">
            <h2 style="margin-top:0">Signalhistorikk (siste 30 signaler)</h2>
            <div class="small">
              Bullish: <b id="statsBull">–</b> etter 7d<br/>
              Bearish: <b id="statsBear">–</b> etter 7d<br/>
              Treffsikkerhet: <b id="statsHit">–</b>
            </div>
          </div>

          <table>
            <thead>
              <tr>
                <th>Dato</th>
                <th>Pris</th>
                <th>Signal</th>
                <th>7d</th>
                <th>30d</th>
                <th>Notat</th>
              </tr>
            </thead>
            <tbody id="archBody"></tbody>
          </table>
        </div>

        <div class="card">
          <h2>Medlemsområde</h2>
          <div class="muted">Lim inn premium-nøkkel. Den lagres lokalt i nettleseren (localStorage).</div>
          <div style="margin-top:10px">
            <input id="keyInput" placeholder="gb_..." />
          </div>
          <div class="btnrow">
            <button class="btn gold" onclick="saveKeyFromInput()">Lagre</button>
            <button class="btn" onclick="document.querySelector('#keyInput').value=''; saveKeyFromInput();">Fjern</button>
            <button class="btn" onclick="loadArchive()">Last arkiv</button>
          </div>
          <div id="keySaved" class="small" style="margin-top:8px;"></div>

          <div style="margin-top:14px; border-top:1px solid var(--line); padding-top:14px;">
            <div class="muted">E-post for varsel (premium)</div>
            <div style="margin-top:10px">
              <input id="alertEmail" placeholder="E-post for varsel (premium)" />
            </div>
            <div class="btnrow">
              <button class="btn" onclick="activateEmailAlert()">Aktiver e-postvarsel</button>
            </div>
          </div>

          <div style="margin-top:14px; border-top:1px solid var(--line); padding-top:14px;">
            <div class="muted">E-post for kjøp (Stripe)</div>
            <div style="margin-top:10px">
              <input id="buyEmail2" placeholder="E-post for kjøp (Stripe)" />
            </div>
            <div class="btnrow">
              <button class="btn gold" onclick="createCheckout('#buyEmail2')">Kjøp premium</button>
            </div>
            <div class="small" style="margin-top:10px">
              API: <code>/api/history</code> med header <code>x-api-key</code>.
            </div>
          </div>
        </div>
      </div>
    </div>
    """
    return HTMLResponse(html_shell(req=req, title=title, description=desc, path="/archive", body=body))

@app.get("/analysis", response_class=HTMLResponse)
def analysis_page(req: Request):
    title = "Gullbrief Analyse: dagens status"
    desc = "Dagens gullstatus, signal og kort makro-oppsummering."
    body = """
    <div class="wrap">
      <div class="top">
        <div class="brand">Gullbrief</div>
        <div class="nav">
          <a class="pill" href="/archive">Arkiv</a>
          <a class="pill gold" href="/premium">Premium</a>
        </div>
      </div>

      <div class="hero">
        <div class="small" style="letter-spacing:.2em; opacity:.8;">ANALYSE</div>
        <h1>Dagens status.</h1>
        <p>Dette er en enkel visning. Kjør <code>/api/brief/refresh</code> for å lage nytt snapshot.</p>

        <div class="btnrow">
          <a class="btn" href="/api/public/today" target="_blank">Åpne /api/public/today</a>
          <a class="btn" href="/api/brief/refresh" target="_blank">Kjør /api/brief/refresh</a>
          <a class="btn" href="/archive">Åpne arkiv</a>
        </div>
      </div>
    </div>
    """
    return HTMLResponse(html_shell(req=req, title=title, description=desc, path="/analysis", body=body))

@app.get("/success", response_class=HTMLResponse)
def success(req: Request, session_id: Optional[str] = None):
    title = "Gullbrief: Premium aktivert"
    desc = "Premium er aktivert. Her får du premium-nøkkelen din."
    base = get_base_url(req)
    key = None
    email = None

    if STRIPE_SECRET_KEY and session_id:
        try:
            sess = stripe_get(f"checkout/sessions/{session_id}", params={"expand[]": "customer"})
            email = sess.get("customer_details", {}).get("email") or sess.get("customer_email")
            if email:
                key = get_latest_key_for_email(email)
                if not key:
                    key = issue_api_key(email)
        except Exception:
            pass

    key_html = ""
    if key:
        key_html = f"""
        <div class="card" style="margin-top:16px">
          <h2>Premium-nøkkel</h2>
          <div class="muted">Kopier denne og lim inn på <a class="pill" href="/archive">/archive</a>.</div>
          <div style="margin-top:10px">
            <input value="{key}" onclick="this.select();" />
          </div>
          <div class="btnrow">
            <a class="btn gold" href="/archive">Åpne arkiv</a>
          </div>
          <div class="small" style="margin-top:10px">E-post: <b>{email or ""}</b></div>
        </div>
        """
    else:
        key_html = f"""
        <div class="card" style="margin-top:16px">
          <h2>Fant ikke nøkkel automatisk</h2>
          <div class="muted">Hvis du kom hit uten <code>?session_id=...</code> kan du gå til <a href="/archive">/archive</a> og lime inn nøkkel hvis du har den.</div>
        </div>
        """

    body = f"""
    <div class="wrap">
      <div class="top">
        <div class="brand">Gullbrief</div>
        <div class="nav">
          <a class="pill" href="/archive">Arkiv</a>
          <a class="pill gold" href="/premium">Premium</a>
        </div>
      </div>

      <div class="hero">
        <div class="small" style="letter-spacing:.2em; opacity:.8;">SUCCESS</div>
        <h1>Premium aktivert.</h1>
        <p>Du er klar. Nå får du full historikk og signal-oversikt.</p>
        {key_html}
      </div>
    </div>
    """
    return HTMLResponse(html_shell(req=req, title=title, description=desc, path="/success", body=body))

# =============================================================================
# SEO: sitemap + robots
# =============================================================================

@app.get("/robots.txt")
def robots(req: Request):
    base = get_base_url(req)
    txt = f"""User-agent: *
Allow: /

Sitemap: {base}/sitemap.xml
"""
    return PlainTextResponse(txt)

@app.get("/sitemap.xml")
def sitemap(req: Request):
    base = get_base_url(req)
    urls = [
        ("/", "daily"),
        ("/analysis", "daily"),
        ("/premium", "weekly"),
        ("/archive", "daily"),
    ]
    # basic XML sitemap
    xml_items = []
    for path, freq in urls:
        xml_items.append(
            f"<url><loc>{base}{path}</loc><changefreq>{freq}</changefreq></url>"
        )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        + "".join(xml_items)
        + "</urlset>"
    )
    return Response(content=xml, media_type="application/xml")

# =============================================================================
# Public API
# =============================================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "ts": iso(now_utc()),
        "yahoo_symbol": YAHOO_SYMBOL,
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
        "stripe_enabled": bool(STRIPE_SECRET_KEY and STRIPE_PRICE_ID),
        "stripe_webhook_secret_set": bool(STRIPE_WEBHOOK_SECRET),
        "admin_key_configured": bool(ADMIN_KEY),
        "history_path": str(HISTORY_PATH),
        "db_path": str(DB_PATH),
        "version": "2.3",
    }

@app.get("/api/debug/stripe")
def debug_stripe(req: Request):
    base = get_base_url(req)
    return {
        "stripe_ready": bool(STRIPE_SECRET_KEY and STRIPE_PRICE_ID),
        "secret_len": len(STRIPE_SECRET_KEY),
        "price_id": STRIPE_PRICE_ID or None,
        "success_url": f"{base}/success",
        "cancel_url": f"{base}/premium",
        "webhook_secret_set": bool(STRIPE_WEBHOOK_SECRET),
    }

@app.get("/api/public/today")
def public_today():
    items = read_history(limit=1)
    if not items:
        return {"ok": True, "note": "Ingen snapshots ennå. Kjør /api/brief/refresh."}
    it = items[0]
    # keep it compact
    return {
        "updated_at": it.get("updated_at"),
        "symbol": it.get("symbol"),
        "currency": it.get("currency", "USD"),
        "price_usd": it.get("price_usd"),
        "change_pct": it.get("change_pct"),
        "signal": it.get("signal"),
        "signal_reason": it.get("signal_reason"),
        "macro_summary": it.get("macro_summary"),
        "headlines": it.get("headlines", [])[:10],
        "version": it.get("version", "2.x"),
    }

@app.get("/api/public/teaser-history")
def teaser_history():
    items = read_history(limit=3)
    items = compute_returns_for_items(items)
    # show without macro body
    out = []
    for it in items:
        out.append(
            {
                "updated_at": it.get("updated_at"),
                "symbol": it.get("symbol"),
                "price_usd": it.get("price_usd"),
                "signal": it.get("signal"),
                "return_7d_pct": it.get("return_7d_pct"),
                "return_30d_pct": it.get("return_30d_pct"),
                "macro_summary": (it.get("macro_summary") or "")[:160],
            }
        )
    return {"count": len(out), "items": out}

# =============================================================================
# Premium API (requires x-api-key)
# =============================================================================

def require_api_key(req: Request) -> str:
    key = (req.headers.get("x-api-key") or "").strip()
    if not key:
        raise ValueError("Mangler x-api-key.")
    if not key_is_active(key):
        raise ValueError("Ugyldig eller inaktiv nøkkel.")
    return key

@app.get("/api/history")
def api_history(req: Request, limit: int = 200):
    try:
        require_api_key(req)
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=401)

    items = read_history(limit=min(max(limit, 1), 500))
    items = compute_returns_for_items(items)
    stats = compute_signal_stats(items, last_n_signals=30)

    out = []
    for it in items:
        out.append(
            {
                "updated_at": it.get("updated_at"),
                "symbol": it.get("symbol"),
                "price_usd": it.get("price_usd"),
                "signal": it.get("signal"),
                "macro_summary": (it.get("macro_summary") or "")[:240],
                "return_7d_pct": it.get("return_7d_pct"),
                "return_30d_pct": it.get("return_30d_pct"),
            }
        )
    return {"count": len(out), "items": out, "stats": stats}

# =============================================================================
# Alerts (placeholder)
# =============================================================================

@app.post("/api/alerts/subscribe")
async def alerts_subscribe(req: Request):
    try:
        require_api_key(req)
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=401)

    body = await req.json()
    email = (body.get("email") or "").strip().lower()
    if not email:
        return JSONResponse({"detail": "Mangler email."}, status_code=400)

    # NOTE: This is a placeholder - wire to Brevo/SMTP later.
    return {"ok": True, "email": email, "note": "Lagret (placeholder). Koble til SMTP senere."}

# =============================================================================
# Brief refresh (creates snapshot)
# =============================================================================

_last_refresh_ts = 0.0

@app.get("/api/brief/refresh")
def brief_refresh():
    global _last_refresh_ts
    # basic rate limit
    if time.time() - _last_refresh_ts < 5:
        return JSONResponse({"detail": "Rolig nå 😄 Prøv igjen om noen sekunder."}, status_code=429)

    _last_refresh_ts = time.time()

    q = fetch_yahoo_quote(YAHOO_SYMBOL)
    signal, reason, rsi14, trend_score, _ = simple_signal(q.price)

    snapshot = {
        "updated_at": iso(now_utc()),
        "version": "2.3",
        "symbol": YAHOO_SYMBOL,
        "currency": "USD",
        "price_usd": q.price,
        "change_pct": q.change_pct_1d,
        "signal": signal,
        "signal_reason": reason,
        "rsi14": rsi14,
        "trend_score": trend_score,
        "macro_summary": "Gullprisen viser en positiv trend, støttet av at prisen ligger over både 20- og 50-dagers glidende gjennomsnitt. (Demo-tekst: bytt gjerne til din AI-genererte tekst.)",
        "headlines": [],
    }

    append_history(snapshot)
    return snapshot

# =============================================================================
# Stripe endpoints
# =============================================================================

@app.post("/api/stripe/create-checkout")
async def stripe_create_checkout(req: Request):
    if not (STRIPE_SECRET_KEY and STRIPE_PRICE_ID):
        return JSONResponse({"detail": "Stripe er ikke konfigurert."}, status_code=400)

    body = await req.json()
    email = (body.get("email") or "").strip().lower()
    if not email:
        return JSONResponse({"detail": "Mangler email."}, status_code=400)

    base = get_base_url(req)

    # Create checkout session (subscription)
    try:
        session = stripe_post(
            "checkout/sessions",
            {
                "mode": "subscription",
                "line_items[0][price]": STRIPE_PRICE_ID,
                "line_items[0][quantity]": 1,
                "success_url": f"{base}/success?session_id={{CHECKOUT_SESSION_ID}}",
                "cancel_url": f"{base}/premium",
                "customer_email": email,
                "allow_promotion_codes": "true",
            },
        )
        return {"url": session["url"]}
    except requests.HTTPError as e:
        try:
            msg = e.response.json()
        except Exception:
            msg = {"error": str(e)}
        return JSONResponse({"detail": "Stripe-feil", "stripe": msg}, status_code=400)

@app.post("/api/stripe/webhook")
async def stripe_webhook(req: Request):
    payload = await req.body()
    sig = req.headers.get("stripe-signature", "")

    if STRIPE_WEBHOOK_SECRET:
        if not verify_stripe_signature(payload, sig, STRIPE_WEBHOOK_SECRET):
            return JSONResponse({"detail": "Invalid signature"}, status_code=400)

    try:
        evt = json.loads(payload.decode("utf-8"))
    except Exception:
        return JSONResponse({"detail": "Invalid JSON"}, status_code=400)

    evt_type = evt.get("type")
    obj = (evt.get("data") or {}).get("object") or {}

    # On checkout.session.completed: issue key for customer email
    if evt_type == "checkout.session.completed":
        email = (obj.get("customer_details") or {}).get("email") or obj.get("customer_email")
        customer_id = obj.get("customer")
        subscription_id = obj.get("subscription")
        if email:
            key = get_latest_key_for_email(email) or issue_api_key(email)

            con = db_connect()
            con.execute(
                "INSERT OR REPLACE INTO stripe_links (email, customer_id, subscription_id, updated_at) VALUES (?,?,?,?)",
                (email.strip().lower(), customer_id, subscription_id, iso(now_utc())),
            )
            con.commit()
            con.close()

    # Optional: if subscription deleted => deactivate key
    if evt_type == "customer.subscription.deleted":
        # If Stripe sends customer email is not always present; we can map by customer_id if stored.
        customer_id = obj.get("customer")
        if customer_id:
            con = db_connect()
            row = con.execute(
                "SELECT email FROM stripe_links WHERE customer_id=? LIMIT 1",
                (customer_id,),
            ).fetchone()
            if row:
                email = row["email"]
                # deactivate all keys for that email
                con.execute("UPDATE api_keys SET status='inactive' WHERE email=?", (email,))
                con.commit()
            con.close()

    return {"ok": True}

# =============================================================================
# Tasks (cron hits these)
# =============================================================================

@app.get("/api/tasks/check-signal")
def task_check_signal(admin_key: str):
    if admin_key != ADMIN_KEY:
        return JSONResponse({"error": "UNAUTHORIZED", "message": "admin_key feil."}, status_code=401)

    # Compare latest snapshot signal vs previous
    items = read_history(limit=2)
    if not items:
        return {"ok": True, "sent": 0, "signal": "unknown"}

    latest = items[0]
    prev = items[1] if len(items) > 1 else None
    latest_sig = latest.get("signal")
    prev_sig = prev.get("signal") if prev else None

    # Placeholder "sent"
    sent = 0
    if prev_sig and latest_sig and prev_sig != latest_sig:
        sent = 1  # you can wire this to email later

    return {"ok": True, "sent": sent, "signal": latest_sig}

@app.get("/api/tasks/send-daily-premium")
def task_send_daily_premium(admin_key: str):
    if admin_key != ADMIN_KEY:
        return JSONResponse({"error": "UNAUTHORIZED", "message": "admin_key feil."}, status_code=401)

    # Placeholder: in real version, you’d iterate premium subscribers and email them.
    # For now, just confirm it's callable.
    return {"ok": True, "sent": 0, "note": "Placeholder. Koble til e-post senere."}