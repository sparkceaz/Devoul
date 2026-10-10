#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Devoul Bot - odds-dropping scanner (Pinnacle + SportyBet)
=========================================================

Single-user Telegram bot that scans prematch football odds in a rolling
24-hour window across two bookmakers and reports dropping-odds signals.

  * Pinnacle  -> oddspapi.io  (unchanged behaviour)
  * SportyBet -> its own public factsCenter API (derived from the HAR capture)

All configuration lives in a single JSON file (devoul_variable.json) - there
are NO environment variables required.  The database is Supabase (Postgres)
accessed over PostgREST with the service-role key.

Deploy target: Railway (see DEPLOY-RAILWAY.md).
"""

import asyncio
import json
import logging
import os
import re
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlencode

import aiohttp
import pytz
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    BotCommandScopeChat,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from google import genai
from openai import OpenAI

# ---------------------------------------------------------------------------
# CONFIG  (single JSON file - no env vars)
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(_HERE, "devoul_variable.json")

with open(CONFIG_PATH, "r", encoding="utf-8") as _f:
    CFG = json.load(_f)


def _cfg(key, default=None):
    return CFG.get(key, default)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("devoul")

# --- Telegram ---------------------------------------------------------------
TOKEN = _cfg("TELEGRAM_BOT_TOKEN") or _cfg("TOKEN")
ADMIN_ID = int(_cfg("ADMIN_ID"))
BOT_USERNAME = _cfg("BOT_USERNAME", "devoulbot")
ACCESS_DENIED_URL = _cfg("ACCESS_DENIED_URL", "https://t.me/sparkcea")
ACCESS_DENIED_CONTACT = _cfg("ACCESS_DENIED_CONTACT", "sparkcea")

# --- Supabase ---------------------------------------------------------------
SUPABASE_URL = _cfg("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = _cfg("SUPABASE_SERVICE_ROLE_KEY")

# --- Pinnacle (oddspapi) ----------------------------------------------------
ODDSPAPI_KEY = _cfg("ODDSPAPI_KEY")
ODDSPAPI_BASE = _cfg("ODDSPAPI_BASE", "https://api.oddspapi.io")

# --- SportyBet --------------------------------------------------------------
SB_BASE = _cfg("SPORTYBET_BASE", "https://www.sportybet.com/api/ng/factsCenter")
SB_PRODUCT_ID = _cfg("SPORTYBET_PRODUCT_ID", 3)
SB_SPORT_ID = _cfg("SPORTYBET_SPORT_ID", "sr:sport:1")
SB_PAGE_SIZE = int(_cfg("SPORTYBET_PAGE_SIZE", 100))
SB_MAX_PAGES = int(_cfg("SPORTYBET_MAX_PAGES", 60))
SB_TIMELINE_HOURS = int(_cfg("SPORTYBET_TIMELINE_HOURS", 24))
SB_OUTCOMES_BATCH = int(_cfg("SPORTYBET_OUTCOMES_BATCH", 600))
SB_DELAY = float(_cfg("SPORTYBET_REQUEST_DELAY_SEC", 0.35))
SB_EVENTS_PATH = _cfg("SPORTYBET_EVENTS_PATH", "/wapConfigurableEventsByOrder")
SB_OUTCOMES_PATH = _cfg("SPORTYBET_OUTCOMES_PATH", "/Outcomes")
SB_BANNED_PATH = _cfg("SPORTYBET_BANNED_EVENTS_PATH", "/banned/events")

SB_M_1X2 = str(_cfg("SPORTYBET_MARKET_1X2", "1"))
SB_M_HT_1X2 = str(_cfg("SPORTYBET_MARKET_HT_1X2", "60"))
SB_M_CS = str(_cfg("SPORTYBET_MARKET_CS", "45"))
SB_M_HT_CS = str(_cfg("SPORTYBET_MARKET_HT_CS", "81"))
SB_M_OU25 = str(_cfg("SPORTYBET_MARKET_OU25", "18"))
SB_M_BTTS = str(_cfg("SPORTYBET_MARKET_BTTS", "29"))
SB_SPEC_OU25 = _cfg("SPORTYBET_SPECIFIER_OU25", "total=2.5")

SB_O_HOME = str(_cfg("SPORTYBET_OUTCOME_HOME", "1"))
SB_O_DRAW = str(_cfg("SPORTYBET_OUTCOME_DRAW", "2"))
SB_O_AWAY = str(_cfg("SPORTYBET_OUTCOME_AWAY", "3"))
SB_O_OVER25 = str(_cfg("SPORTYBET_OUTCOME_OVER25", "12"))
SB_O_UNDER25 = str(_cfg("SPORTYBET_OUTCOME_UNDER25", "13"))
SB_O_BTTS_YES = str(_cfg("SPORTYBET_OUTCOME_BTTS_YES", "74"))
SB_O_BTTS_NO = str(_cfg("SPORTYBET_OUTCOME_BTTS_NO", "76"))

SB_CS_OUTCOMES = {str(k): v for k, v in _cfg("SPORTYBET_CS_OUTCOMES", {}).items()}
SB_HT_CS_OUTCOMES = {str(k): v for k, v in _cfg("SPORTYBET_HT_CS_OUTCOMES", {}).items()}
SB_CS_IDS = [str(x) for x in _cfg("SPORTYBET_CS_IDS", list(SB_CS_OUTCOMES.keys()))]
SB_HT_CS_IDS = [str(x) for x in _cfg("SPORTYBET_HT_CS_IDS", list(SB_HT_CS_OUTCOMES.keys()))]

# --- Analysis thresholds ----------------------------------------------------
SCAN_WINDOW_HOURS = int(_cfg("SCAN_WINDOW_HOURS", 24))
MAX_MINUTES_TO_KICKOFF = int(_cfg("MAX_MINUTES_TO_KICKOFF", 1440))
MIN_ODDS = float(_cfg("MIN_ODDS", 1.25))
MAX_INITIAL_ODDS_D = float(_cfg("MAX_INITIAL_ODDS_D", 1.81))
MAX_INITIAL_ODDS_FT_DRAW = float(_cfg("MAX_INITIAL_ODDS_FT_DRAW", 4.0))
MAX_INITIAL_ODDS_HT_DRAW = float(_cfg("MAX_INITIAL_ODDS_HT_DRAW", 2.2))
MIN_DROP_PCT = float(_cfg("MIN_DROP_PCT", 0.5))
D_ANALYSIS_MIN_DROP_PCT = float(_cfg("D_ANALYSIS_MIN_DROP_PCT", 10))
CS_MIN_DROP_PCT = float(_cfg("CS_MIN_DROP_PCT", 12))
FT_DRAW_MIN_DROP_PCT = float(_cfg("FT_DRAW_MIN_DROP_PCT", 12))
HT_DRAW_MIN_DROP_PCT = float(_cfg("HT_DRAW_MIN_DROP_PCT", 3))
SB_FT_DRAW_MIN_DROP_PCT = float(_cfg("SPORTYBET_FT_DRAW_MIN_DROP_PCT", 6))

EXCLUDED_KEYWORDS = [k.lower() for k in _cfg("EXCLUDED_LEAGUE_KEYWORDS", [])]
LAGOS = pytz.timezone(_cfg("TIMEZONE", "Africa/Lagos"))
CACHE_KEEP_ROWS = int(_cfg("CACHE_KEEP_ROWS", 50))
SCAN_SCHEDULE = _cfg("SCAN_SCHEDULE", [])

# Pinnacle market ids (loaded at startup from oddspapi)
MARKET_H2H = None
MARKET_BTTS = None
MARKET_OVER15 = None
MARKET_OVER25 = None
MARKET_HT_H2H = 10208
MARKET_CS = 10336
OUTCOME_MAP = {}
MARKET_HANDICAP = {}
D_MARKETS = []
GIFT_D_MARKETS = []

ALLOWED_CS = {
    "0:0", "1:0", "0:1", "1:1", "0:2", "1:2",
    "2:0", "2:1", "2:2", "3:0", "0:3", "1:3", "3:1", "3:3",
}

# AI ACCA
ai_buttons_public = False
openrouter_api_key = ""
deepseek_client = None
gemini_client = genai.Client(api_key=_cfg("GEMINI_API_KEY", "")) if _cfg("GEMINI_API_KEY") else None


# ===========================================================================
# SUPABASE (PostgREST) DATA LAYER
# ===========================================================================
class Supabase:
    """Minimal async PostgREST client using the service-role key."""

    def __init__(self, url, key):
        self.url = (url or "").rstrip("/")
        self.key = key or ""
        self.rest = f"{self.url}/rest/v1"
        self._session = None

    async def _sess(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._session

    def _headers(self, prefer=None):
        h = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        }
        if prefer:
            h["Prefer"] = prefer
        return h

    async def insert(self, table, rows):
        if not rows:
            return True
        if isinstance(rows, dict):
            rows = [rows]
        try:
            s = await self._sess()
            async with s.post(
                f"{self.rest}/{table}",
                headers=self._headers("return=minimal"),
                json=rows,
            ) as r:
                if r.status >= 300:
                    logger.error("supabase insert %s -> %s %s", table, r.status, await r.text())
                    return False
                return True
        except Exception as e:
            logger.error("supabase insert %s error: %s", table, e)
            return False

    async def select(self, table, params=None):
        try:
            s = await self._sess()
            async with s.get(
                f"{self.rest}/{table}", headers=self._headers(), params=params or {}
            ) as r:
                if r.status >= 300:
                    logger.error("supabase select %s -> %s %s", table, r.status, await r.text())
                    return []
                return await r.json()
        except Exception as e:
            logger.error("supabase select %s error: %s", table, e)
            return []

    async def delete(self, table, params):
        try:
            s = await self._sess()
            async with s.delete(
                f"{self.rest}/{table}",
                headers=self._headers("return=minimal"),
                params=params,
            ) as r:
                if r.status >= 300:
                    logger.error("supabase delete %s -> %s %s", table, r.status, await r.text())
                    return False
                return True
        except Exception as e:
            logger.error("supabase delete %s error: %s", table, e)
            return False

    async def rpc(self, fn, args=None):
        try:
            s = await self._sess()
            async with s.post(
                f"{self.rest}/rpc/{fn}", headers=self._headers(), json=args or {}
            ) as r:
                if r.status >= 300:
                    logger.error("supabase rpc %s -> %s %s", fn, r.status, await r.text())
                    return None
                try:
                    return await r.json()
                except Exception:
                    return True
        except Exception as e:
            logger.error("supabase rpc %s error: %s", fn, e)
            return None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


DB = Supabase(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


async def db_get_setting(key, default=None):
    rows = await DB.select("settings", {"key": f"eq.{key}", "select": "value", "limit": 1})
    if rows:
        return rows[0].get("value")
    return default


async def db_set_setting(key, value):
    await DB.insert("settings", {"key": key, "value": str(value)})


async def db_is_paused():
    return (await db_get_setting("system_paused", "0")) == "1"


# ===========================================================================
# HELPERS
# ===========================================================================
def lagos_time():
    return datetime.now(LAGOS)


def now_utc():
    return datetime.now(pytz.utc)


def mins_to_kickoff(commence):
    """Minutes until kickoff. Accepts ISO string or epoch-ms int."""
    try:
        if isinstance(commence, (int, float)):
            dt = datetime.fromtimestamp(commence / 1000.0, tz=pytz.utc)
        else:
            dt = datetime.fromisoformat(str(commence).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.utc)
        return max(0, int((dt - now_utc()).total_seconds() / 60))
    except Exception:
        return 9999


def iso_from_epoch_ms(ms):
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=pytz.utc).isoformat()
    except Exception:
        return ""


def is_excluded_league(name):
    n = (name or "").lower()
    return any(k in n for k in EXCLUDED_KEYWORDS)


def clean_name(name):
    if not name:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", "", str(name).lower())).strip()


def match_key(tournament_id, home, away):
    return f"{tournament_id}_{home}_{away}"


def source_label(source):
    return {"pinnacle": "P", "sportybet": "B"}.get(source, "?")


def source_emoji(source):
    return {"pinnacle": "\U0001F17F\uFE0F", "sportybet": "\U0001F171\uFE0F"}.get(source, "")


def fmt_kickoff(commence):
    try:
        if isinstance(commence, (int, float)):
            dt = datetime.fromtimestamp(commence / 1000.0, tz=pytz.utc)
        else:
            dt = datetime.fromisoformat(str(commence).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.utc)
        return dt.astimezone(LAGOS).strftime("%d %b \u2022 %H:%M WAT")
    except Exception:
        return "TBC"


# ===========================================================================
# SPORTYBET API CLIENT
# ===========================================================================
def _urllib_request(url, method, data, headers):
    """Blocking HTTP request via urllib (runs in a worker thread).

    urllib's TLS fingerprint passes the SportyBet AWS WAF; aiohttp's does not.
    Returns (status, text).  HTTPError is unwrapped so the caller can retry.
    """
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


class SportyBet:
    """Client for the SportyBet factsCenter API (derived from the HAR)."""

    def __init__(self):
        self._session = None

    async def _sess(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=40)
            )
        return self._session

    def _headers(self):
        return {
            "accept": "*/*",
            "content-type": "application/json;charset=UTF-8",
            "operid": str(_cfg("SPORTYBET_OPERID", "2")),
            "origin": "https://www.sportybet.com",
            "platform": _cfg("SPORTYBET_PLATFORM", "wap"),
            "clientid": _cfg("SPORTYBET_CLIENTID", "wap"),
            "user-agent": _cfg("SPORTYBET_USER_AGENT", "Mozilla/5.0"),
            "referer": _cfg("SPORTYBET_REFERER", "https://www.sportybet.com/ng/m/sport/football"),
        }

    async def _request(self, method, path, body=None, params=None, attempts=4):
        """HTTP with retry/backoff.

        NOTE: SportyBet sits behind AWS WAF, which issues a JS "challenge"
        (HTTP 202, x-amzn-waf-action: challenge) to aiohttp's TLS fingerprint
        while allowing urllib's.  We therefore perform the request with
        urllib inside a worker thread - it is the transport that actually
        passes the WAF.  Retries cover transient 202/429/5xx.
        """
        url = f"{SB_BASE}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"
        data = json.dumps(body).encode() if body is not None else None
        headers = self._headers()
        last = None
        for i in range(attempts):
            try:
                status, text = await asyncio.to_thread(
                    _urllib_request, url, method, data, headers
                )
                if status == 200:
                    return json.loads(text)
                last = status
                if status in (202, 429) or status >= 500:
                    await asyncio.sleep(1.5 * (i + 1))
                    continue
                logger.warning("sportybet %s -> HTTP %s", path, status)
                return None
            except Exception as e:
                last = e
                await asyncio.sleep(1.5 * (i + 1))
        logger.warning("sportybet %s -> giving up after %s attempts (last=%s)", path, attempts, last)
        return None

    async def _post(self, path, body):
        return await self._request("POST", path, body=body)

    async def _get(self, path, params=None):
        return await self._request("GET", path, params=params)

    async def fetch_all_events(self):
        """Paginate the prematch football list for the 24h window."""
        events = []
        seen = set()
        page = 1
        while page <= SB_MAX_PAGES:
            body = {
                "timeline": SB_TIMELINE_HOURS,
                "productId": SB_PRODUCT_ID,
                "sportId": SB_SPORT_ID,
                "order": 0,
                "pageNum": page,
                "pageSize": SB_PAGE_SIZE,
                "withTwoUpMarket": True,
                "withOneUpMarket": True,
            }
            data = await self._post(SB_EVENTS_PATH, body)
            if not data or data.get("bizCode") != 10000:
                break
            d = data.get("data") or {}
            tours = d.get("tournaments") or []
            page_events = [e for t in tours for e in (t.get("events") or [])]
            if not page_events:
                break
            for e in page_events:
                eid = e.get("eventId")
                if eid and eid not in seen:
                    seen.add(eid)
                    events.append(e)
            if not d.get("moreEvents"):
                break
            page += 1
            await asyncio.sleep(SB_DELAY)
        logger.info("SportyBet: fetched %s prematch events (%s pages)", len(events), page)
        return events

    async def fetch_banned_events(self):
        data = await self._get(SB_BANNED_PATH)
        if data and data.get("bizCode") == 10000:
            return set(data.get("data") or [])
        return set()

    async def fetch_outcomes(self, items):
        """items: list of {eventId, marketId, outcomeId, specifier}."""
        out = {}
        for i in range(0, len(items), SB_OUTCOMES_BATCH):
            batch = items[i:i + SB_OUTCOMES_BATCH]
            data = await self._post(SB_OUTCOMES_PATH, batch)
            if data and data.get("bizCode") == 10000:
                for d in data.get("data") or []:
                    eid = d.get("eventId")
                    for m in d.get("markets") or []:
                        for o in m.get("outcomes") or []:
                            out[(eid, str(m.get("id")), str(o.get("id")))] = o
            await asyncio.sleep(SB_DELAY)
        return out

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


SB = SportyBet()


def sb_event_meta(e):
    """Normalise a SportyBet event dict into our internal shape."""
    sport = e.get("sport") or {}
    cat = sport.get("category") or {}
    tour = cat.get("tournament") or {}
    return {
        "event_id": e.get("eventId"),
        "home": e.get("homeTeamName", ""),
        "away": e.get("awayTeamName", ""),
        "tournament_id": tour.get("id", ""),
        "tournament_name": tour.get("name", ""),
        "category_name": cat.get("name", ""),
        "commence_ms": e.get("estimateStartTime"),
        "commence": iso_from_epoch_ms(e.get("estimateStartTime")),
        "status": e.get("status", 0),
        "match_status": e.get("matchStatus", ""),
    }


def sb_extract_list_markets(e, meta):
    """Extract 1X2 / O-U 2.5 / BTTS / HT 1X2 from the events-list payload."""
    cands = []
    for m in e.get("markets") or []:
        mid = str(m.get("id"))
        spec = m.get("specifier")
        outs = {str(o.get("id")): o for o in (m.get("outcomes") or [])}
        if mid == SB_M_1X2:
            for oid, label in ((SB_O_HOME, "Home Win"), (SB_O_DRAW, "Draw"), (SB_O_AWAY, "Away Win")):
                o = outs.get(oid)
                if o:
                    cands.append(_sb_cand(meta, mid, oid, label, o, "1X2"))
        elif mid == SB_M_OU25 and spec == SB_SPEC_OU25:
            for oid, label in ((SB_O_OVER25, "Over 2.5"), (SB_O_UNDER25, "Under 2.5")):
                o = outs.get(oid)
                if o:
                    cands.append(_sb_cand(meta, mid, oid, label, o, "OU25"))
        elif mid == SB_M_BTTS:
            for oid, label in ((SB_O_BTTS_YES, "BTTS Yes"), (SB_O_BTTS_NO, "BTTS No")):
                o = outs.get(oid)
                if o:
                    cands.append(_sb_cand(meta, mid, oid, label, o, "BTTS"))
        elif mid == SB_M_HT_1X2:
            o = outs.get(SB_O_DRAW)
            if o:
                cands.append(_sb_cand(meta, mid, SB_O_DRAW, "HT Draw", o, "HT1X2"))
    return cands


def _sb_cand(meta, market_id, outcome_id, label, o, group):
    try:
        odds = float(o.get("odds") or 0)
    except Exception:
        odds = 0.0
    return {
        "home": meta["home"],
        "away": meta["away"],
        "commence": meta["commence"],
        "commence_ms": meta["commence_ms"],
        "tournament_id": meta["tournament_id"],
        "tournament_name": meta["tournament_name"],
        "market_id": market_id,
        "market_group": group,
        "outcome_id": outcome_id,
        "outcome_name": label,
        "odds": odds,
        "active": int(o.get("isActive", 1) or 0),
        "api_fixture_id": meta["event_id"],
        "source": "sportybet",
    }


# ===========================================================================
# PINNACLE (oddspapi) CLIENT  - unchanged behaviour
# ===========================================================================
async def load_market_ids():
    global MARKET_H2H, MARKET_BTTS, MARKET_OVER15, MARKET_OVER25
    global D_MARKETS, GIFT_D_MARKETS, OUTCOME_MAP, MARKET_HANDICAP
    logger.info("Loading market IDs from OddsPapi...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{ODDSPAPI_BASE}/v4/markets",
                params={"language": "en", "apiKey": ODDSPAPI_KEY},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    logger.error("Market fetch failed: %s", resp.status)
                    _set_fallback_markets()
                    return
                markets = await resp.json()
    except Exception as e:
        logger.error("Market fetch error: %s", e)
        _set_fallback_markets()
        return

    for m in markets:
        mid = m.get("marketId")
        mtype = m.get("marketType", "")
        handicap = m.get("handicap", 0)
        period = m.get("period", "")
        if not mid:
            continue
        if "outcomes" in m:
            for o in m["outcomes"]:
                OUTCOME_MAP[(mid, o["outcomeId"])] = o["outcomeName"]
        if mid == 101 and period in ("", "fulltime"):
            MARKET_H2H = mid
        elif mid == 104:
            MARKET_BTTS = mid
        elif mtype == "totals" and period in ("", "fulltime"):
            MARKET_HANDICAP[mid] = handicap
            if handicap == 1.5:
                MARKET_OVER15 = mid
            elif handicap == 2.5:
                MARKET_OVER25 = mid

    if not MARKET_HT_H2H:
        MARKET_HT_H2H = 10208
    if not MARKET_CS:
        MARKET_CS = 10336
    _set_fallback_markets()
    logger.info(
        "Markets: H2H=%s BTTS=%s O1.5=%s O2.5=%s HT=%s CS=%s",
        MARKET_H2H, MARKET_BTTS, MARKET_OVER15, MARKET_OVER25, MARKET_HT_H2H, MARKET_CS,
    )


def _set_fallback_markets():
    global D_MARKETS, GIFT_D_MARKETS
    global MARKET_H2H, MARKET_BTTS, MARKET_OVER15, MARKET_OVER25
    if not MARKET_H2H:
        MARKET_H2H = 101
    if not MARKET_BTTS:
        MARKET_BTTS = 104
    if not MARKET_OVER15:
        MARKET_OVER15 = 108
    if not MARKET_OVER25:
        MARKET_OVER25 = 1010
    D_MARKETS = [MARKET_H2H, MARKET_BTTS, MARKET_OVER15, MARKET_OVER25]
    GIFT_D_MARKETS = [MARKET_H2H]


def outcome_label(market_id, outcome_id):
    name = OUTCOME_MAP.get((market_id, outcome_id), "")
    if not name:
        return None
    if market_id in (MARKET_H2H, MARKET_HT_H2H):
        if name == "1":
            return "Home Win"
        if name == "X":
            return "Draw"
        if name == "2":
            return "Away Win"
        return None
    if market_id == MARKET_BTTS:
        if name == "Yes":
            return "BTTS Yes"
        if name == "No":
            return "BTTS No"
        return None
    if market_id in (MARKET_OVER15, MARKET_OVER25):
        h = MARKET_HANDICAP.get(market_id, "")
        if name == "Over":
            return f"Over {h}"
        if name == "Under":
            return f"Under {h}"
        return None
    if market_id == MARKET_CS:
        return name
    return None


async def fetch_odds_for_tournaments(tournament_ids, bookmaker, market_ids=None, batch_size=5):
    if not tournament_ids:
        return []
    all_fixtures = []
    for i in range(0, len(tournament_ids), batch_size):
        batch = tournament_ids[i:i + batch_size]
        ids_str = ",".join(str(t) for t in batch)
        url = (
            f"{ODDSPAPI_BASE}/v4/odds-by-tournaments?tournamentIds={ids_str}"
            f"&bookmaker={bookmaker}&language=en&verbosity=3&apiKey={ODDSPAPI_KEY}"
        )
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if isinstance(data, list):
                            all_fixtures.extend(data)
                        else:
                            all_fixtures.append(data)
                    else:
                        logger.warning("Odds fetch %s batch %s: HTTP %s", bookmaker, i // batch_size + 1, resp.status)
        except Exception as e:
            logger.error("fetch_odds_for_tournaments error: %s", e)
        await asyncio.sleep(2.0)
    return all_fixtures


def extract_odds_from_fixture(fixture, bookmaker, target_markets):
    home = fixture.get("participant1Name", "")
    away = fixture.get("participant2Name", "")
    if not home or not away:
        return
    commence = fixture.get("startTime", "")
    tid = fixture.get("tournamentId", 0)
    tname = fixture.get("tournamentName", "")
    fix_id = fixture.get("fixtureId", "")
    bm_data = fixture.get("bookmakerOdds", {}).get(bookmaker)
    if not bm_data:
        return
    markets = bm_data.get("markets", {})
    for mk_id_str, market in markets.items():
        mk_id = int(mk_id_str)
        if mk_id not in target_markets:
            continue
        outcomes = market.get("outcomes", {})
        for out_id_str, out_data in outcomes.items():
            out_id = int(out_id_str)
            player = out_data.get("players", {}).get("0")
            if not player or not player.get("active"):
                continue
            odds = float(player.get("price", 0) or 0)
            if odds < 1.01:
                continue
            label = outcome_label(mk_id, out_id)
            if not label:
                if mk_id in (MARKET_HT_H2H, MARKET_CS):
                    label = outcomes.get(out_id_str, {}).get("outcomeName", "")
                    if not label:
                        continue
                else:
                    continue
            yield {
                "home": home, "away": away, "commence": commence,
                "tournament_id": tid, "tournament_name": tname,
                "market_id": mk_id, "outcome_id": out_id,
                "outcome_name": label, "odds": odds,
                "api_fixture_id": fix_id, "source": "pinnacle",
            }


async def refresh_daily_fixtures():
    logger.info("Refreshing daily fixtures (Pinnacle)...")
    now = now_utc()
    from_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_time = (now + timedelta(hours=SCAN_WINDOW_HOURS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{ODDSPAPI_BASE}/v4/fixtures"
    params = {
        "sportId": 10, "from": from_time, "to": to_time, "hasOdds": "true",
        "bookmakers": "pinnacle", "apiKey": ODDSPAPI_KEY, "language": "en",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    logger.error("Fixture fetch failed: %s", resp.status)
                    return
                data = await resp.json()
    except Exception as e:
        logger.error("Fixture fetch error: %s", e)
        return

    fixtures = data if isinstance(data, list) else data.get("data", [])
    await DB.delete("daily_fixtures", {"fixture_id": "neq.__none__"})
    rows = []
    for f in fixtures:
        fid = f.get("fixtureId")
        if fid and f.get("startTime"):
            rows.append({
                "fixture_id": str(fid),
                "tournament_id": str(f.get("tournamentId", "")),
                "start_time": f["startTime"],
                "home": f.get("participant1Name", ""),
                "away": f.get("participant2Name", ""),
            })
    if rows:
        await DB.insert("daily_fixtures", rows)
    await db_set_setting("daily_fixtures_updated", now.isoformat())
    logger.info("Daily fixtures loaded: %s matches", len(rows))


# ===========================================================================
# SNAPSHOT STORAGE
# ===========================================================================
async def save_odds_snapshots(cands):
    rows = []
    for c in cands:
        rows.append({
            "match_key": match_key(c["tournament_id"], c["home"], c["away"]),
            "tournament_id": str(c["tournament_id"]),
            "home": c["home"], "away": c["away"],
            "market_id": str(c["market_id"]), "outcome_id": str(c["outcome_id"]),
            "outcome_name": c["outcome_name"], "odds": float(c["odds"]),
            "source": c["source"],
        })
    if rows:
        await DB.insert("odds_history", rows)


async def save_gift_snapshots(cands):
    rows = []
    for c in cands:
        rows.append({
            "match_key": match_key(c["tournament_id"], c["home"], c["away"]),
            "tournament_id": str(c["tournament_id"]),
            "home": c["home"], "away": c["away"],
            "market_id": str(c["market_id"]), "outcome_id": str(c["outcome_id"]),
            "outcome_name": c["outcome_name"], "odds": float(c["odds"]),
            "source": c["source"],
        })
    if rows:
        await DB.insert("gift_snapshot", rows)


async def get_odds_snapshots(mkey, market_id, outcome_id, hours_back=SCAN_WINDOW_HOURS, source=None):
    cutoff = (now_utc() - timedelta(hours=hours_back)).isoformat()
    params = {
        "match_key": f"eq.{mkey}",
        "market_id": f"eq.{market_id}",
        "outcome_id": f"eq.{outcome_id}",
        "recorded_at": f"gte.{cutoff}",
        "select": "odds,recorded_at",
        "order": "recorded_at.asc",
    }
    if source:
        params["source"] = f"eq.{source}"
    return await DB.select("odds_history", params)


async def get_gift_snapshots_past(mkey, market_id, outcome_id, hours_back=SCAN_WINDOW_HOURS, source=None):
    cutoff = (now_utc() - timedelta(hours=hours_back)).isoformat()
    params = {
        "match_key": f"eq.{mkey}",
        "market_id": f"eq.{market_id}",
        "outcome_id": f"eq.{outcome_id}",
        "recorded_at": f"gte.{cutoff}",
        "select": "odds,recorded_at",
        "order": "recorded_at.asc",
    }
    if source:
        params["source"] = f"eq.{source}"
    return await DB.select("gift_snapshot", params)


# ===========================================================================
# ANALYSIS
# ===========================================================================
def _passes_common(cand):
    mins = mins_to_kickoff(cand["commence"])
    if mins <= 0 or mins > MAX_MINUTES_TO_KICKOFF:
        return False
    if cand["odds"] < MIN_ODDS:
        return False
    if is_excluded_league(cand.get("tournament_name", "")):
        return False
    return True


async def run_d_analysis(candidates, source):
    """Dropping-odds analysis for one bookmaker (same rules for both)."""
    await save_odds_snapshots(candidates)
    groups = defaultdict(list)
    for c in candidates:
        if not _passes_common(c):
            continue
        key = (match_key(c["tournament_id"], c["home"], c["away"]), str(c["market_id"]))
        groups[key].append(c)

    signals = []
    for (mkey, mk_id), cands in groups.items():
        fav = min(cands, key=lambda x: x["odds"])
        if str(mk_id) in (str(MARKET_H2H), SB_M_1X2) and fav["outcome_name"] == "Draw":
            continue
        history = await get_odds_snapshots(mkey, mk_id, fav["outcome_id"], source=source)
        if len(history) < 2:
            continue
        original = float(history[0]["odds"])
        current = float(fav["odds"])
        if original <= 0 or original > MAX_INITIAL_ODDS_D or current < MIN_ODDS:
            continue
        drop = ((original - current) / original) * 100
        if drop <= 0:
            continue
        signals.append({
            "match": f"{fav['home']} vs {fav['away']}",
            "league": fav["tournament_name"],
            "market": fav["outcome_name"],
            "outcome": fav["outcome_id"],
            "original_odds": round(original, 2),
            "current_odds": round(current, 2),
            "drop_pct": round(drop, 2),
            "confidence": 50,
            "confidence_label": "\u26A1 MEDIUM",
            "minutes_to_kickoff": mins_to_kickoff(fav["commence"]),
            "commence_time": fav["commence"],
            "api_fixture_id": fav.get("api_fixture_id", ""),
            "source": source,
        })

    seen, unique = set(), []
    for s in signals:
        if s["match"] not in seen:
            seen.add(s["match"])
            unique.append(s)
    unique.sort(key=lambda x: x["drop_pct"], reverse=True)
    return unique


async def run_ft_draw_analysis(candidates, source):
    """FT draw (H2H X) dropping odds for one bookmaker."""
    await save_gift_snapshots(candidates)
    draws = []
    for c in candidates:
        if c["outcome_name"] != "Draw":
            continue
        if not _passes_common(c):
            continue
        mkey = match_key(c["tournament_id"], c["home"], c["away"])
        past = await get_gift_snapshots_past(mkey, str(c["market_id"]), str(c["outcome_id"]), source=source)
        if not past:
            continue
        original = float(past[0]["odds"])
        current = float(c["odds"])
        if original <= 0 or original > MAX_INITIAL_ODDS_FT_DRAW:
            continue
        drop = ((original - current) / original) * 100
        if drop < MIN_DROP_PCT or current > MAX_INITIAL_ODDS_FT_DRAW:
            continue
        draws.append({
            "match": f"{c['home']} vs {c['away']}",
            "league": c["tournament_name"],
            "prediction": "Draw",
            "odds": round(current, 2),
            "original_odds": round(original, 2),
            "drop_pct": round(drop, 2),
            "confidence": min(int(drop * 5), 90),
            "commence_time": c["commence"],
            "source": source,
        })
    draws.sort(key=lambda x: x["drop_pct"], reverse=True)
    return draws


async def run_ht_draw_analysis(candidates, source):
    """HT draw (HT 1X2 X) dropping odds for one bookmaker."""
    await save_gift_snapshots(candidates)
    draws = []
    for c in candidates:
        if c["outcome_name"] != "HT Draw":
            continue
        if not _passes_common(c):
            continue
        mkey = match_key(c["tournament_id"], c["home"], c["away"])
        past = await get_gift_snapshots_past(mkey, str(c["market_id"]), str(c["outcome_id"]), source=source)
        if not past:
            continue
        original = float(past[0]["odds"])
        current = float(c["odds"])
        if original <= 0 or original > MAX_INITIAL_ODDS_HT_DRAW:
            continue
        drop = ((original - current) / original) * 100
        if drop < MIN_DROP_PCT or current > MAX_INITIAL_ODDS_FT_DRAW:
            continue
        draws.append({
            "match": f"{c['home']} vs {c['away']}",
            "league": c["tournament_name"],
            "prediction": "HT Draw",
            "odds": round(current, 2),
            "original_odds": round(original, 2),
            "drop_pct": round(drop, 2),
            "confidence": min(int(drop * 5), 90),
            "commence_time": c["commence"],
            "source": source,
        })
    draws.sort(key=lambda x: x["drop_pct"], reverse=True)
    return draws


async def run_cs_analysis(candidates, source, allowed):
    """Correct-score dropping odds for one bookmaker."""
    await save_gift_snapshots(candidates)
    drops = []
    for c in candidates:
        if c["outcome_name"] not in allowed:
            continue
        if not _passes_common(c):
            continue
        mkey = match_key(c["tournament_id"], c["home"], c["away"])
        past = await get_gift_snapshots_past(mkey, str(c["market_id"]), str(c["outcome_id"]), source=source)
        if not past:
            continue
        original = float(past[0]["odds"])
        current = float(c["odds"])
        if original <= 0:
            continue
        drop = ((original - current) / original) * 100
        if drop < MIN_DROP_PCT:
            continue
        drops.append({
            "match": f"{c['home']} vs {c['away']}",
            "league": c["tournament_name"],
            "prediction": c["outcome_name"],
            "odds": round(current, 2),
            "original_odds": round(original, 2),
            "drop_pct": round(drop, 2),
            "confidence": min(int(drop * 5), 90),
            "commence_time": c["commence"],
            "source": source,
        })
    drops.sort(key=lambda x: x["drop_pct"], reverse=True)
    return drops


# ===========================================================================
# SPORTYBET SCAN ORCHESTRATION
# ===========================================================================
async def sportybet_collect():
    """Fetch all prematch SportyBet candidates for the 24h window."""
    events = await SB.fetch_all_events()
    banned = await SB.fetch_banned_events()

    list_cands = []
    cs_events = []
    for e in events:
        meta = sb_event_meta(e)
        if not meta["event_id"] or not meta["home"] or not meta["away"]:
            continue
        if meta["event_id"] in banned:
            continue
        if meta["status"] != 0:  # 0 = not started (prematch only)
            continue
        mins = mins_to_kickoff(meta["commence_ms"])
        if mins <= 0 or mins > MAX_MINUTES_TO_KICKOFF:
            continue
        if is_excluded_league(meta["tournament_name"]):
            continue
        list_cands.extend(sb_extract_list_markets(e, meta))
        cs_events.append(meta)

    # Correct score + HT correct score via the Outcomes endpoint
    items = []
    for meta in cs_events:
        for oid in SB_CS_IDS:
            items.append({"eventId": meta["event_id"], "marketId": SB_M_CS, "outcomeId": oid, "specifier": None})
        for oid in SB_HT_CS_IDS:
            items.append({"eventId": meta["event_id"], "marketId": SB_M_HT_CS, "outcomeId": oid, "specifier": None})

    outcomes = await SB.fetch_outcomes(items) if items else {}
    meta_by_id = {m["event_id"]: m for m in cs_events}

    cs_cands, ht_cs_cands = [], []
    for (eid, mid, oid), o in outcomes.items():
        meta = meta_by_id.get(eid)
        if not meta:
            continue
        try:
            odds = float(o.get("odds") or 0)
        except Exception:
            odds = 0.0
        if odds < 1.01 or not int(o.get("isActive", 1) or 0):
            continue
        if mid == SB_M_CS:
            label = SB_CS_OUTCOMES.get(oid)
            if not label:
                continue
            cs_cands.append(_sb_cand(meta, mid, oid, label, o, "CS"))
        elif mid == SB_M_HT_CS:
            label = SB_HT_CS_OUTCOMES.get(oid)
            if not label:
                continue
            ht_cs_cands.append(_sb_cand(meta, mid, oid, label, o, "HTCS"))

    logger.info(
        "SportyBet candidates: list=%s cs=%s ht_cs=%s",
        len(list_cands), len(cs_cands), len(ht_cs_cands),
    )
    return list_cands, cs_cands, ht_cs_cands


# ===========================================================================
# COMBINED ANALYSIS JOB
# ===========================================================================
async def combined_analysis_job(bot, post_d=True, post_gift=False, target_chat_id=None):
    if await db_is_paused():
        logger.info("System paused - skipping job")
        return

    now = lagos_time()
    time_label = now.strftime("%d %b \u2022 %H:%M WAT")
    logger.info("Analysis job at %s | D=%s Gift=%s", time_label, post_d, post_gift)

    chat = target_chat_id or ADMIN_ID

    if post_d and not target_chat_id:
        try:
            await bot.send_message(chat, f"\U0001F504 *Running Analysis...*\n\U0001F550 `{time_label}`", parse_mode="Markdown")
        except Exception:
            pass

    # --- Pinnacle -----------------------------------------------------------
    last_upd = await db_get_setting("daily_fixtures_updated", "0")
    try:
        needs = (datetime.now() - datetime.fromisoformat(last_upd)).total_seconds() > 43200
    except Exception:
        needs = True
    if needs:
        await refresh_daily_fixtures()

    rows = await DB.select("daily_fixtures", {"select": "tournament_id"})
    tournament_ids = list({r["tournament_id"] for r in rows if r.get("tournament_id")})

    pinnacle_markets = list({m for m in (D_MARKETS + GIFT_D_MARKETS + [MARKET_HT_H2H, MARKET_CS]) if m})
    d_fixtures = await fetch_odds_for_tournaments(tournament_ids, "pinnacle", pinnacle_markets) if (post_d or post_gift) else []
    logger.info("Pinnacle fixtures fetched: %s", len(d_fixtures))

    pin_d_cands, pin_ft_cands, pin_ht_cands, pin_cs_cands = [], [], [], []
    for fx in d_fixtures:
        for c in extract_odds_from_fixture(fx, "pinnacle", D_MARKETS):
            pin_d_cands.append(c)
        for c in extract_odds_from_fixture(fx, "pinnacle", GIFT_D_MARKETS):
            pin_ft_cands.append(c)
        for c in extract_odds_from_fixture(fx, "pinnacle", [MARKET_HT_H2H]):
            if c["outcome_name"] in ("Draw", "X"):
                c["outcome_name"] = "HT Draw"
                pin_ht_cands.append(c)
        for c in extract_odds_from_fixture(fx, "pinnacle", [MARKET_CS]):
            if c["outcome_name"] in ALLOWED_CS:
                pin_cs_cands.append(c)

    # --- SportyBet ----------------------------------------------------------
    sb_list, sb_cs, sb_ht_cs = await sportybet_collect()
    sb_d_cands = [c for c in sb_list if c["market_group"] in ("1X2", "OU25", "BTTS")]
    sb_ft_cands = [c for c in sb_list if c["market_group"] == "1X2"]
    sb_ht_cands = [c for c in sb_list if c["market_group"] == "HT1X2"]

    # --- Run analyses -------------------------------------------------------
    pin_d = await run_d_analysis(pin_d_cands, "pinnacle") if pin_d_cands else []
    sb_d = await run_d_analysis(sb_d_cands, "sportybet") if sb_d_cands else []
    pin_ft = await run_ft_draw_analysis(pin_ft_cands, "pinnacle") if pin_ft_cands else []
    sb_ft = await run_ft_draw_analysis(sb_ft_cands, "sportybet") if sb_ft_cands else []
    pin_ht = await run_ht_draw_analysis(pin_ht_cands, "pinnacle") if pin_ht_cands else []
    sb_ht = await run_ht_draw_analysis(sb_ht_cands, "sportybet") if sb_ht_cands else []
    pin_cs = await run_cs_analysis(pin_cs_cands, "pinnacle", ALLOWED_CS) if pin_cs_cands else []
    sb_cs = await run_cs_analysis(sb_cs, "sportybet", set(SB_CS_OUTCOMES.values())) if sb_cs else []
    sb_ht_cs = await run_cs_analysis(sb_ht_cs, "sportybet", set(SB_HT_CS_OUTCOMES.values())) if sb_ht_cs else []

    # --- Combine ------------------------------------------------------------
    d_signals = _dedup_sort(pin_d + sb_d)
    ft_draws = _dedup_sort(pin_ft + sb_ft)
    ht_draws = _dedup_sort(pin_ht + sb_ht)
    cs_drops = _dedup_sort(pin_cs + sb_cs)
    ht_cs_drops = _dedup_sort(sb_ht_cs)

    # --- Cache --------------------------------------------------------------
    await DB.insert("d_analysis_cache", {
        "data": d_signals,
        "gift_data": ft_draws,
        "ht_draw_data": ht_draws,
        "cs_data": cs_drops,
        "ht_cs_data": ht_cs_drops,
    })
    await DB.rpc("devoul_cleanup_old_data")

    # --- Post ---------------------------------------------------------------
    if post_d and not target_chat_id:
        await _post_d(bot, chat, d_signals, time_label)

    if post_gift:
        await _post_gift(bot, chat, ft_draws, ht_draws, cs_drops, time_label, target_chat_id)

    logger.info(
        "Job done: D=%s FT=%s HT=%s CS=%s HtCS=%s",
        len(d_signals), len(ft_draws), len(ht_draws), len(cs_drops), len(ht_cs_drops),
    )


def _dedup_sort(items):
    best = {}
    for s in items:
        k = s["match"]
        if k not in best or s["drop_pct"] > best[k]["drop_pct"]:
            best[k] = s
    return sorted(best.values(), key=lambda x: x["drop_pct"], reverse=True)


async def _post_d(bot, chat, d_signals, time_label):
    try:
        if d_signals:
            await bot.send_message(
                chat,
                f"\U0001F4C9 *D ANALYSIS*\n\U0001F550 `{time_label}`\n" + "\u2501" * 30,
                parse_mode="Markdown",
            )
            for i, sig in enumerate(d_signals[:2], 1):
                await bot.send_message(chat, format_d_signal(sig, i), parse_mode="Markdown")
                await DB.insert("signal_results", {
                    "signal_type": "D", "match": sig["match"], "league": sig["league"],
                    "market": sig["market"], "prediction": str(sig["outcome"]),
                    "confidence": sig["confidence"], "odds": sig["current_odds"],
                    "api_fixture_id": sig.get("api_fixture_id", ""),
                })
                await asyncio.sleep(0.5)
        else:
            await bot.send_message(
                chat,
                f"\U0001F4C9 *D ANALYSIS*\n\U0001F550 `{time_label}`\n\n\U0001F4ED No qualifying signals.",
                parse_mode="Markdown",
            )
    except Exception as e:
        logger.error("D post error: %s", e)


async def _post_gift(bot, chat, ft_draws, ht_draws, cs_drops, time_label, target_chat_id):
    try:
        if target_chat_id:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("\U0001F381 FT X", callback_data="gift_ft"),
                InlineKeyboardButton("\U0001F550 HT X", callback_data="gift_ht"),
                InlineKeyboardButton("\U0001F3AF CS", callback_data="gift_cs"),
            ]])
            await bot.send_message(
                chat,
                f"\U0001F381 *GIFT PACK*\n\U0001F550 `{time_label}`\n\U0001F447 Select a category:",
                parse_mode="Markdown", reply_markup=kb,
            )
        else:
            msg = f"\U0001F381 *GIFT PACK*\n\U0001F550 `{time_label}`\n" + "\u2501" * 30 + "\n"
            if ft_draws:
                g = ft_draws[0]
                msg += f"\n\U0001F91D *FT Draw:* {g['match']} [{source_label(g['source'])}]\n\U0001F3C6 {g['league']}\n\U0001F4C9 Drop: {g['drop_pct']}% | Odds: {g['odds']}\n"
            if ht_draws:
                g = ht_draws[0]
                msg += f"\n\U0001F550 *HT Draw:* {g['match']} [{source_label(g['source'])}]\n\U0001F3C6 {g['league']}\n\U0001F4C9 Drop: {g['drop_pct']}% | Odds: {g['odds']}\n"
            if cs_drops:
                g = cs_drops[0]
                msg += f"\n\U0001F3AF *Correct Score:* {g['match']} [{source_label(g['source'])}]\n\U0001F3C6 {g['league']}\n\U0001F52E {g['prediction']}\n\U0001F4C9 Drop: {g['drop_pct']}% | Odds: {g['odds']}\n"
            msg += "\u2501" * 30 + f"\n\u26A0\uFE0F _Bet responsibly_ | \U0001F916 @{BOT_USERNAME}"
            await bot.send_message(chat, msg, parse_mode="Markdown")
    except Exception as e:
        logger.error("Gift post error: %s", e)


# ===========================================================================
# AI ACCA
# ===========================================================================
SYSTEM_PROMPT = (
    "You are the specialized sports trading brain of Devoul Bot. "
    "Analyze the provided dropping odds data across the last three sequential scans to identify trends "
    "and create a high-probability 3-Match Accumulator (ACCA).\n\n"
    "CRITICAL VALUE RULE: Absolutely NO highly speculative 'big man' risky selections are allowed in the final accumulator. "
    "Do NOT output exact Correct Scores or raw Full-Time Draws. Instead, deeply calculate the momentum of the drop across the historical scans "
    "and translate it into popular, liquid, high-probability markets. "
    "Examples: Double Chance (1X, X2, 12), Draw No Bet (DNB), Match Totals (Over/Under), or Clean Asian Handicaps (+1.5, +2).\n\n"
    "Take your time to deeply weigh the parameters to pick the best of the best 3 selections.\n\n"
    "Output Format Requirement: Provide ONLY a clean, premium quality, visually attractive, and highly scannable Telegram post "
    "using cool, vibrant emojis. Write ONLY the match details, the translated safe prediction, and your calculated confidence percentage. "
    "Do NOT include paragraphs of text, long-winded analysis explanations, introductions, or any meta-text."
)


async def get_ai_aggregated_data():
    rows = await DB.select("d_analysis_cache", {
        "select": "data,gift_data,ht_draw_data,cs_data,cached_at",
        "order": "id.desc", "limit": 3,
    })
    if not rows or len(rows) < 2:
        return None

    all_d, all_ft, all_ht, all_cs = [], [], [], []
    for scan_idx, row in enumerate(reversed(rows)):
        for s in (row.get("data") or []):
            all_d.append((s["match"], s["league"], s["market"], s.get("outcome", ""), s["drop_pct"], scan_idx, s["source"], s.get("commence_time", "")))
        for s in (row.get("gift_data") or []):
            all_ft.append((s["match"], s["league"], "Draw", None, s["drop_pct"], scan_idx, s["source"], s.get("commence_time", "")))
        for s in (row.get("ht_draw_data") or []):
            all_ht.append((s["match"], s["league"], "HT Draw", None, s["drop_pct"], scan_idx, s["source"], s.get("commence_time", "")))
        for s in (row.get("cs_data") or []):
            all_cs.append((s["match"], s["league"], s["prediction"], None, s["drop_pct"], scan_idx, s["source"], s.get("commence_time", "")))

    def future_only(e):
        try:
            ct = datetime.fromisoformat(str(e[7]).replace("Z", "+00:00"))
            if ct.tzinfo is None:
                ct = ct.replace(tzinfo=pytz.utc)
            return ct > now_utc()
        except Exception:
            return False

    all_d = [e for e in all_d if future_only(e)]
    all_ft = [e for e in all_ft if future_only(e)]
    all_ht = [e for e in all_ht if future_only(e)]
    all_cs = [e for e in all_cs if future_only(e)]

    def group(lst):
        g = defaultdict(list)
        for match, league, market, outcome, drop, scan_idx, source, commence in lst:
            g[(match, market, source)].append((drop, scan_idx, league, commence))
        return g

    d_g, ft_g, ht_g, cs_g = group(all_d), group(all_ft), group(all_ht), group(all_cs)
    sections = []

    d_items = [(k[0], k[1], k[2], v) for k, v in d_g.items() if max(x[0] for x in v) >= D_ANALYSIS_MIN_DROP_PCT]
    if d_items:
        sections.append("\U0001F4C9 *D Analysis (\u226510% drop)*")
        for match, market, source, drops in d_items:
            hist = ", ".join(f"scan{x[1]+1}: {x[0]:.1f}%" for x in sorted(drops, key=lambda z: z[1]))
            sections.append(f"{match} | {market} | {source_label(source)} | {hist}")

    cs_items = [(k[0], k[1], v) for k, v in cs_g.items() if k[2] == "pinnacle" and max(x[0] for x in v) >= CS_MIN_DROP_PCT]
    if cs_items:
        sections.append("\n\U0001F3AF *Pinnacle Correct Score (\u226512%)*")
        for match, pred, drops in cs_items:
            hist = ", ".join(f"scan{x[1]+1}: {x[0]:.1f}%" for x in sorted(drops, key=lambda z: z[1]))
            sections.append(f"{match} | {pred} | {hist}")

    ft_items = [(k[0], v) for k, v in ft_g.items() if k[2] == "pinnacle" and k[1] == "Draw" and max(x[0] for x in v) >= FT_DRAW_MIN_DROP_PCT]
    if ft_items:
        sections.append("\n\U0001F91D *Pinnacle FT Draw (\u226512%)*")
        for match, drops in ft_items:
            hist = ", ".join(f"scan{x[1]+1}: {x[0]:.1f}%" for x in sorted(drops, key=lambda z: z[1]))
            sections.append(f"{match} | Draw | {hist}")

    ht_items = [(k[0], v) for k, v in ht_g.items() if k[2] == "pinnacle" and max(x[0] for x in v) >= HT_DRAW_MIN_DROP_PCT]
    if ht_items:
        sections.append("\n\U0001F550 *Pinnacle HT Draw (\u22653%)*")
        for match, drops in ht_items:
            hist = ", ".join(f"scan{x[1]+1}: {x[0]:.1f}%" for x in sorted(drops, key=lambda z: z[1]))
            sections.append(f"{match} | HT Draw | {hist}")

    sb_ft_items = [(k[0], v) for k, v in ft_g.items() if k[2] == "sportybet" and k[1] == "Draw" and max(x[0] for x in v) >= SB_FT_DRAW_MIN_DROP_PCT]
    if sb_ft_items:
        sections.append("\n\U0001F171\uFE0F *SportyBet FT Draw (\u22656%)*")
        for match, drops in sb_ft_items:
            hist = ", ".join(f"scan{x[1]+1}: {x[0]:.1f}%" for x in sorted(drops, key=lambda z: z[1]))
            sections.append(f"{match} | Draw | {hist}")

    if not sections:
        return None
    return "\n".join(sections)


async def call_gemini_acca(prompt):
    if not gemini_client:
        return "\u274C Gemini key not configured."
    try:
        resp = await asyncio.to_thread(
            gemini_client.models.generate_content,
            model="gemini-2.0-flash",
            contents=f"{SYSTEM_PROMPT}\n\nData:\n{prompt}",
        )
        return resp.text
    except Exception as e:
        logger.error("Gemini ACCA error: %s", e)
        return "\u274C Gemini failed to generate ACCA. Try again later."


async def call_deepseek_acca(prompt):
    if not deepseek_client:
        return "\u274C OpenRouter API key not configured. Use /changeapi openrouter <key>"
    try:
        resp = await asyncio.to_thread(
            deepseek_client.chat.completions.create,
            model="deepseek/deepseek-chat",
            messages=[{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}],
            temperature=0.7,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.error("DeepSeek ACCA error: %s", e)
        return "\u274C DeepSeek failed to generate ACCA. Try again later."


# ===========================================================================
# FORMATTING
# ===========================================================================
def format_d_signal(signal, rank=1):
    rank_emoji = ["\U0001F947", "\U0001F948"][rank - 1] if rank <= 2 else "\U0001F525"
    drop = signal.get("drop_pct", 0)
    mins = signal.get("minutes_to_kickoff", 0)
    h, m = divmod(mins, 60)
    time_str = f"{h}h {m}m" if h else f"{m}m"
    return (
        f"{rank_emoji} *D ANALYSIS \u2014 SIGNAL #{rank}* [{source_label(signal.get('source'))}]\n"
        + "\u2501" * 30 + "\n"
        f"\u26BD *{signal['match']}*\n"
        f"\U0001F3C6 `{signal['league']}`\n\n"
        f"\U0001F4CC *Prediction:* `{signal['market']}`\n"
        f"\U0001F3AF *Confidence:* `{signal['confidence']}%`\n"
        f"\u26A1 *Signal Strength:* {signal['confidence_label']}\n"
        + "\u2501" * 30 + "\n"
        f"\U0001F4C9 Odds: `{signal['original_odds']}` \u2192 `{signal['current_odds']}` (*\u2193{drop:.1f}%*)\n"
        f"\u23F1 Kickoff in `{time_str}` ({fmt_kickoff(signal.get('commence_time'))})\n"
        + "\u2501" * 30 + "\n"
        f"\U0001F916 @{BOT_USERNAME}"
    )


def format_gift(gift, gift_type="draw"):
    label = {
        "ht": "\U0001F550 HT DRAW GIFT",
        "cs": "\U0001F3AF CORRECT SCORE GIFT",
        "htcs": "\U0001F550 HT CORRECT SCORE GIFT",
    }.get(gift_type, "\U0001F91D FT DRAW GIFT")
    return (
        f"\U0001F381 *{label}* [{source_label(gift.get('source'))}]\n"
        + "\u2501" * 30 + "\n"
        f"\u26BD *{gift.get('match', '')}*\n"
        f"\U0001F3C6 `{gift.get('league', '')}`\n\n"
        f"\U0001F52E *Prediction:* `{gift.get('prediction', 'Draw')}`\n"
        f"\U0001F48E *Odds:* `{gift.get('odds', gift.get('original_odds', 'N/A'))}`\n"
        f"\U0001F4CA *Confidence:* `{gift.get('confidence', 0)}%`\n"
        + "\u2501" * 30 + "\n"
        f"\U0001F4C9 Drop: `{gift.get('drop_pct', '')}%`\n"
        f"\U0001F550 {fmt_kickoff(gift.get('commence_time'))}\n\n"
        f"\u26A0\uFE0F _Bet responsibly_ | \U0001F916 @{BOT_USERNAME}"
    )


# ===========================================================================
# SECRET TEXT PARSING
# ===========================================================================
SECRET_RE = re.compile(r"^(da|x|htx|cs|htcs)\s+(\d+)(?:\s*-\s*(\d+))?$", re.IGNORECASE)


def parse_secret(text):
    """Parse a secret command. Returns dict(kind, start, end) or None.

    DA 1-5  -> d-analysis 1st..5th      DA 6 -> 6th
    X 1-5   -> FT draw highest..5th     X 2  -> 2nd
    Htx 1-5 -> HT draw 1st..5th         Htx 7 -> 7th
    CS 1-7  -> correct score list
    HtCS 1-9-> HT correct score list
    """
    if not text:
        return None
    m = SECRET_RE.match(text.strip())
    if not m:
        return None
    kind = m.group(1).lower()
    start = int(m.group(2))
    end = int(m.group(3)) if m.group(3) else start
    if start < 1 or end < start:
        return None
    return {"kind": kind, "start": start, "end": end}


def _slice(items, start, end):
    return items[start - 1:end]


# ===========================================================================
# BOT HANDLERS
# ===========================================================================
def access_denied_kb():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("\U0001F4AC Contact @%s" % ACCESS_DENIED_CONTACT, url=ACCESS_DENIED_URL)
    ]])


async def deny(update):
    await update.message.reply_text(
        "\U0001F512 *Access Denied*\n\nThis bot is private.\n"
        f"Contact the owner to request access.",
        parse_mode="Markdown",
        reply_markup=access_denied_kb(),
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid != ADMIN_ID:
        await deny(update)
        return
    name = update.effective_user.first_name or "Champ"
    await update.message.reply_text(
        f"\U0001F451 *Welcome back, {name}!*\n\n"
        f"\U0001F52E *Devoul* \u2014 Mathematical Betting Intelligence\n\n"
        f"\U0001F4C9 *D Analysis* \u2014 Sharp market drops (Pinnacle + SportyBet)\n"
        f"\U0001F381 *Gifts* \u2014 FT Draw, HT Draw, Correct Score & HT Correct Score\n\n"
        f"Use /scanprivate to run a private scan.\n"
        f"_Secret texts:_ `DA 1-5`, `X 1-5`, `Htx 1-5`, `CS 1-7`, `HtCS 1-9`",
        parse_mode="Markdown",
    )


async def cmd_scanprivate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await deny(update)
        return
    args = context.args
    mode = args[0].lower() if args else "both"
    pd = mode in ("d", "both")
    pg = mode in ("gift", "both")
    if mode == "gift":
        pd = False
    await update.message.reply_text(f"\U0001F504 Running private scan: `{mode}`...", parse_mode="Markdown")
    await combined_analysis_job(context.bot, post_d=pd, post_gift=pg, target_chat_id=update.effective_chat.id)
    await update.message.reply_text("\u2705 Private scan complete.")


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await db_set_setting("system_paused", "1")
    await update.message.reply_text("\U0001F534 *System PAUSED*", parse_mode="Markdown")


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await db_set_setting("system_paused", "0")
    await update.message.reply_text("\U0001F7E2 *System RESUMED*", parse_mode="Markdown")


async def cmd_changeapi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    global ODDSPAPI_KEY, gemini_client, openrouter_api_key, deepseek_client
    args = context.args
    if len(args) < 2 or args[0] not in ("oddspapi", "gemini", "openrouter"):
        await update.message.reply_text(
            "Usage:\n`/changeapi oddspapi NEW_KEY`\n`/changeapi gemini NEW_KEY`\n`/changeapi openrouter NEW_KEY`",
            parse_mode="Markdown",
        )
        return
    service, new_key = args[0], args[1]
    if service == "oddspapi":
        ODDSPAPI_KEY = new_key
        await db_set_setting("oddspapi_key", new_key)
        await load_market_ids()
        await update.message.reply_text("\u2705 *OddsPapi key updated!*", parse_mode="Markdown")
    elif service == "gemini":
        gemini_client = genai.Client(api_key=new_key)
        await db_set_setting("gemini_key", new_key)
        await update.message.reply_text("\u2705 *Gemini key updated!*", parse_mode="Markdown")
    elif service == "openrouter":
        openrouter_api_key = new_key
        await db_set_setting("openrouter_api_key", new_key)
        deepseek_client = OpenAI(api_key=new_key, base_url="https://openrouter.ai/api/v1") if new_key else None
        await update.message.reply_text("\u2705 *OpenRouter key updated!*", parse_mode="Markdown")


async def cmd_toggle_ai(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    global ai_buttons_public
    ai_buttons_public = not ai_buttons_public
    await db_set_setting("ai_public", "1" if ai_buttons_public else "0")
    state = "\U0001F30D public" if ai_buttons_public else "\U0001F510 admin-only"
    await update.message.reply_text(f"\U0001F916 *AI ACCA visibility*: `{state}`", parse_mode="Markdown")


async def handle_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = q.from_user.id
    await q.answer()
    if uid != ADMIN_ID:
        await q.message.reply_text("\U0001F512 Access denied.", reply_markup=access_denied_kb())
        return

    if q.data in ("gift_ft", "gift_ht", "gift_cs"):
        col = {"gift_ft": "gift_data", "gift_ht": "ht_draw_data", "gift_cs": "cs_data"}[q.data]
        rows = await DB.select("d_analysis_cache", {"select": col, "order": "id.desc", "limit": 1})
        items = (rows[0].get(col) if rows else None) or []
        if not items:
            await q.message.reply_text("\U0001F4ED No data available.")
            return
        kind = {"gift_ft": "draw", "gift_ht": "ht", "gift_cs": "cs"}[q.data]
        for g in items[:2]:
            await q.message.reply_text(format_gift(g, kind), parse_mode="Markdown")
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("\U0001F381 FT X", callback_data="gift_ft"),
            InlineKeyboardButton("\U0001F550 HT X", callback_data="gift_ht"),
            InlineKeyboardButton("\U0001F3AF CS", callback_data="gift_cs"),
        ]])
        await q.message.reply_text("\U0001F447 Select a category:", reply_markup=kb)

    elif q.data in ("ai_gemini", "ai_deepseek"):
        if not (uid == ADMIN_ID or ai_buttons_public):
            await q.answer("\u26D4 Access denied.", show_alert=True)
            return
        await q.message.reply_text("\u23F3 *Analysing data & building premium ACCA...*", parse_mode="Markdown")
        data = await get_ai_aggregated_data()
        if not data:
            await q.message.reply_text("\u274C Not enough qualifying data to generate an ACCA right now.")
            return
        text = await (call_gemini_acca(data) if q.data == "ai_gemini" else call_deepseek_acca(data))
        await q.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    if uid != ADMIN_ID:
        await deny(update)
        return

    text = (update.message.text or "").strip()

    if text == "\U0001F916 AI ACCA":
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("\U0001F9E0 Gemini", callback_data="ai_gemini"),
            InlineKeyboardButton("\U0001F52E DeepSeek", callback_data="ai_deepseek"),
        ]])
        await update.message.reply_text("\U0001F916 *AI ACCA Generator*\n\nChoose a model:", parse_mode="Markdown", reply_markup=kb)
        return

    parsed = parse_secret(text)
    if not parsed:
        return
    await _dispatch_secret(update, parsed)


async def _dispatch_secret(update, parsed):
    kind, start, end = parsed["kind"], parsed["start"], parsed["end"]

    if kind == "da":
        rows = await DB.select("d_analysis_cache", {"select": "data", "order": "id.desc", "limit": 1})
        items = (rows[0].get("data") if rows else None) or []
        if not items:
            await update.message.reply_text("\U0001F4C9 No D analysis data.")
            return
        sel = _slice(items, start, end)
        if not sel:
            await update.message.reply_text(f"\U0001F4C9 Only {len(items)} signals available.")
            return
        for i, s in enumerate(sel, start):
            await update.message.reply_text(format_d_signal(s, i), parse_mode="Markdown")
        return

    if kind == "x":
        rows = await DB.select("d_analysis_cache", {"select": "gift_data", "order": "id.desc", "limit": 1})
        items = (rows[0].get("gift_data") if rows else None) or []
        if not items:
            await update.message.reply_text("\U0001F91D No FT draw data.")
            return
        sel = _slice(items, start, end)
        if not sel:
            await update.message.reply_text(f"\U0001F91D Only {len(items)} FT draws available.")
            return
        for g in sel:
            await update.message.reply_text(format_gift(g, "draw"), parse_mode="Markdown")
        return

    if kind == "htx":
        rows = await DB.select("d_analysis_cache", {"select": "ht_draw_data", "order": "id.desc", "limit": 1})
        items = (rows[0].get("ht_draw_data") if rows else None) or []
        if not items:
            await update.message.reply_text("\U0001F550 No HT draw data.")
            return
        sel = _slice(items, start, end)
        if not sel:
            await update.message.reply_text(f"\U0001F550 Only {len(items)} HT draws available.")
            return
        for g in sel:
            await update.message.reply_text(format_gift(g, "ht"), parse_mode="Markdown")
        return

    if kind == "cs":
        rows = await DB.select("d_analysis_cache", {"select": "cs_data", "order": "id.desc", "limit": 1})
        items = (rows[0].get("cs_data") if rows else None) or []
        if not items:
            await update.message.reply_text("\U0001F3AF No correct score data.")
            return
        sel = _slice(items, start, end)
        if not sel:
            await update.message.reply_text(f"\U0001F3AF Only {len(items)} correct score drops available.")
            return
        for g in sel:
            await update.message.reply_text(format_gift(g, "cs"), parse_mode="Markdown")
        return

    if kind == "htcs":
        rows = await DB.select("d_analysis_cache", {"select": "ht_cs_data", "order": "id.desc", "limit": 1})
        items = (rows[0].get("ht_cs_data") if rows else None) or []
        if not items:
            await update.message.reply_text("\U0001F550 No HT correct score data.")
            return
        sel = _slice(items, start, end)
        if not sel:
            await update.message.reply_text(f"\U0001F550 Only {len(items)} HT correct score drops available.")
            return
        for g in sel:
            await update.message.reply_text(format_gift(g, "htcs"), parse_mode="Markdown")
        return


# ===========================================================================
# SCHEDULER
# ===========================================================================
def setup_scheduler(bot):
    s = AsyncIOScheduler(timezone=LAGOS)
    grace = 3600

    def job(pd, pg):
        async def runner():
            await combined_analysis_job(bot, post_d=pd, post_gift=pg)
        return runner

    for i, entry in enumerate(SCAN_SCHEDULE):
        s.add_job(
            job(bool(entry.get("d")), bool(entry.get("gift"))),
            CronTrigger(hour=int(entry["hour"]), minute=int(entry.get("minute", 0)), timezone=LAGOS),
            misfire_grace_time=grace,
            id=f"job_{i}_{entry['hour']}_{entry.get('minute', 0)}",
        )
    return s


# ===========================================================================
# POST INIT
# ===========================================================================
async def post_init(application):
    bot = application.bot
    global ODDSPAPI_KEY, gemini_client, ai_buttons_public, openrouter_api_key, deepseek_client

    saved = await db_get_setting("oddspapi_key")
    if saved:
        ODDSPAPI_KEY = saved
    saved_gemini = await db_get_setting("gemini_key")
    if saved_gemini:
        gemini_client = genai.Client(api_key=saved_gemini)

    await load_market_ids()

    ai_buttons_public = (await db_get_setting("ai_public", "0")) == "1"
    openrouter_api_key = await db_get_setting("openrouter_api_key", "") or ""
    deepseek_client = OpenAI(api_key=openrouter_api_key, base_url="https://openrouter.ai/api/v1") if openrouter_api_key else None

    await bot.set_my_commands([BotCommand("start", "Start")])
    await bot.set_my_commands([
        BotCommand("start", "Start"),
        BotCommand("scanprivate", "Private scan (d/gift/both)"),
        BotCommand("pause", "Pause all jobs"),
        BotCommand("resume", "Resume all jobs"),
        BotCommand("changeapi", "Change API keys"),
        BotCommand("toggle_ai", "Toggle AI ACCA visibility"),
    ], scope=BotCommandScopeChat(chat_id=ADMIN_ID))

    scheduler = setup_scheduler(bot)
    scheduler.start()
    application.bot_data["scheduler"] = scheduler
    logger.info("Scheduler started with %s jobs.", len(SCAN_SCHEDULE))


# ===========================================================================
# MAIN
# ===========================================================================
def build_app():
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("scanprivate", cmd_scanprivate))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("changeapi", cmd_changeapi))
    app.add_handler(CommandHandler("toggle_ai", cmd_toggle_ai))
    app.add_handler(CallbackQueryHandler(handle_callbacks))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    return app


if __name__ == "__main__":
    logger.info("Devoul Bot starting...")
    build_app().run_polling(drop_pending_updates=True)
