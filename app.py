import sqlite3
import asyncio
import aiohttp
import json
import logging
import re
from datetime import datetime, timedelta
from collections import defaultdict
import pytz
from google import genai
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    LabeledPrice, BotCommand, BotCommandScopeChat, ReplyKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    PreCheckoutQueryHandler, MessageHandler, filters, ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from openai import OpenAI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
TOKEN          = "8634769890:AAEqnezhWPQiTSaCF7BMvq7uA0G_rmATCEw"
GEMINI_API_KEY = "AQ.Ab8RN6KKyXZmNDgKEaIAYMDEbmAckAapflP4zO40vmxRBdPGHg"
ODDSPAPI_KEY   = "cae3b636-42d5-4ad7-9994-18eea9e683f4"
ODDSPAPI_BASE  = "https://api.oddspapi.io"
ODDSAPI_IO_KEY = "51695d0a03ae6cd5436b89eec5a82c6a0091c58c1e8ec7336debe722d816d5f4"
ODDSAPI_IO_BASE = "https://api.odds-api.io/v3"
ODDSAPI_SINGBET_NAME = "SingBet"   # used only for odds-api.io

ADMIN_ID          = 6186002683
BOT_USERNAME      = "devoulbot"
FREE_CHANNEL_ID   = -1003909848049
FREE_CHANNEL_LINK = "https://t.me/devoulclan"
D_CHANNEL_ID      = -1003217106495
V_CHANNEL_ID      = -1003848965303

TRIAL_DAYS = 7
LAGOS      = pytz.timezone("Africa/Lagos")
SPORT_ID   = 10   # football

# Market IDs – loaded at startup from OddsPapi
MARKET_H2H      = None
MARKET_BTTS     = None
MARKET_OVER15   = None
MARKET_OVER25   = None
MARKET_UNDER15  = None
MARKET_UNDER25  = None
MARKET_HT_H2H   = 10208   # First Half Result (1X2)
MARKET_CS       = 10336   # Full Time Correct Score
OUTCOME_MAP     = {}      # (market_id, outcome_id) -> outcomeName
MARKET_HANDICAP = {}      # market_id -> handicap float

D_MARKETS   = []   # H2H, BTTS, O1.5, O2.5
GIFT_D_MARKETS = []  # FT draw (H2H X) only

# Supported correct score outcomes (OddsPapi uses colon, e.g., "0:0")
ALLOWED_CS = {
    "0:0", "1:0", "0:1", "1:1", "0:2", "1:2",
    "2:0", "2:1", "2:2", "3:0", "0:3", "1:3", "3:1", "3:3"
}

# Exchange detection (V disabled)
V_BOOKMAKER_AVAILABLE = False
V_BOOKMAKER_SLUG = None

gemini_client = genai.Client(api_key=GEMINI_API_KEY)

# NEW: AI ACCA global toggle & OpenRouter client
ai_buttons_public = False
openrouter_api_key = ""  # will be loaded from settings
deepseek_client = None   # will be initialized once key is known

# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────
def db_query(query, params=(), fetch=False):
    conn = sqlite3.connect("devoul.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(query, params)
    res = cursor.fetchall() if fetch else None
    conn.commit()
    conn.close()
    return [tuple(r) for r in res] if res else ([] if fetch else None)

def init_db():
    db_query("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, joined_date TEXT, trial_end TEXT,
        d_access INTEGER DEFAULT 0, v_access INTEGER DEFAULT 0,
        is_banned INTEGER DEFAULT 0, d_expiry TEXT, v_expiry TEXT,
        is_free_vip INTEGER DEFAULT 0)""")
    db_query("""CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER,
        amount INTEGER, item TEXT, date TEXT)""")
    db_query("""CREATE TABLE IF NOT EXISTS odds_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_key TEXT, tournament_id INTEGER, home TEXT, away TEXT,
        market_id TEXT, outcome_id TEXT, outcome_name TEXT,
        odds REAL, source TEXT DEFAULT 'pinnacle',
        recorded_at TEXT)""")
    db_query("""CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY, value TEXT)""")
    db_query("""CREATE TABLE IF NOT EXISTS d_analysis_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT, gift_data TEXT, cached_at TEXT)""")
    db_query("""CREATE TABLE IF NOT EXISTS v_analysis_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT, gift_data TEXT, cached_at TEXT)""")
    db_query("""CREATE TABLE IF NOT EXISTS signal_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_type TEXT, match TEXT, league TEXT,
        market TEXT, prediction TEXT, confidence INTEGER,
        odds REAL, result TEXT DEFAULT 'pending',
        api_fixture_id TEXT DEFAULT '',
        recorded_at TEXT, settled_at TEXT)""")
    db_query("""CREATE TABLE IF NOT EXISTS daily_fixtures (
        fixture_id TEXT PRIMARY KEY,
        tournament_id INTEGER,
        start_time TEXT,
        recorded_at TEXT,
        oddsapi_event_id TEXT,
        home TEXT,
        away TEXT)""")
    db_query("""CREATE TABLE IF NOT EXISTS movement_memory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_key TEXT, market_id INTEGER, outcome_name TEXT,
        snapshot_time TEXT, odds REAL, change_pct REAL,
        movement_type TEXT)""")
    db_query("""CREATE TABLE IF NOT EXISTS gift_snapshot (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_key TEXT, tournament_id INTEGER, home TEXT, away TEXT,
        market_id TEXT, outcome_id TEXT, outcome_name TEXT,
        odds REAL, volume REAL DEFAULT 0,
        source TEXT DEFAULT 'pinnacle',
        recorded_at TEXT)""")
    db_query("INSERT OR IGNORE INTO settings VALUES ('d_price','0')")
    db_query("INSERT OR IGNORE INTO settings VALUES ('v_price','0')")
    db_query("INSERT OR IGNORE INTO settings VALUES ('system_paused','0')")
    db_query("INSERT OR IGNORE INTO settings VALUES ('daily_fixtures_updated','0')")
    db_query("INSERT OR IGNORE INTO settings VALUES ('oddsapi_key',?)", (ODDSAPI_IO_KEY,))
    db_query("INSERT OR IGNORE INTO settings VALUES ('singbet_source','oddspapi')")
    db_query("INSERT OR IGNORE INTO settings VALUES ('ai_public','0')")
    db_query("INSERT OR IGNORE INTO settings VALUES ('openrouter_api_key','')")   # NEW
    # New columns for cache
    for col in ["ht_draw_data TEXT", "cs_data TEXT"]:
        try: db_query(f"ALTER TABLE d_analysis_cache ADD COLUMN {col}")
        except: pass
    for col in ["d_expiry TEXT","v_expiry TEXT","is_free_vip INTEGER DEFAULT 0"]:
        try: db_query(f"ALTER TABLE users ADD COLUMN {col}")
        except: pass
    for col in ["oddsapi_event_id TEXT", "home TEXT", "away TEXT"]:
        try: db_query(f"ALTER TABLE daily_fixtures ADD COLUMN {col}")
        except: pass
    try: db_query("ALTER TABLE odds_history ADD COLUMN source TEXT DEFAULT 'pinnacle'")
    except: pass
    try: db_query("ALTER TABLE gift_snapshot ADD COLUMN source TEXT DEFAULT 'pinnacle'")
    except: pass

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def get_setting(key):
    row = db_query("SELECT value FROM settings WHERE key=?", (key,), True)
    return row[0][0] if row else None

def set_setting(key, value):
    db_query("INSERT OR REPLACE INTO settings VALUES (?,?)", (key, str(value)))

def is_paused(): return get_setting('system_paused') == '1'

def get_user(uid):
    r = db_query("SELECT * FROM users WHERE user_id=?", (uid,), True)
    return r[0] if r else None

def upsert_user(uid):
    if not get_user(uid):
        db_query("INSERT INTO users (user_id,joined_date) VALUES (?,?)",
                 (uid, datetime.now().isoformat()))

def is_admin(uid): return uid == ADMIN_ID

def user_has_trial(uid):
    u = get_user(uid)
    return bool(u and u[2] and datetime.now() < datetime.fromisoformat(u[2]))

def user_has_d_access(uid):
    if is_admin(uid): return True
    u = get_user(uid)
    if not u or u[5] == 1: return False
    if u[8] == 1 or user_has_trial(uid): return True
    if u[3] == 1:
        return (not u[6]) or datetime.now() < datetime.fromisoformat(u[6])
    return False

def user_has_v_access(uid):
    if is_admin(uid): return True
    u = get_user(uid)
    if not u or u[5] == 1: return False
    if u[8] == 1 or user_has_trial(uid): return True
    if u[4] == 1:
        return (not u[7]) or datetime.now() < datetime.fromisoformat(u[7])
    return False

def get_d_price(): return int(get_setting('d_price') or 0)
def get_v_price(): return int(get_setting('v_price') or 0)
def lagos_time(): return datetime.now(LAGOS)

def get_singbet_source():
    src = get_setting('singbet_source')
    if src in ('oddspapi', 'oddsapi'):
        return src
    return 'oddspapi'

def main_menu_kb(uid):
    d = "📉 D Analysis" if user_has_d_access(uid) else "📉 D Analysis 🔒"
    v = "📊 V Analysis" if user_has_v_access(uid) else "📊 V Analysis 🔒"
    rows = [[d, v], ["📋 My Account", "ℹ️ Help & Support"]]
    # AI ACCA button visible to admin always, or public if toggled
    if is_admin(uid) or ai_buttons_public:
        rows.append(["🤖 AI ACCA"])   # NEW
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def mins_to_kickoff(commence_str):
    try:
        dt = datetime.fromisoformat(commence_str.replace("Z", "+00:00"))
        return max(0, int((dt - datetime.now(pytz.utc)).total_seconds() / 60))
    except:
        return 9999

# ─────────────────────────────────────────────────────────────
# AI ACCA AGGREGATION (3-SCAN HISTORICAL FILTER)
# ─────────────────────────────────────────────────────────────
def get_ai_aggregated_data():
    """Returns filtered text payload for AI based on last 3 scans."""
    rows = db_query(
        "SELECT data, gift_data, ht_draw_data, cs_data, cached_at "
        "FROM d_analysis_cache ORDER BY id DESC LIMIT 3", (), fetch=True
    )
    if not rows or len(rows) < 2:
        return None

    # Flatten data from all three scans (oldest to newest)
    all_d = []
    all_ft = []
    all_ht = []
    all_cs = []

    for scan_idx, row in enumerate(reversed(rows)):
        d_signals = json.loads(row[0]) if row[0] else []
        ft_draws  = json.loads(row[1]) if row[1] else []
        ht_draws  = json.loads(row[2]) if row[2] else []
        cs_drops  = json.loads(row[3]) if row[3] else []

        for s in d_signals:
            all_d.append((s["match"], s["league"], s["market"], s.get("outcome", ""),
                          s["drop_pct"], scan_idx, s["source"], s.get("commence_time", "")))
        for s in ft_draws:
            all_ft.append((s["match"], s["league"], "Draw", None,
                           s["drop_pct"], scan_idx, s["source"], s.get("commence_time", "")))
        for s in ht_draws:
            all_ht.append((s["match"], s["league"], "HT Draw", None,
                           s["drop_pct"], scan_idx, s["source"], s.get("commence_time", "")))
        for s in cs_drops:
            all_cs.append((s["match"], s["league"], s["prediction"], None,
                           s["drop_pct"], scan_idx, s["source"], s.get("commence_time", "")))

    # Remove past matches
    now_utc = datetime.now(pytz.utc)
    def future_only(entry):
        try:
            ct = datetime.fromisoformat(entry[7].replace("Z", "+00:00"))
            return ct > now_utc
        except:
            return False

    all_d  = [e for e in all_d if future_only(e)]
    all_ft = [e for e in all_ft if future_only(e)]
    all_ht = [e for e in all_ht if future_only(e)]
    all_cs = [e for e in all_cs if future_only(e)]

    def group_data(lst):
        groups = defaultdict(list)
        for match, league, market, outcome, drop, scan_idx, source, commence in lst:
            key = (match, market, source)
            groups[key].append((drop, scan_idx, league, commence))
        return groups

    d_grouped = group_data(all_d)
    ft_grouped = group_data(all_ft)
    ht_grouped = group_data(all_ht)
    cs_grouped = group_data(all_cs)

    sections = []

    # D Analysis (≥10% drop)
    d_items = []
    for key, drops in d_grouped.items():
        if max(d[0] for d in drops) >= 10:
            d_items.append((key[0], key[1], key[2], drops))
    if d_items:
        sections.append("📉 *D Analysis (≥10% drop)*")
        for match, market, source, drops in d_items:
            scan_history = ", ".join([f"scan{d[1]+1}: {d[0]:.1f}%" for d in sorted(drops, key=lambda x: x[1])])
            sections.append(f"{match} | {market} | {source} | {scan_history}")

    # Pinnacle CS ≥12%
    cs_items = []
    for key, drops in cs_grouped.items():
        if key[2] == "pinnacle" and max(d[0] for d in drops) >= 12:
            cs_items.append((key[0], key[1], drops))
    if cs_items:
        sections.append("\n🎯 *Pinnacle Correct Score (≥12%)*")
        for match, pred, drops in cs_items:
            scan_history = ", ".join([f"scan{d[1]+1}: {d[0]:.1f}%" for d in sorted(drops, key=lambda x: x[1])])
            sections.append(f"{match} | {pred} | {scan_history}")

    # Pinnacle FT Draw ≥12%
    ft_p_items = []
    for key, drops in ft_grouped.items():
        if key[2] == "pinnacle" and key[1] == "Draw" and max(d[0] for d in drops) >= 12:
            ft_p_items.append((key[0], drops))
    if ft_p_items:
        sections.append("\n🤝 *Pinnacle FT Draw (≥12%)*")
        for match, drops in ft_p_items:
            scan_history = ", ".join([f"scan{d[1]+1}: {d[0]:.1f}%" for d in sorted(drops, key=lambda x: x[1])])
            sections.append(f"{match} | Draw | {scan_history}")

    # Pinnacle HT Draw ≥3%
    ht_p_items = []
    for key, drops in ht_grouped.items():
        if key[2] == "pinnacle" and max(d[0] for d in drops) >= 3:
            ht_p_items.append((key[0], drops))
    if ht_p_items:
        sections.append("\n🕐 *Pinnacle HT Draw (≥3%)*")
        for match, drops in ht_p_items:
            scan_history = ", ".join([f"scan{d[1]+1}: {d[0]:.1f}%" for d in sorted(drops, key=lambda x: x[1])])
            sections.append(f"{match} | HT Draw | {scan_history}")

    # Singbet FT Draw ≥6% (with danger note)
    ft_s_items = []
    for key, drops in ft_grouped.items():
        if key[2] == "singbet" and key[1] == "Draw" and max(d[0] for d in drops) >= 6:
            ft_s_items.append((key[0], drops))
    if ft_s_items:
        sections.append("\n🅱️ *Singbet FT Draw (≥6%, equivalent to 12%+ on Pinnacle)*")
        for match, drops in ft_s_items:
            scan_history = ", ".join([f"scan{d[1]+1}: {d[0]:.1f}%" for d in sorted(drops, key=lambda x: x[1])])
            sections.append(f"{match} | Draw | {scan_history}")

    if not sections:
        return None

    payload = "\n".join(sections)
    return payload

# ─────────────────────────────────────────────────────────────
# AI SYSTEM PROMPT & MODEL CALLERS
# ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = (
    "You are the specialized sports trading brain of Devoul Bot. "
    "Analyze the provided dropping odds data across the last three sequential scans to identify trends "
    "and create a high-probability 3-Match Accumulator (ACCA).\n\n"
    "CRITICAL VALUE RULE: Absolutely NO highly speculative 'big man' risky selections are allowed in the final accumulator. "
    "Do NOT output exact Correct Scores or raw Full-Time Draws. Instead, deeply calculate the momentum of the drop across the historical scans "
    "and translate it into popular, liquid, high-probability markets. "
    "Examples: Double Chance (1X, X2, 12), Draw No Bet (DNB), Match Totals (Over/Under), or Clean Asian Handicaps (+1.5, +2).\n\n"
    "Take your time to deeply weigh the parameters, especially paying attention to the dangerous Singbet 6%+ signals, "
    "to pick the best of the best 3 selections.\n\n"
    "Output Format Requirement: Provide ONLY a clean, premium quality, visually attractive, and highly scannable Telegram post "
    "using cool, vibrant emojis. Write ONLY the match details, the translated safe prediction, and your calculated confidence percentage. "
    "Do NOT include paragraphs of text, long-winded analysis explanations, introductions, or any meta-text."
)

async def call_gemini_acca(prompt: str) -> str:
    # Use the global GEMINI_API_KEY (now updated)
    client = genai.Client(api_key=GEMINI_API_KEY)
    try:
        response = client.models.generate_content(
            model="gemini-3.5-flash",
            contents=f"{SYSTEM_PROMPT}\n\nData:\n{prompt}",
        )
        return response.text
    except Exception as e:
        logger.error(f"Gemini ACCA error: {e}")
        return "❌ Gemini failed to generate ACCA. Try again later."

async def call_deepseek_acca(prompt: str) -> str:
    if not deepseek_client:
        return "❌ OpenRouter API key not configured. Use /changeapi openrouter <key>"
    try:
        response = deepseek_client.chat.completions.create(
            model="deepseek/deepseek-v4-pro",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"DeepSeek (OpenRouter) ACCA error: {e}")
        return "❌ DeepSeek failed to generate ACCA. Try again later."

# ─────────────────────────────────────────────────────────────
# MARKET LOADING (OddsPapi) (unchanged)
# ─────────────────────────────────────────────────────────────
async def load_market_ids():
    global MARKET_H2H, MARKET_BTTS, MARKET_OVER15, MARKET_OVER25
    global MARKET_UNDER15, MARKET_UNDER25, D_MARKETS, GIFT_D_MARKETS
    global OUTCOME_MAP, MARKET_HANDICAP, MARKET_HT_H2H, MARKET_CS

    logger.info("Loading market IDs from OddsPapi...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{ODDSPAPI_BASE}/v4/markets",
                params={"language": "en", "apiKey": ODDSPAPI_KEY},
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Market fetch failed: {resp.status}")
                    _set_fallback_markets()
                    return
                markets = await resp.json()
    except Exception as e:
        logger.error(f"Market fetch error: {e}")
        _set_fallback_markets()
        return

    for m in markets:
        mid      = m.get("marketId")
        mtype    = m.get("marketType", "")
        handicap = m.get("handicap", 0)
        period   = m.get("period", "")
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
    logger.info(f"Markets: H2H={MARKET_H2H} BTTS={MARKET_BTTS} O1.5={MARKET_OVER15} O2.5={MARKET_OVER25} HT={MARKET_HT_H2H} CS={MARKET_CS}")

def _set_fallback_markets():
    global D_MARKETS, GIFT_D_MARKETS
    global MARKET_H2H, MARKET_BTTS, MARKET_OVER15, MARKET_OVER25
    if not MARKET_H2H:    MARKET_H2H    = 101
    if not MARKET_BTTS:   MARKET_BTTS   = 104
    if not MARKET_OVER15: MARKET_OVER15 = 108
    if not MARKET_OVER25: MARKET_OVER25 = 1010
    D_MARKETS      = [MARKET_H2H, MARKET_BTTS, MARKET_OVER15, MARKET_OVER25]
    GIFT_D_MARKETS = [MARKET_H2H]

def outcome_label(market_id, outcome_id, home, away):
    name = OUTCOME_MAP.get((market_id, outcome_id), "")
    if not name:
        return None
    if market_id in (MARKET_H2H, MARKET_HT_H2H):
        if name == "1":   return "Home Win"
        if name == "X":   return "Draw"
        if name == "2":   return "Away Win"
        return None
    if market_id == MARKET_BTTS:
        if name == "Yes": return "BTTS Yes"
        if name == "No":  return "BTTS No"
        return None
    if market_id in (MARKET_OVER15, MARKET_OVER25):
        h = MARKET_HANDICAP.get(market_id, "")
        if name == "Over":  return f"Over {h}"
        if name == "Under": return f"Under {h}"
        return None
    if market_id == MARKET_CS:
        return name
    return None

# ─────────────────────────────────────────────────────────────
# DAILY FIXTURES (OddsPapi + oddsapi mapping)
# ─────────────────────────────────────────────────────────────
async def refresh_oddsapi_event_mapping():
    key = get_setting("oddsapi_key") or ODDSAPI_IO_KEY
    now = datetime.now(pytz.utc)
    from_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_time   = (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info("Mapping odds-api.io events for today...")
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{ODDSAPI_IO_BASE}/events"
            params = {"apiKey": key, "sport": "football", "from": from_time, "to": to_time, "limit": 500}
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    logger.error(f"Odds-API.io events fetch failed: {resp.status}")
                    return
                data = await resp.json()
    except Exception as e:
        logger.error(f"refresh_oddsapi_event_mapping error: {e}")
        return

    events = data if isinstance(data, list) else []
    lookup = {}
    for ev in events:
        home = _clean_name(ev.get("home"))
        away = _clean_name(ev.get("away"))
        if home and away:
            lookup[(home, away)] = ev["id"]

    rows = db_query("SELECT fixture_id, home, away FROM daily_fixtures", (), True)
    updated = 0
    for row in rows:
        fid, home_raw, away_raw = row
        if not home_raw or not away_raw:
            continue
        home_clean = _clean_name(home_raw)
        away_clean = _clean_name(away_raw)
        match_id = lookup.get((home_clean, away_clean))
        if not match_id:
            for (h, a), eid in lookup.items():
                if (home_clean in h or h in home_clean) and (away_clean in a or a in away_clean):
                    match_id = eid
                    break
        if match_id:
            db_query("UPDATE daily_fixtures SET oddsapi_event_id=? WHERE fixture_id=?", (match_id, fid))
            updated += 1
    logger.info(f"Odds-API.io mapping updated: {updated} matches")

def _clean_name(name):
    if not name:
        return ""
    return re.sub(r'\s+', ' ', re.sub(r'[^a-z0-9\s]', '', name.lower())).strip()

async def refresh_daily_fixtures():
    logger.info("Refreshing daily fixtures...")
    now = datetime.now(pytz.utc)
    from_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_time   = (now + timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{ODDSPAPI_BASE}/v4/fixtures"
    params = {"sportId": 10, "from": from_time, "to": to_time, "hasOdds": "true",
              "bookmakers": "pinnacle,singbet", "apiKey": ODDSPAPI_KEY, "language": "en"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    logger.error(f"Fixture fetch failed: {resp.status}")
                    return
                data = await resp.json()
    except Exception as e:
        logger.error(f"Fixture fetch error: {e}")
        return

    fixtures = data if isinstance(data, list) else data.get("data", [])
    db_query("DELETE FROM daily_fixtures")
    now_iso = datetime.now().isoformat()
    count = 0
    for f in fixtures:
        fid = f.get("fixtureId")
        if fid and f.get("startTime"):
            db_query("INSERT INTO daily_fixtures (fixture_id, tournament_id, start_time, recorded_at, home, away) VALUES (?,?,?,?,?,?)",
                     (fid, f.get("tournamentId", 0), f["startTime"], now_iso, f.get("participant1Name", ""), f.get("participant2Name", "")))
            count += 1
    set_setting("daily_fixtures_updated", now_iso)
    logger.info(f"Daily fixtures loaded: {count} matches")
    if get_setting("oddsapi_key"):
        await refresh_oddsapi_event_mapping()

# ─────────────────────────────────────────────────────────────
# ODDS FETCH (OddsPapi)
# ─────────────────────────────────────────────────────────────
async def fetch_odds_for_tournaments(tournament_ids, bookmaker, market_ids=None, batch_size=5):
    if not tournament_ids:
        return []
    all_fixtures = []
    for i in range(0, len(tournament_ids), batch_size):
        batch = tournament_ids[i:i+batch_size]
        ids_str = ",".join(str(t) for t in batch)
        url = f"{ODDSPAPI_BASE}/v4/odds-by-tournaments?tournamentIds={ids_str}&bookmaker={bookmaker}&language=en&verbosity=3&apiKey={ODDSPAPI_KEY}"
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
                        logger.warning(f"Odds fetch {bookmaker} batch {i//batch_size+1}: HTTP {resp.status}")
        except Exception as e:
            logger.error(f"fetch_odds_for_tournaments error: {e}")
        await asyncio.sleep(2.0)
    return all_fixtures

# ─────────────────────────────────────────────────────────────
# PARSE FIXTURE (OddsPapi) – FIXED FOR HT/CS
# ─────────────────────────────────────────────────────────────
def extract_odds_from_fixture(fixture, bookmaker, target_markets):
    home    = fixture.get("participant1Name", "")
    away    = fixture.get("participant2Name", "")
    if not home or not away:
        return
    commence = fixture.get("startTime", "")
    tid      = fixture.get("tournamentId", 0)
    tname    = fixture.get("tournamentName", "")
    fix_id   = fixture.get("fixtureId", "")
    bm_data  = fixture.get("bookmakerOdds", {}).get(bookmaker)
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
            label = outcome_label(mk_id, out_id, home, away)
            if not label:
                # For HT and CS, get the outcome name directly from the market definition
                if mk_id in (MARKET_HT_H2H, MARKET_CS):
                    outcome_info = outcomes.get(out_id_str, {})
                    label = outcome_info.get("outcomeName", "")
                    if not label:
                        continue
                else:
                    continue
            yield {
                "home": home, "away": away, "commence": commence,
                "tournament_id": tid, "tournament_name": tname,
                "market_id": mk_id, "outcome_id": out_id,
                "outcome_name": label, "odds": odds,
                "api_fixture_id": fix_id,
            }

# ─────────────────────────────────────────────────────────────
# ODDS HISTORY & GIFT SNAPSHOTS (unchanged)
# ─────────────────────────────────────────────────────────────
def save_odds_snapshot(tournament_id, home, away, market_id, outcome_id, outcome_name, odds, source="pinnacle"):
    match_key = f"{tournament_id}_{home}_{away}"
    db_query("""INSERT INTO odds_history
                (match_key,tournament_id,home,away,market_id,outcome_id,outcome_name,odds,source,recorded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
             (match_key, tournament_id, home, away, str(market_id), str(outcome_id),
              outcome_name, odds, source, datetime.now().isoformat()))

def get_odds_snapshots(match_key, market_id, outcome_id, hours_back=24, source=None):
    cutoff = (datetime.now() - timedelta(hours=hours_back)).isoformat()
    query = """SELECT odds, recorded_at FROM odds_history
               WHERE match_key=? AND market_id=? AND outcome_id=? AND recorded_at>=?"""
    params = [match_key, str(market_id), str(outcome_id), cutoff]
    if source:
        query += " AND source=?"
        params.append(source)
    query += " ORDER BY recorded_at ASC"
    return db_query(query, params, True) or []

def clean_old_odds():
    cutoff = (datetime.now() - timedelta(hours=48)).isoformat()
    db_query("DELETE FROM odds_history WHERE recorded_at<?", (cutoff,))
    db_query("DELETE FROM movement_memory WHERE snapshot_time<?", (cutoff,))
    db_query("DELETE FROM gift_snapshot WHERE recorded_at<?", (cutoff,))

def save_gift_snapshot(tournament_id, home, away, market_id, outcome_id, outcome_name, odds, source="pinnacle"):
    match_key = f"{tournament_id}_{home}_{away}"
    db_query("""INSERT INTO gift_snapshot
                (match_key,tournament_id,home,away,market_id,outcome_id,outcome_name,odds,source,recorded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
             (match_key, tournament_id, home, away, str(market_id), str(outcome_id),
              outcome_name, odds, source, datetime.now().isoformat()))

def get_gift_snapshots_past(match_key, market_id, outcome_id, hours_back=24, source=None):
    cutoff = (datetime.now() - timedelta(hours=hours_back)).isoformat()
    query = """SELECT odds, recorded_at FROM gift_snapshot
               WHERE match_key=? AND market_id=? AND outcome_id=? AND recorded_at>=?"""
    params = [match_key, str(market_id), str(outcome_id), cutoff]
    if source:
        query += " AND source=?"
        params.append(source)
    query += " ORDER BY recorded_at ASC"
    return db_query(query, params, True) or []

# ─────────────────────────────────────────────────────────────
# ODDS-API.IO HELPERS (for SingBet – kept for future toggle)
# ─────────────────────────────────────────────────────────────
async def fetch_oddsapi_odds_for_d_analysis(event_ids):
    if not event_ids: return []
    key = get_setting("oddsapi_key") or ODDSAPI_IO_KEY
    bookmaker = ODDSAPI_SINGBET_NAME
    all_data = []
    batch_size = 10
    for i in range(0, len(event_ids), batch_size):
        batch = event_ids[i:i+batch_size]
        ids_str = ",".join(str(eid) for eid in batch)
        url = f"{ODDSAPI_IO_BASE}/odds/multi?apiKey={key}&eventIds={ids_str}&bookmakers={bookmaker}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if isinstance(data, list): all_data.extend(data)
                        else: all_data.append(data)
                    else: logger.warning(f"Singbet fetch batch {i//batch_size+1} status {resp.status}")
        except Exception as e:
            logger.error(f"fetch_oddsapi_odds_for_d_analysis error: {e}")
        await asyncio.sleep(3.0)
    return all_data

def extract_singbet_d_markets(odds_data, home, away, league, commence):
    bm_data = odds_data.get("bookmakers", {}).get(ODDSAPI_SINGBET_NAME)
    if not bm_data: return
    for market in bm_data:
        name = market.get("name", "")
        odds_list = market.get("odds", [])
        if not odds_list: continue
        if name in ("ML", "1X2"):
            for odd in odds_list:
                home_odds = float(odd.get("home", 0))
                away_odds = float(odd.get("away", 0))
                draw_odds = float(odd.get("draw", 0))
                if home_odds >= 1.25:
                    yield {"home": home, "away": away, "commence": commence, "league": league,
                           "market_id": "ML_home", "outcome_id": "home", "outcome_name": "Home Win", "odds": home_odds}
                if away_odds >= 1.25:
                    yield {"home": home, "away": away, "commence": commence, "league": league,
                           "market_id": "ML_away", "outcome_id": "away", "outcome_name": "Away Win", "odds": away_odds}
                if draw_odds >= 1.25:
                    yield {"home": home, "away": away, "commence": commence, "league": league,
                           "market_id": "ML_draw", "outcome_id": "draw", "outcome_name": "Draw", "odds": draw_odds}
        elif name == "Totals":
            hdp = market.get("hdp")
            if hdp is None: continue
            for odd in odds_list:
                over_odds = float(odd.get("over", 0))
                under_odds = float(odd.get("under", 0))
                if hdp == 1.5:
                    if over_odds >= 1.25:
                        yield {"home": home, "away": away, "commence": commence, "league": league,
                               "market_id": "Totals_1.5_over", "outcome_id": "over", "outcome_name": "Over 1.5", "odds": over_odds}
                    if under_odds >= 1.25:
                        yield {"home": home, "away": away, "commence": commence, "league": league,
                               "market_id": "Totals_1.5_under", "outcome_id": "under", "outcome_name": "Under 1.5", "odds": under_odds}
                elif hdp == 2.5:
                    if over_odds >= 1.25:
                        yield {"home": home, "away": away, "commence": commence, "league": league,
                               "market_id": "Totals_2.5_over", "outcome_id": "over", "outcome_name": "Over 2.5", "odds": over_odds}
                    if under_odds >= 1.25:
                        yield {"home": home, "away": away, "commence": commence, "league": league,
                               "market_id": "Totals_2.5_under", "outcome_id": "under", "outcome_name": "Under 2.5", "odds": under_odds}
        elif name == "Both Teams To Score":
            for odd in odds_list:
                yes_odds = float(odd.get("yes", 0))
                no_odds = float(odd.get("no", 0))
                if yes_odds >= 1.25:
                    yield {"home": home, "away": away, "commence": commence, "league": league,
                           "market_id": "BTTS_yes", "outcome_id": "yes", "outcome_name": "BTTS Yes", "odds": yes_odds}
                if no_odds >= 1.25:
                    yield {"home": home, "away": away, "commence": commence, "league": league,
                           "market_id": "BTTS_no", "outcome_id": "no", "outcome_name": "BTTS No", "odds": no_odds}

# ─────────────────────────────────────────────────────────────
# D ANALYSIS – Pinnacle (initial odds ≤ 1.80)
# ─────────────────────────────────────────────────────────────
def get_higher_prob_outcome(outcomes_list):
    if not outcomes_list: return None
    return min(outcomes_list, key=lambda x: x["odds"])

async def run_d_analysis_pinnacle(fixtures):
    for fixture in fixtures:
        for cand in extract_odds_from_fixture(fixture, "pinnacle", D_MARKETS):
            save_odds_snapshot(cand["tournament_id"], cand["home"], cand["away"],
                               cand["market_id"], cand["outcome_id"], cand["outcome_name"], cand["odds"],
                               source="pinnacle")
    drop_signals = []
    match_market_groups = {}
    for fixture in fixtures:
        for cand in extract_odds_from_fixture(fixture, "pinnacle", D_MARKETS):
            mins = mins_to_kickoff(cand["commence"])
            if mins <= 0 or mins > 720: continue
            if cand["odds"] < 1.25: continue
            if any(k in cand["tournament_name"].lower() for k in
                   ["women","female","u17","u18","u19","u20","u21","u23","youth","virtual","esoccer","simulated","esports","indoor","futsal","srl","reserve"]): continue
            key = (f"{cand['tournament_id']}_{cand['home']}_{cand['away']}", cand["market_id"])
            match_market_groups.setdefault(key, []).append(cand)
    for (match_key, mk_id), cands in match_market_groups.items():
        fav = get_higher_prob_outcome(cands)
        if not fav: continue
        if mk_id == MARKET_H2H and fav["outcome_name"] == "Draw": continue
        history = get_odds_snapshots(match_key, mk_id, fav["outcome_id"], source="pinnacle")
        if len(history) < 2: continue
        original_odds = history[0][0]
        current_odds = fav["odds"]
        if original_odds <= 0 or original_odds > 1.81 or current_odds < 1.25: continue
        drop_pct = ((original_odds - current_odds) / original_odds) * 100
        if drop_pct <= 0: continue
        drop_signals.append({
            "match": f"{fav['home']} vs {fav['away']}",
            "league": fav["tournament_name"],
            "market": fav["outcome_name"],
            "outcome": fav["outcome_id"],
            "original_odds": round(original_odds, 2),
            "current_odds": round(current_odds, 2),
            "drop_pct": round(drop_pct, 2),
            "confidence": 50,
            "confidence_label": "⚡ MEDIUM",
            "minutes_to_kickoff": mins_to_kickoff(fav["commence"]),
            "commence_time": fav["commence"],
            "api_fixture_id": fav["api_fixture_id"],
            "source": "pinnacle",
        })
    seen, unique = set(), []
    for s in drop_signals:
        k = s["match"]
        if k not in seen:
            seen.add(k); unique.append(s)
    unique.sort(key=lambda x: x["drop_pct"], reverse=True)
    return unique

# ─────────────────────────────────────────────────────────────
# D ANALYSIS – SingBet (same filter, source='singbet')
# ─────────────────────────────────────────────────────────────
async def run_d_analysis_singbet_oddspapi(candidates):
    for cand in candidates:
        save_odds_snapshot(cand["tournament_id"], cand["home"], cand["away"],
                           cand["market_id"], cand["outcome_id"], cand["outcome_name"], cand["odds"],
                           source="singbet")
    drop_signals = []
    match_market_groups = {}
    for cand in candidates:
        mins = mins_to_kickoff(cand["commence"])
        if mins <= 0 or mins > 720: continue
        if cand["odds"] < 1.25: continue
        if any(k in cand.get("league", cand.get("tournament_name", "")).lower() for k in
               ["women","female","u17","u18","u19","u20","u21","u23","youth","virtual","esoccer","simulated","esports","indoor","futsal","srl","reserve"]): continue
        key = (f"{cand['tournament_id']}_{cand['home']}_{cand['away']}", cand["market_id"])
        match_market_groups.setdefault(key, []).append(cand)
    for (match_key, mk_id), cands in match_market_groups.items():
        fav = get_higher_prob_outcome(cands)
        if not fav: continue
        if mk_id == MARKET_H2H and fav["outcome_name"] == "Draw": continue
        history = get_odds_snapshots(match_key, mk_id, fav["outcome_id"], source="singbet")
        if len(history) < 2: continue
        original_odds = history[0][0]
        current_odds = fav["odds"]
        if original_odds <= 0 or original_odds > 1.81 or current_odds < 1.25: continue
        drop_pct = ((original_odds - current_odds) / original_odds) * 100
        if drop_pct <= 0: continue
        drop_signals.append({
            "match": f"{fav['home']} vs {fav['away']}",
            "league": fav.get("league", fav.get("tournament_name", "")),
            "market": fav["outcome_name"],
            "outcome": fav["outcome_id"],
            "original_odds": round(original_odds, 2),
            "current_odds": round(current_odds, 2),
            "drop_pct": round(drop_pct, 2),
            "confidence": 50,
            "confidence_label": "⚡ MEDIUM",
            "minutes_to_kickoff": mins_to_kickoff(fav["commence"]),
            "commence_time": fav["commence"],
            "api_fixture_id": fav.get("api_fixture_id", ""),
            "source": "singbet",
        })
    seen, unique = set(), []
    for s in drop_signals:
        k = s["match"]
        if k not in seen:
            seen.add(k); unique.append(s)
    unique.sort(key=lambda x: x["drop_pct"], reverse=True)
    return unique

async def run_d_analysis_singbet(singbet_fixtures):
    # odds-api.io version (kept for toggle)
    candidates_all = []
    for fix in singbet_fixtures:
        odds_data = fix["odds_data"]
        home = fix["home"]
        away = fix["away"]
        league = fix["league"]
        commence = fix["start_time"]
        tid = fix.get("tournament_id", 0)
        for cand in extract_singbet_d_markets(odds_data, home, away, league, commence):
            mins = mins_to_kickoff(cand["commence"])
            if mins <= 0 or mins > 720: continue
            if any(k in (league or "").lower() for k in
                   ["women","female","u17","u18","u19","u20","u21","u23","youth","virtual","esoccer","simulated","esports","indoor","futsal","srl","reserve"]): continue
            save_odds_snapshot(tid, home, away, cand["market_id"], cand["outcome_id"], cand["outcome_name"], cand["odds"], source="singbet")
            candidates_all.append({**cand, "tournament_id": tid})
    match_market_groups = {}
    for cand in candidates_all:
        generic_market = cand["market_id"].rsplit("_", 1)[0]
        key = (f"{cand['tournament_id']}_{cand['home']}_{cand['away']}", generic_market)
        match_market_groups.setdefault(key, []).append(cand)
    drop_signals = []
    for (match_key, mk_id), cands in match_market_groups.items():
        fav = get_higher_prob_outcome(cands)
        if not fav: continue
        if "draw" in fav["outcome_name"].lower(): continue
        history = get_odds_snapshots(match_key, fav["market_id"], fav["outcome_id"], source="singbet")
        if len(history) < 2: continue
        original_odds = history[0][0]
        current_odds = fav["odds"]
        if original_odds <= 0 or original_odds > 1.81 or current_odds < 1.25: continue
        drop_pct = ((original_odds - current_odds) / original_odds) * 100
        if drop_pct <= 0: continue
        drop_signals.append({
            "match": f"{fav['home']} vs {fav['away']}",
            "league": fav["league"],
            "market": fav["outcome_name"],
            "outcome": fav["outcome_id"],
            "original_odds": round(original_odds, 2),
            "current_odds": round(current_odds, 2),
            "drop_pct": round(drop_pct, 2),
            "confidence": 50,
            "confidence_label": "⚡ MEDIUM",
            "minutes_to_kickoff": mins_to_kickoff(fav["commence"]),
            "commence_time": fav["commence"],
            "source": "singbet",
        })
    seen, unique = set(), []
    for s in drop_signals:
        k = s["match"]
        if k not in seen:
            seen.add(k); unique.append(s)
    unique.sort(key=lambda x: x["drop_pct"], reverse=True)
    return unique

# ─────────────────────────────────────────────────────────────
# GIFT DRAW ANALYSIS – Pinnacle FT (≤4.0 final odds)
# ─────────────────────────────────────────────────────────────
async def run_gift_d_analysis_pinnacle(fixtures):
    for fixture in fixtures:
        for cand in extract_odds_from_fixture(fixture, "pinnacle", GIFT_D_MARKETS):
            save_gift_snapshot(cand["tournament_id"], cand["home"], cand["away"],
                               cand["market_id"], cand["outcome_id"], cand["outcome_name"], cand["odds"],
                               source="pinnacle")
    ft_draws = []
    for fixture in fixtures:
        for cand in extract_odds_from_fixture(fixture, "pinnacle", GIFT_D_MARKETS):
            mins = mins_to_kickoff(cand["commence"])
            if mins <= 0 or mins > 720: continue
            if cand["odds"] < 1.25: continue
            if any(k in cand["tournament_name"].lower() for k in
                   ["women","female","u17","u18","u19","u20","u21","u23","youth","virtual","esoccer","simulated","esports","indoor","futsal","srl","reserve"]): continue
            if cand["market_id"] != MARKET_H2H or cand["outcome_name"] != "Draw": continue
            match_key = f"{cand['tournament_id']}_{cand['home']}_{cand['away']}"
            past = get_gift_snapshots_past(match_key, MARKET_H2H, cand["outcome_id"], source="pinnacle")
            if not past: continue
            original_odds = past[0][0]
            current_odds = cand["odds"]
            if original_odds <= 0 or original_odds > 4.0: continue   # initial odds ≤ 4.0
            drop_pct = ((original_odds - current_odds) / original_odds) * 100
            if drop_pct < 0.5: continue
            if current_odds > 4.0: continue
            ft_draws.append({
                "match": f"{cand['home']} vs {cand['away']}",
                "league": cand["tournament_name"],
                "prediction": "Draw",
                "odds": round(current_odds, 2),
                "original_odds": round(original_odds, 2),
                "drop_pct": round(drop_pct, 2),
                "confidence": min(int(drop_pct * 5), 90),
                "commence_time": cand["commence"],
                "source": "pinnacle",
            })
    ft_draws.sort(key=lambda x: x["drop_pct"], reverse=True)
    return ft_draws

# ─────────────────────────────────────────────────────────────
# GIFT DRAW ANALYSIS – SingBet (OddsPapi version)
# ─────────────────────────────────────────────────────────────
async def run_gift_d_analysis_singbet_oddspapi(candidates):
    for cand in candidates:
        save_gift_snapshot(cand["tournament_id"], cand["home"], cand["away"],
                           cand["market_id"], cand["outcome_id"], cand["outcome_name"], cand["odds"],
                           source="singbet")
    ft_draws = []
    for cand in candidates:
        mins = mins_to_kickoff(cand["commence"])
        if mins <= 0 or mins > 720: continue
        if cand["odds"] < 1.25: continue
        league = cand.get("league", cand.get("tournament_name", ""))
        if any(k in league.lower() for k in
               ["women","female","u17","u18","u19","u20","u21","u23","youth","virtual","esoccer","simulated","esports","indoor","futsal","srl","reserve"]): continue
        if cand["market_id"] != MARKET_H2H or cand["outcome_name"] != "Draw": continue
        match_key = f"{cand['tournament_id']}_{cand['home']}_{cand['away']}"
        past = get_gift_snapshots_past(match_key, MARKET_H2H, cand["outcome_id"], source="singbet")
        if not past: continue
        original_odds = past[0][0]
        current_odds = cand["odds"]
        if original_odds <= 0 or original_odds > 4.0: continue
        drop_pct = ((original_odds - current_odds) / original_odds) * 100
        if drop_pct < 0.5: continue
        if current_odds > 4.0: continue
        ft_draws.append({
            "match": f"{cand['home']} vs {cand['away']}",
            "league": league,
            "prediction": "Draw",
            "odds": round(current_odds, 2),
            "original_odds": round(original_odds, 2),
            "drop_pct": round(drop_pct, 2),
            "confidence": min(int(drop_pct * 5), 90),
            "commence_time": cand["commence"],
            "source": "singbet",
        })
    ft_draws.sort(key=lambda x: x["drop_pct"], reverse=True)
    return ft_draws

async def run_gift_d_analysis_singbet(singbet_fixtures):
    # odds-api.io version (kept)
    draws = []
    for fix in singbet_fixtures:
        odds_data = fix["odds_data"]
        home = fix["home"]
        away = fix["away"]
        league = fix["league"]
        commence = fix["start_time"]
        tid = fix.get("tournament_id", 0)
        bm_data = odds_data.get("bookmakers", {}).get(ODDSAPI_SINGBET_NAME)
        if not bm_data: continue
        for market in bm_data:
            if market.get("name") not in ("ML", "1X2"): continue
            for odd in market.get("odds", []):
                draw_odds = float(odd.get("draw", 0))
                if draw_odds < 1.25: continue
                mins = mins_to_kickoff(commence)
                if mins <= 0 or mins > 720: continue
                if any(k in (league or "").lower() for k in
                       ["women","female","u17","u18","u19","u20","u21","u23","youth","virtual","esoccer","simulated","esports","indoor","futsal","srl","reserve"]): continue
                match_key = f"{tid}_{home}_{away}"
                save_gift_snapshot(tid, home, away, "ML_draw", "draw", "Draw", draw_odds, source="singbet")
                past = get_gift_snapshots_past(match_key, "ML_draw", "draw", source="singbet")
                if not past: continue
                original_odds = past[0][0]
                drop_pct = ((original_odds - draw_odds) / original_odds) * 100
                if drop_pct < 0.5: continue
                if draw_odds > 4.0: continue
                draws.append({
                    "match": f"{home} vs {away}",
                    "league": league,
                    "prediction": "Draw",
                    "odds": round(draw_odds, 2),
                    "original_odds": round(original_odds, 2),
                    "drop_pct": round(drop_pct, 2),
                    "confidence": min(int(drop_pct * 5), 90),
                    "commence_time": commence,
                    "source": "singbet",
                })
    draws.sort(key=lambda x: x["drop_pct"], reverse=True)
    return draws

# ─────────────────────────────────────────────────────────────
# HT DRAW ANALYSIS – Pinnacle only (initial odds ≤ 2.2)
# ─────────────────────────────────────────────────────────────
async def run_ht_draw_analysis_pinnacle(fixtures):
    ht_market = MARKET_HT_H2H
    for fixture in fixtures:
        for cand in extract_odds_from_fixture(fixture, "pinnacle", [ht_market]):
            # The label for HT draw is "X" (as in 1X2)
            if cand["outcome_name"] not in ("Draw", "X"):
                continue
            save_gift_snapshot(cand["tournament_id"], cand["home"], cand["away"],
                               cand["market_id"], cand["outcome_id"], cand["outcome_name"], cand["odds"],
                               source="pinnacle")
    ht_draws = []
    for fixture in fixtures:
        for cand in extract_odds_from_fixture(fixture, "pinnacle", [ht_market]):
            if cand["outcome_name"] not in ("Draw", "X"):
                continue
            mins = mins_to_kickoff(cand["commence"])
            if mins <= 0 or mins > 720: continue
            if cand["odds"] < 1.25: continue
            if any(k in cand["tournament_name"].lower() for k in
                   ["women","female","u17","u18","u19","u20","u21","u23","youth","virtual","esoccer","simulated","esports","indoor","futsal","srl","reserve"]): continue
            match_key = f"{cand['tournament_id']}_{cand['home']}_{cand['away']}"
            past = get_gift_snapshots_past(match_key, ht_market, cand["outcome_id"], source="pinnacle")
            if not past: continue
            original_odds = past[0][0]
            current_odds = cand["odds"]
            if original_odds <= 0 or original_odds > 2.2: continue   # initial odds ≤ 2.2
            drop_pct = ((original_odds - current_odds) / original_odds) * 100
            if drop_pct < 0.5: continue
            if current_odds > 4.0: continue
            ht_draws.append({
                "match": f"{cand['home']} vs {cand['away']}",
                "league": cand["tournament_name"],
                "prediction": "HT Draw",
                "odds": round(current_odds, 2),
                "original_odds": round(original_odds, 2),
                "drop_pct": round(drop_pct, 2),
                "confidence": min(int(drop_pct * 5), 90),
                "commence_time": cand["commence"],
                "source": "pinnacle",
            })
    ht_draws.sort(key=lambda x: x["drop_pct"], reverse=True)
    return ht_draws

# ─────────────────────────────────────────────────────────────
# CORRECT SCORE ANALYSIS – Pinnacle only (allowed scores, no odds limits)
# ─────────────────────────────────────────────────────────────
async def run_cs_analysis_pinnacle(fixtures):
    cs_market = MARKET_CS
    for fixture in fixtures:
        for cand in extract_odds_from_fixture(fixture, "pinnacle", [cs_market]):
            if cand["outcome_name"] not in ALLOWED_CS:
                continue
            save_gift_snapshot(cand["tournament_id"], cand["home"], cand["away"],
                               cand["market_id"], cand["outcome_id"], cand["outcome_name"], cand["odds"],
                               source="pinnacle")
    cs_drops = []
    for fixture in fixtures:
        for cand in extract_odds_from_fixture(fixture, "pinnacle", [cs_market]):
            if cand["outcome_name"] not in ALLOWED_CS:
                continue
            mins = mins_to_kickoff(cand["commence"])
            if mins <= 0 or mins > 720: continue
            if any(k in cand["tournament_name"].lower() for k in
                   ["women","female","u17","u18","u19","u20","u21","u23","youth","virtual","esoccer","simulated","esports","indoor","futsal","srl","reserve"]): continue
            match_key = f"{cand['tournament_id']}_{cand['home']}_{cand['away']}"
            past = get_gift_snapshots_past(match_key, cs_market, cand["outcome_id"], source="pinnacle")
            if not past: continue
            original_odds = past[0][0]
            current_odds = cand["odds"]
            if original_odds <= 0: continue
            drop_pct = ((original_odds - current_odds) / original_odds) * 100
            if drop_pct < 0.5: continue
            cs_drops.append({
                "match": f"{cand['home']} vs {cand['away']}",
                "league": cand["tournament_name"],
                "prediction": cand["outcome_name"],
                "odds": round(current_odds, 2),
                "original_odds": round(original_odds, 2),
                "drop_pct": round(drop_pct, 2),
                "confidence": min(int(drop_pct * 5), 90),
                "commence_time": cand["commence"],
                "source": "pinnacle",
            })
    cs_drops.sort(key=lambda x: x["drop_pct"], reverse=True)
    return cs_drops

# ─────────────────────────────────────────────────────────────
# FORMATTING (with emojis)
# ─────────────────────────────────────────────────────────────
def format_d_signal(signal, rank=1):
    rank_emoji = ["🥇", "🥈"][rank - 1] if rank <= 2 else "🔥"
    drop = signal.get("drop_pct", 0)
    mins = signal.get("minutes_to_kickoff", 0)
    h, m = divmod(mins, 60)
    time_str = f"{h}h {m}m" if h else f"{m}m"
    try:
        dt = datetime.fromisoformat(signal.get("commence_time", "").replace("Z", "+00:00"))
        kickoff = dt.astimezone(LAGOS).strftime("%d %b • %H:%M WAT")
    except:
        kickoff = "TBC"
    source_emoji = {"pinnacle": "🅿️", "singbet": "🅱"}.get(signal.get("source"), "")
    return (
        f"{rank_emoji} *D ANALYSIS — SIGNAL #{rank}* {source_emoji}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚽ *{signal['match']}*\n"
        f"🏆 `{signal['league']}`\n\n"
        f"📌 *Prediction:* `{signal['market']}`\n"
        f"🎯 *Confidence:* `{signal['confidence']}%`\n"
        f"⚡ *Signal Strength:* {signal['confidence_label']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📉 Odds: `{signal['original_odds']}` → `{signal['current_odds']}` (*↓{drop:.1f}%*)\n"
        f"⏱ Kickoff in `{time_str}` ({kickoff})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 @devoulbot"
    )

def format_gift(gift, gift_type="draw"):
    if gift_type == "ht":
        label = "🕐 HT DRAW GIFT"
    elif gift_type == "cs":
        label = "🎯 CORRECT SCORE GIFT"
    else:
        label = "🤝 FT DRAW GIFT"
    pred = gift.get("prediction", "Draw")
    try:
        dt = datetime.fromisoformat(gift.get("commence_time", "").replace("Z", "+00:00"))
        t_str = dt.astimezone(LAGOS).strftime("%d %b • %H:%M WAT")
    except:
        t_str = "TBC"
    source_emoji = {"pinnacle": "🅿️", "singbet": "🅱️"}.get(gift.get("source"), "")
    return (
        f"🎁 *{label}* {source_emoji}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚽ *{gift.get('match', '')}*\n"
        f"🏆 `{gift.get('league', '')}`\n\n"
        f"🔮 *Prediction:* `{pred}`\n"
        f"💎 *Odds:* `{gift.get('odds', gift.get('original_odds', 'N/A'))}`\n"
        f"📊 *Confidence:* `{gift.get('confidence', 0)}%`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📉 Drop: `{gift.get('drop_pct', '')}%`\n"
        f"🕐 {t_str}\n\n"
        f"⚠️ _Bet responsibly_ | 🤖 @devoulbot"
    )

def format_v_signal(signal, rank=1):
    return "📊 *V Analysis*\n🔜 Coming soon…"

# ─────────────────────────────────────────────────────────────
# COMBINED ANALYSIS JOB (with correct logic for scan types)
# ─────────────────────────────────────────────────────────────
async def combined_analysis_job(bot, post_d=True, post_v=True, post_gift=False, target_chat_id=None):
    if is_paused():
        logger.info("System paused — skipping job")
        return

    now = lagos_time()
    time_label = now.strftime("%d %b • %H:%M WAT")
    logger.info(f"Analysis job at {time_label} | D={post_d} V={post_v} Gift={post_gift}")

    d_chat = target_chat_id if target_chat_id else D_CHANNEL_ID
    v_chat = target_chat_id if target_chat_id else V_CHANNEL_ID

    running_msg = (f"🔄 *Running Analysis...*\n🕐 `{time_label}`\n\n"
                   f"📡 Scanning today's fixtures...\n_Results may take a few minutes_")
    if post_d and not target_chat_id:
        try: await bot.send_message(d_chat, running_msg, parse_mode="Markdown")
        except: pass

    clean_old_odds()

    last_fixture_upd = get_setting("daily_fixtures_updated") or "0"
    try:
        needs_fixture_refresh = (datetime.now() - datetime.fromisoformat(last_fixture_upd)).total_seconds() > 43200
    except:
        needs_fixture_refresh = True
    if needs_fixture_refresh:
        await refresh_daily_fixtures()

    tournament_ids = list(set(r[0] for r in db_query("SELECT tournament_id FROM daily_fixtures", (), True)))
    logger.info(f"Today's tournaments: {len(tournament_ids)}")

    # Pinnacle odds
    pinnacle_markets = list(set(D_MARKETS + GIFT_D_MARKETS + [MARKET_HT_H2H, MARKET_CS]))
    d_fixtures = await fetch_odds_for_tournaments(tournament_ids, "pinnacle", pinnacle_markets) if (post_d or post_gift) else []
    logger.info(f"Fixtures fetched (Pinnacle): {len(d_fixtures)}")

    singbet_source = get_singbet_source()
    logger.info(f"SingBet source: {singbet_source}")

    singbet_signals = []
    singbet_draws = []
    if singbet_source == "oddspapi":
        singbet_candidates = []
        singbet_draw_candidates = []
        if post_d or post_gift:
            singbet_markets = list(set(D_MARKETS + GIFT_D_MARKETS))
            singbet_fixtures = await fetch_odds_for_tournaments(tournament_ids, "singbet", singbet_markets)
            logger.info(f"SingBet fixtures fetched (OddsPapi): {len(singbet_fixtures)}")
            for fixture in singbet_fixtures:
                for cand in extract_odds_from_fixture(fixture, "singbet", D_MARKETS):
                    mins = mins_to_kickoff(cand["commence"])
                    if mins <= 0 or mins > 720: continue
                    if any(k in cand["tournament_name"].lower() for k in
                           ["women","female","u17","u18","u19","u20","u21","u23","youth",
                            "virtual","esoccer","simulated","esports","indoor","futsal","srl","reserve"]): continue
                    singbet_candidates.append(cand)
                for cand in extract_odds_from_fixture(fixture, "singbet", GIFT_D_MARKETS):
                    mins = mins_to_kickoff(cand["commence"])
                    if mins <= 0 or mins > 720: continue
                    if any(k in cand["tournament_name"].lower() for k in
                           ["women","female","u17","u18","u19","u20","u21","u23","youth",
                            "virtual","esoccer","simulated","esports","indoor","futsal","srl","reserve"]): continue
                    singbet_draw_candidates.append(cand)
            logger.info(f"SingBet D candidates: {len(singbet_candidates)}, Draw candidates: {len(singbet_draw_candidates)}")
        singbet_signals = await run_d_analysis_singbet_oddspapi(singbet_candidates) if singbet_candidates else []
        singbet_draws = await run_gift_d_analysis_singbet_oddspapi(singbet_draw_candidates) if singbet_draw_candidates else []
    else:
        singbet_fixtures = []
        if get_setting("oddsapi_key"):
            rows = db_query("SELECT oddsapi_event_id, home, away, start_time, tournament_id FROM daily_fixtures WHERE oddsapi_event_id IS NOT NULL", (), True)
            if rows:
                event_ids = [r[0] for r in rows if r[0]]
                logger.info(f"Fetching SingBet odds for {len(event_ids)} events...")
                singbet_odds_data = await fetch_oddsapi_odds_for_d_analysis(event_ids)
                odds_lookup = {od.get("id"): od for od in singbet_odds_data}
                for row in rows:
                    eid, home, away, start, tid = row
                    if int(eid) in odds_lookup:
                        league = odds_lookup[int(eid)].get("league", {}).get("name", "")
                        singbet_fixtures.append({
                            "oddsapi_event_id": eid, "home": home, "away": away,
                            "start_time": start, "tournament_id": tid,
                            "league": league, "odds_data": odds_lookup[int(eid)]
                        })
                logger.info(f"SingBet fixtures built (odds-api.io): {len(singbet_fixtures)}")
        singbet_signals = await run_d_analysis_singbet(singbet_fixtures) if singbet_fixtures else []
        singbet_draws = await run_gift_d_analysis_singbet(singbet_fixtures) if singbet_fixtures else []

    # Pinnacle analysis
    pinnacle_signals = await run_d_analysis_pinnacle(d_fixtures) if d_fixtures else []
    pinnacle_ft_draws = await run_gift_d_analysis_pinnacle(d_fixtures) if d_fixtures else []
    ht_draws = await run_ht_draw_analysis_pinnacle(d_fixtures) if d_fixtures else []
    cs_drops = await run_cs_analysis_pinnacle(d_fixtures) if d_fixtures else []

    # Combine D signals
    combined_d = list(pinnacle_signals) + list(singbet_signals)
    dedup_d = {}
    for sig in combined_d:
        match = sig["match"]
        if match not in dedup_d or sig["drop_pct"] > dedup_d[match]["drop_pct"]:
            dedup_d[match] = sig
    d_signals_full = sorted(dedup_d.values(), key=lambda x: x["drop_pct"], reverse=True)

    # All FT draws (both bookmakers)
    all_ft_draws = list(pinnacle_ft_draws) + list(singbet_draws)
    dedup_ft = {}
    for draw in all_ft_draws:
        match = draw["match"]
        if match not in dedup_ft or draw["drop_pct"] > dedup_ft[match]["drop_pct"]:
            dedup_ft[match] = draw
    ft_draws_sorted = sorted(dedup_ft.values(), key=lambda x: x["drop_pct"], reverse=True)

    # Cache
    db_query("INSERT INTO d_analysis_cache (data, gift_data, ht_draw_data, cs_data, cached_at) VALUES (?,?,?,?,?)",
             (json.dumps(d_signals_full), json.dumps(ft_draws_sorted),
              json.dumps(ht_draws), json.dumps(cs_drops), now.isoformat()))
    db_query("DELETE FROM d_analysis_cache WHERE id NOT IN (SELECT id FROM d_analysis_cache ORDER BY id DESC LIMIT 50)")

    # Post D signals to channel if needed
    if post_d and not target_chat_id:
        d_post = d_signals_full[:2]
        if d_post:
            try:
                await bot.send_message(d_chat,
                    f"📉 *D ANALYSIS*\n🕐 `{time_label}`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode="Markdown")
                for i, sig in enumerate(d_post, 1):
                    await bot.send_message(d_chat, format_d_signal(sig, i), parse_mode="Markdown")
                    db_query("""INSERT INTO signal_results
                                (signal_type,match,league,market,prediction,confidence,odds,api_fixture_id,recorded_at)
                                VALUES (?,?,?,?,?,?,?,?,?)""",
                             ("D", sig["match"], sig["league"], sig["market"],
                              str(sig["outcome"]), sig["confidence"], sig["current_odds"],
                              sig.get("api_fixture_id", ""), now.isoformat()))
                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"D post error: {e}")
        else:
            try:
                await bot.send_message(d_chat,
                    f"📉 *D ANALYSIS*\n🕐 `{time_label}`\n\n📭 No qualifying signals.\n🔄 Next analysis soon.",
                    parse_mode="Markdown")
            except: pass

    # Gift posting
    if post_gift:
        if target_chat_id:
            # Private gift scan: SingBet draws first, then Pinnacle buttons
            singbet_draw_list = singbet_draws[:2]
            if singbet_draw_list:
                await bot.send_message(target_chat_id,
                    "🎁 *GIFT PACK*\n🅱️ *SingBet dropping odds*",
                    parse_mode="Markdown")
                for g in singbet_draw_list:
                    await bot.send_message(target_chat_id, format_gift(g, "draw"), parse_mode="Markdown")
            else:
                await bot.send_message(target_chat_id, "🎁 *GIFT PACK*\n🅱️ No SingBet draws found.",
                                       parse_mode="Markdown")
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🎁 FT X", callback_data="pinnacle_ft"),
                 InlineKeyboardButton("🕐 HT X", callback_data="pinnacle_ht"),
                 InlineKeyboardButton("🎯 CS", callback_data="pinnacle_cs")]
            ])
            await bot.send_message(target_chat_id,
                "🅿️ *Pinnacle dropping odds*\n👇 Select a category:",
                parse_mode="Markdown", reply_markup=kb)
        else:
            # Channel gift: three top picks
            top_ft = ft_draws_sorted[0] if ft_draws_sorted else None
            top_ht = ht_draws[0] if ht_draws else None
            top_cs = cs_drops[0] if cs_drops else None
            msg = f"🎁 *GIFT PACK*\n🕐 `{time_label}`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            if top_ft:
                msg += f"\n🤝 *FT Draw:* {top_ft['match']} 🅿️\n🏆 {top_ft['league']}\n📉 Drop: {top_ft['drop_pct']}% | Odds: {top_ft['odds']}\n"
            if top_ht:
                msg += f"\n🕐 *HT Draw:* {top_ht['match']} 🅿️\n🏆 {top_ht['league']}\n📉 Drop: {top_ht['drop_pct']}% | Odds: {top_ht['odds']}\n"
            if top_cs:
                msg += f"\n🎯 *Correct Score:* {top_cs['match']} 🅿️\n🏆 {top_cs['league']}\n🔮 {top_cs['prediction']}\n📉 Drop: {top_cs['drop_pct']}% | Odds: {top_cs['odds']}\n"
            msg += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n⚠️ _Bet responsibly_ | 🤖 @devoulbot"
            try:
                await bot.send_message(d_chat, msg, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Gift post error: {e}")

# ─────────────────────────────────────────────────────────────
# BOT HANDLERS (existing, with AI ACCA button & toggle)
# ─────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    name = update.effective_user.first_name or "Champ"
    upsert_user(uid)
    u = get_user(uid)
    if u and u[5] == 1:
        await update.message.reply_text("🚫 You have been banned."); return
    if is_paused() and not is_admin(uid):
        await update.message.reply_text("🔧 *System paused for maintenance.*\n\nBack shortly!", parse_mode="Markdown"); return
    if is_admin(uid) or user_has_trial(uid) or user_has_d_access(uid):
        await update.message.reply_text(
            f"👑 *Welcome back, {name}!*\n\n"
            f"🔮 *Devoul* — Mathematical Betting Intelligence\n\n"
            f"📉 *D Analysis* — Sharp market drops\n"
            f"🎁 *Gifts* — FT Draw, HT Draw & Correct Score\n"
            f"📊 *V Analysis* — Coming soon\n\n"
            f"_Use the buttons below_ 👇",
            parse_mode="Markdown", reply_markup=main_menu_kb(uid))
    else:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Join Free Channel", url=FREE_CHANNEL_LINK)],
            [InlineKeyboardButton("✅ I've Joined — Start Free Week!", callback_data="check_join")]])
        await update.message.reply_text(
            f"🔮 *Welcome to Devoul, {name}!*\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ Mathematical betting intelligence\n\n"
            f"📉 *D Analysis* — Sharp market drops (all leagues)\n"
            f"🎁 *Free gifts* — FT Draw, HT Draw & Correct Score\n"
            f"🧮 *100% Mathematical* — Pure data, no guessing\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎁 *Join our channel for a FREE 7-day trial!*\n\n"
            f"👇 Join then tap activate:",
            parse_mode="Markdown", reply_markup=kb)

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message: return
    uid = update.effective_user.id
    text = update.message.text
    upsert_user(uid)
    u = get_user(uid)
    if u and u[5] == 1:
        await update.message.reply_text("🚫 You have been banned."); return
    if is_paused() and not is_admin(uid):
        await update.message.reply_text("🔧 *System paused.*", parse_mode="Markdown"); return

    no_access = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Join Free Channel", url=FREE_CHANNEL_LINK)],
        [InlineKeyboardButton("✅ I've Joined!", callback_data="check_join")]])

    # Secret multi-drop handling (D analysis N, gift pack N)
    d_match = re.match(r'📉 D Analysis (\d+)', text)
    if d_match:
        n = int(d_match.group(1))
        if not user_has_d_access(uid):
            await update.message.reply_text("🔒 *Premium feature.* Join the free channel or purchase access.",
                                            parse_mode="Markdown", reply_markup=no_access)
            return
        cache = db_query("SELECT data FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache or not cache[0][0]:
            await update.message.reply_text("📉 No analysis cached yet."); return
        sigs = json.loads(cache[0][0])
        if n < 1 or n > len(sigs):
            await update.message.reply_text(f"📉 Only {len(sigs)} signals available."); return
        await update.message.reply_text(format_d_signal(sigs[n-1], n), parse_mode="Markdown")
        return

    gift_match = re.match(r'🎁 GIFT PACK (\d+)', text)
    if gift_match:
        n = int(gift_match.group(1))
        if not user_has_d_access(uid):
            await update.message.reply_text("🔒 *Premium feature.* Join the free channel or purchase access.",
                                            parse_mode="Markdown", reply_markup=no_access)
            return
        cache = db_query("SELECT gift_data, ht_draw_data, cs_data FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache:
            await update.message.reply_text("🎁 No gift data available."); return
        ft_draws = json.loads(cache[0][0]) if cache[0][0] else []
        ht_draws = json.loads(cache[0][1]) if cache[0][1] else []
        cs_drops = json.loads(cache[0][2]) if cache[0][2] else []
        if n <= len(ft_draws):
            await update.message.reply_text(format_gift(ft_draws[n-1], "draw"), parse_mode="Markdown")
        elif n <= len(ft_draws) + len(ht_draws):
            idx = n - len(ft_draws) - 1
            await update.message.reply_text(format_gift(ht_draws[idx], "ht"), parse_mode="Markdown")
        elif n <= len(ft_draws) + len(ht_draws) + len(cs_drops):
            idx = n - len(ft_draws) - len(ht_draws) - 1
            await update.message.reply_text(format_gift(cs_drops[idx], "cs"), parse_mode="Markdown")
        else:
            await update.message.reply_text("🎁 Invalid gift number.")
        return

    # NEW: AI ACCA button
    if text == "🤖 AI ACCA":
        if not (is_admin(uid) or ai_buttons_public):
            await update.message.reply_text("⛔ *Access Denied.*", parse_mode="Markdown")
            return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🧠 Gemini 3.5 Flash", callback_data="ai_gemini"),
             InlineKeyboardButton("🔮 DeepSeek V4 Pro", callback_data="ai_deepseek")]
        ])
        await update.message.reply_text(
            "🤖 *AI ACCA Generator*\n\n"
            "Choose a model to analyze the latest 3‑scan dropping odds data.\n\n"
            "⚠️ This may take a few seconds.",
            parse_mode="Markdown", reply_markup=kb
        )
        return

    # Standard button handling (unchanged)
    if text in ["📉 D Analysis", "📉 D Analysis 🔒"]:
        if not user_has_d_access(uid):
            p = get_d_price()
            if p > 0:
                await update.message.reply_text(
                    f"🔒 *D Analysis — Premium*\n\nUnlock sharp market drop signals.\n\n💎 *Price: {p} ⭐*",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(f"💳 Unlock D — {p} ⭐", callback_data="pay_d")]]))
            else:
                await update.message.reply_text("🔒 *Join our free channel for your 7-day trial!*",
                                                parse_mode="Markdown", reply_markup=no_access)
            return
        cache = db_query("SELECT data,gift_data,cached_at FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache or not cache[0][0] or json.loads(cache[0][0]) == []:
            await update.message.reply_text(
                "📉 *D Analysis*\n\n⏳ No signals yet — analyses run automatically.\n\n"
                "📅 *Analyses run at:*\n9:30am, 11:30am, 12pm, 1pm, 2:30pm, 3pm, 5pm, 7pm, 11pm WAT",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎁 Get Gift Pick", callback_data="d_gift")]])); return
        sigs = json.loads(cache[0][0])
        try:
            t = datetime.fromisoformat(cache[0][2]).astimezone(LAGOS).strftime("%d %b • %H:%M WAT")
        except: t = "recently"
        await update.message.reply_text(
            f"📉 *D ANALYSIS*\n🕐 Last run: `{t}`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode="Markdown")
        for i, s in enumerate(sigs[:2], 1):
            await update.message.reply_text(format_d_signal(s, i), parse_mode="Markdown")
            await asyncio.sleep(0.3)
        await update.message.reply_text("🎁 *Want the gift pick?*", parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎁 Get Gift Pick", callback_data="d_gift")]]))

    elif text in ["📊 V Analysis", "📊 V Analysis 🔒"]:
        await update.message.reply_text("📊 *V Analysis*\n🔜 Coming soon…", parse_mode="Markdown")
        return

    elif text == "📋 My Account":
        if is_admin(uid):          status = "👑 Admin"
        elif u and u[8] == 1:      status = "🎁 Free VIP"
        elif user_has_trial(uid):
            left = max(0, int((datetime.fromisoformat(u[2]) - datetime.now()).total_seconds() // 3600))
            status = f"🕐 Trial — {left}h remaining"
        else:
            status = f"D: {'✅' if user_has_d_access(uid) else '❌'} | V: {'✅' if user_has_v_access(uid) else '❌'}"
        spent = db_query("SELECT SUM(amount) FROM payments WHERE user_id=?", (uid,), True)[0][0] or 0
        await update.message.reply_text(
            f"📋 *My Account*\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 User ID: `{uid}`\n🔐 Status: {status}\n⭐ Stars Spent: `{spent}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", parse_mode="Markdown")

    elif text == "ℹ️ Help & Support":
        await update.message.reply_text(
            "ℹ️ *Help & Support*\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "📉 *D Analysis:*\n"
            "Tracks sharp market drops across all football leagues.\n"
            "Markets: Home, Away, Over/Under 1.5 & 2.5, BTTS Yes/No\n\n"
            "🎁 *Gift Pack:*\n"
            "FT Draw, HT Draw & Correct Score drops\n"
            "Posted at *12pm, 5pm & 11pm WAT*\n\n"
            "📅 *Analyses run at:*\n"
            "9:30am, 11:30am, 12pm, 1pm, 2:30pm, 3pm, 5pm, 7pm, 11pm WAT\n\n"
            "🧮 *100% Mathematical confidence scoring*\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚠️ *Bet responsibly. Never stake what you can't afford.*",
            parse_mode="Markdown")

# ─────────────────────────────────────────────────────────────
# CALLBACK HANDLERS (AI buttons added)
# ─────────────────────────────────────────────────────────────
async def handle_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = q.from_user.id
    await q.answer()
    u = get_user(uid)
    if u and u[5] == 1: await q.message.reply_text("🚫 Banned."); return
    if is_paused() and not is_admin(uid):
        await q.message.reply_text("🔧 System paused."); return

    if q.data == "check_join":
        try:
            member = await context.bot.get_chat_member(FREE_CHANNEL_ID, uid)
            if member.status in ["member", "administrator", "creator"]:
                if u and not u[2]:
                    trial_end = (datetime.now() + timedelta(days=TRIAL_DAYS)).isoformat()
                    db_query("UPDATE users SET trial_end=? WHERE user_id=?", (trial_end, uid))
                    name = q.from_user.first_name or "Champ"
                    await q.message.reply_text(
                        f"🎉 *7-Day Free Trial Activated, {name}!* 🔮\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"✅ Full D Analysis access\n✅ Full Gift Pack access\n"
                        f"🧮 100% mathematical scoring\n\n_Use the menu below_ 👇",
                        parse_mode="Markdown", reply_markup=main_menu_kb(uid))
                else:
                    await q.message.reply_text("✅ You already have access!", reply_markup=main_menu_kb(uid))
            else:
                await q.message.reply_text(
                    f"❌ *You haven't joined yet!*\n\nJoin: {FREE_CHANNEL_LINK}\nThen tap again.",
                    parse_mode="Markdown")
        except Exception as e:
            logger.error(f"check_join: {e}")
            await q.message.reply_text("❌ Couldn't verify. Try again.")

    elif q.data == "pay_d":
        p = get_d_price()
        if p > 0:
            await q.message.reply_invoice("📉 D Analysis Access",
                "Sharp market drop signals across all football leagues",
                "d_access", "", "XTR", [LabeledPrice("D Analysis", p)])

    elif q.data == "pay_v":
        p = get_v_price()
        if p > 0:
            await q.message.reply_invoice("📊 V Analysis Access",
                "Exchange volume disproportion signals",
                "v_access", "", "XTR", [LabeledPrice("V Analysis", p)])

    elif q.data == "d_gift":
        if not user_has_d_access(uid):
            await q.message.reply_text("🔒 Activate trial or purchase first."); return
        cache = db_query("SELECT gift_data, ht_draw_data, cs_data FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache:
            await q.message.reply_text("🎁 No gift data available."); return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎁 FT Draw", callback_data="pinnacle_ft"),
             InlineKeyboardButton("🕐 HT Draw", callback_data="pinnacle_ht"),
             InlineKeyboardButton("🎯 Correct Score", callback_data="pinnacle_cs")]
        ])
        await q.message.reply_text("🎁 *GIFT PACK* — Choose a category:", parse_mode="Markdown", reply_markup=kb)

    elif q.data == "pinnacle_ft":
        cache = db_query("SELECT gift_data FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache or not cache[0][0]:
            await q.message.reply_text("📭 No FT draws available."); return
        draws = json.loads(cache[0][0])
        for g in draws[:2]:
            await q.message.reply_text(format_gift(g, "draw"), parse_mode="Markdown")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎁 FT X", callback_data="pinnacle_ft"),
             InlineKeyboardButton("🕐 HT X", callback_data="pinnacle_ht"),
             InlineKeyboardButton("🎯 CS", callback_data="pinnacle_cs")]
        ])
        await q.message.reply_text("🅿️ *Pinnacle dropping odds*", parse_mode="Markdown", reply_markup=kb)

    elif q.data == "pinnacle_ht":
        cache = db_query("SELECT ht_draw_data FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache or not cache[0][0]:
            await q.message.reply_text("📭 No HT draws available."); return
        draws = json.loads(cache[0][0])
        for g in draws[:2]:
            await q.message.reply_text(format_gift(g, "ht"), parse_mode="Markdown")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎁 FT X", callback_data="pinnacle_ft"),
             InlineKeyboardButton("🕐 HT X", callback_data="pinnacle_ht"),
             InlineKeyboardButton("🎯 CS", callback_data="pinnacle_cs")]
        ])
        await q.message.reply_text("🅿️ *Pinnacle dropping odds*", parse_mode="Markdown", reply_markup=kb)

    elif q.data == "pinnacle_cs":
        cache = db_query("SELECT cs_data FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache or not cache[0][0]:
            await q.message.reply_text("📭 No correct score drops available."); return
        drops = json.loads(cache[0][0])
        for g in drops[:2]:
            await q.message.reply_text(format_gift(g, "cs"), parse_mode="Markdown")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎁 FT X", callback_data="pinnacle_ft"),
             InlineKeyboardButton("🕐 HT X", callback_data="pinnacle_ht"),
             InlineKeyboardButton("🎯 CS", callback_data="pinnacle_cs")]
        ])
        await q.message.reply_text("🅿️ *Pinnacle dropping odds*", parse_mode="Markdown", reply_markup=kb)

    # NEW: AI ACCA model selection
    elif q.data == "ai_gemini" or q.data == "ai_deepseek":
        if not (is_admin(uid) or ai_buttons_public):
            await q.answer("⛔ Access denied.", show_alert=True)
            return
        await q.answer()
        await q.message.reply_text("⏳ *Analysing data & building premium ACCA...*\n\nThis may take up to 20 seconds.",
                                   parse_mode="Markdown")
        data = get_ai_aggregated_data()
        if not data:
            await q.message.reply_text("❌ Not enough qualifying data to generate an ACCA right now.")
            return
        if q.data == "ai_gemini":
            acca_text = await call_gemini_acca(data)
        else:
            acca_text = await call_deepseek_acca(data)
        await q.message.reply_text(acca_text, parse_mode="Markdown", disable_web_page_preview=True)

# ─────────────────────────────────────────────────────────────
# SECRET TEXT HANDLERS (unchanged)
# ─────────────────────────────────────────────────────────────
async def secret_handlers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().lower()
    uid = update.effective_user.id
    if not user_has_d_access(uid) and not is_admin(uid):
        return

    match = re.match(r'^draw\s+ft\s+(\d+)$', text)
    if match:
        n = int(match.group(1))
        cache = db_query("SELECT gift_data FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache or not cache[0][0]:
            await update.message.reply_text("No FT draw data."); return
        draws = json.loads(cache[0][0])
        if n < 1 or n > len(draws):
            await update.message.reply_text(f"Only {len(draws)} FT draws available."); return
        await update.message.reply_text(format_gift(draws[n-1], "draw"), parse_mode="Markdown")
        return

    match = re.match(r'^draw\s+ht\s+(\d+)$', text)
    if match:
        n = int(match.group(1))
        cache = db_query("SELECT ht_draw_data FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache or not cache[0][0]:
            await update.message.reply_text("No HT draw data."); return
        draws = json.loads(cache[0][0])
        if n < 1 or n > len(draws):
            await update.message.reply_text(f"Only {len(draws)} HT draws available."); return
        await update.message.reply_text(format_gift(draws[n-1], "ht"), parse_mode="Markdown")
        return

    match = re.match(r'^correct\s+score\s+(\d+)$', text)
    if match:
        n = int(match.group(1))
        cache = db_query("SELECT cs_data FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache or not cache[0][0]:
            await update.message.reply_text("No correct score data."); return
        drops = json.loads(cache[0][0])
        if n < 1 or n > len(drops):
            await update.message.reply_text(f"Only {len(drops)} correct score drops available."); return
        await update.message.reply_text(format_gift(drops[n-1], "cs"), parse_mode="Markdown")
        return

    match = re.match(r'^D\s*analysis\s+(\d+)$', text)
    if match:
        n = int(match.group(1))
        cache = db_query("SELECT data FROM d_analysis_cache ORDER BY id DESC LIMIT 1", (), True)
        if not cache or not cache[0][0]:
            await update.message.reply_text("No D analysis data."); return
        sigs = json.loads(cache[0][0])
        if n < 1 or n > len(sigs):
            await update.message.reply_text(f"Only {len(sigs)} D signals available."); return
        await update.message.reply_text(format_d_signal(sigs[n-1], n), parse_mode="Markdown")
        return

# ─────────────────────────────────────────────────────────────
# ADMIN COMMANDS (unchanged, plus toggle_ai_buttons & openrouter key)
# ─────────────────────────────────────────────────────────────
async def cmd_toggle_ai_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    global ai_buttons_public
    ai_buttons_public = not ai_buttons_public
    set_setting('ai_public', '1' if ai_buttons_public else '0')
    state = "🌍 public" if ai_buttons_public else "🔐 admin-only"
    await update.message.reply_text(f"🤖 *AI ACCA visibility*: `{state}`", parse_mode="Markdown")

async def cmd_singbetsource(update, context):
    if not is_admin(update.effective_user.id): return
    args = context.args
    if not args or args[0] not in ("oddspapi", "oddsapi"):
        current = get_singbet_source()
        await update.message.reply_text(f"ℹ️ *SingBet source:* `{current}`\n\nUsage: `/singbetsource oddspapi` or `/singbetsource oddsapi`", parse_mode="Markdown")
        return
    new_src = args[0]
    set_setting("singbet_source", new_src)
    await update.message.reply_text(f"✅ SingBet source set to `{new_src}`.", parse_mode="Markdown")

async def cmd_stats(update, context):
    if not is_admin(update.effective_user.id): return
    total = db_query("SELECT COUNT(*) FROM users", (), True)[0][0]
    trial = db_query("SELECT COUNT(*) FROM users WHERE trial_end IS NOT NULL", (), True)[0][0]
    d_paid = db_query("SELECT COUNT(*) FROM users WHERE d_access=1", (), True)[0][0]
    v_paid = db_query("SELECT COUNT(*) FROM users WHERE v_access=1", (), True)[0][0]
    vip = db_query("SELECT COUNT(*) FROM users WHERE is_free_vip=1", (), True)[0][0]
    revenue = db_query("SELECT SUM(amount) FROM payments", (), True)[0][0] or 0
    n_sigs = db_query("SELECT COUNT(*) FROM signal_results", (), True)[0][0]
    n_fixt = db_query("SELECT COUNT(*) FROM daily_fixtures", (), True)[0][0]
    paused = "🔴 PAUSED" if is_paused() else "🟢 RUNNING"
    await update.message.reply_text(
        f"📊 *Devoul Stats*\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ System: {paused}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Users: `{total}` | Trial: `{trial}`\n"
        f"📉 D Access: `{d_paid}` | 📊 V: `{v_paid}` | 🎁 VIP: `{vip}`\n"
        f"⭐ Revenue: `{revenue} stars`\n"
        f"📡 Signals Posted: `{n_sigs}`\n"
        f"📅 Today's fixtures: `{n_fixt}`\n"
        f"💰 D Price: `{get_d_price()} ⭐` | V: `{get_v_price()} ⭐`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 *Analyses run at (WAT):*\n"
        f"9:30am D | 11:30am D | 12pm Gift\n1pm D | 2:30pm V | 3pm D | 5pm Gift | 7pm V | 11pm Gift",
        parse_mode="Markdown")

async def cmd_history(update, context):
    if not is_admin(update.effective_user.id): return
    total = db_query("SELECT COUNT(*) FROM signal_results", (), True)[0][0] or 0
    settled = db_query("SELECT COUNT(*) FROM signal_results WHERE result!='pending'", (), True)[0][0] or 0
    wins = db_query("SELECT COUNT(*) FROM signal_results WHERE result='win'", (), True)[0][0] or 0
    losses = db_query("SELECT COUNT(*) FROM signal_results WHERE result='loss'", (), True)[0][0] or 0
    wr = round(wins / settled * 100, 1) if settled else 0
    roi = round((wins - losses) / max(settled, 1) * 100, 2)
    avg_conf = (db_query("SELECT AVG(confidence) FROM signal_results", (), True)[0][0] or 0)
    d_w = db_query("SELECT COUNT(*) FROM signal_results WHERE signal_type='D' AND result='win'", (), True)[0][0] or 0
    d_t = db_query("SELECT COUNT(*) FROM signal_results WHERE signal_type='D' AND result!='pending'", (), True)[0][0] or 0
    mkt_rows = db_query("""SELECT market,COUNT(*) total,SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) wins
                           FROM signal_results WHERE result!='pending'
                           GROUP BY market ORDER BY wins*1.0/COUNT(*) DESC LIMIT 5""", (), True)
    mkt_lines = "\n".join(
        f"  `{r[0]}` — {round(r[2]/r[1]*100,1) if r[1] else 0}% WR ({r[2]}/{r[1]})"
        for r in mkt_rows) if mkt_rows else "  _No data yet_"
    recent = db_query("""SELECT signal_type,match,market,prediction,confidence,result,recorded_at
                         FROM signal_results ORDER BY id DESC LIMIT 10""", (), True)
    rec_lines = []
    for r in recent:
        stype, match, mkt, pred, conf, res, rec_at = r
        emoji = {"win": "✅", "loss": "❌", "void": "⬜", "pending": "⏳"}.get(res, "⏳")
        try: t = datetime.fromisoformat(rec_at).astimezone(LAGOS).strftime("%d %b %H:%M")
        except: t = str(rec_at)[:10]
        rec_lines.append(f"{emoji} [{stype}] *{match}* | `{pred}` | `{conf}%` | {t}")
    await update.message.reply_text(
        f"📊 *Devoul Edge Monitor*\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 Total: `{total}` | Settled: `{settled}` | ⏳ Pending: `{total - settled}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🏆 *Win Rate:* `{wr}%` ({wins}W / {losses}L)\n"
        f"💹 *ROI:* `{roi}%` (flat stake)\n"
        f"🎯 *Avg Confidence:* `{round(avg_conf, 1)}/99`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📉 D: `{round(d_w/d_t*100,1) if d_t else 0}%` WR ({d_w}/{d_t})\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Market Accuracy:*\n{mkt_lines}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Recent Signals:*\n" + "\n".join(rec_lines) +
        f"\n\n_/settle [id] [win/loss/void]_",
        parse_mode="Markdown")

async def cmd_settle(update, context):
    if not is_admin(update.effective_user.id): return
    args = context.args
    if len(args) < 2 or not args[0].isdigit() or args[1] not in ["win", "loss", "void"]:
        await update.message.reply_text("Usage: `/settle [id] [win/loss/void]`", parse_mode="Markdown"); return
    db_query("UPDATE signal_results SET result=?, settled_at=? WHERE id=?", (args[1], datetime.now().isoformat(), int(args[0])))
    await update.message.reply_text(f"✅ Signal `{args[0]}` settled as *{args[1]}*.", parse_mode="Markdown")

async def cmd_pause(update, context):
    if not is_admin(update.effective_user.id): return
    set_setting('system_paused', '1')
    await update.message.reply_text("🔴 *System PAUSED*\n\nUse /resume to restart.", parse_mode="Markdown")

async def cmd_resume(update, context):
    if not is_admin(update.effective_user.id): return
    set_setting('system_paused', '0')
    await update.message.reply_text("🟢 *System RESUMED*", parse_mode="Markdown")

async def cmd_setprice(update, context):
    if not is_admin(update.effective_user.id): return
    a = context.args
    if len(a) != 2 or a[0] not in ["d", "v", "both"] or not a[1].isdigit():
        await update.message.reply_text("Usage:\n`/setprice d 500`\n`/setprice v 300`\n`/setprice both 500`\n\n0=free", parse_mode="Markdown"); return
    t, p = a[0], int(a[1])
    if t in ["d", "both"]: set_setting("d_price", p)
    if t in ["v", "both"]: set_setting("v_price", p)
    await update.message.reply_text(f"✅ D: `{get_d_price()} ⭐` | V: `{get_v_price()} ⭐`", parse_mode="Markdown")

async def cmd_changeapi(update, context):
    if not is_admin(update.effective_user.id): return
    global ODDSPAPI_KEY, gemini_client, GEMINI_API_KEY, openrouter_api_key, deepseek_client
    args = context.args
    if len(args) < 2 or args[0] not in ["oddspapi", "gemini", "oddsapi", "openrouter"]:
        await update.message.reply_text("Usage:\n`/changeapi oddspapi NEW_KEY`\n`/changeapi gemini NEW_KEY`\n`/changeapi oddsapi NEW_KEY`\n`/changeapi openrouter NEW_KEY`", parse_mode="Markdown"); return
    service, new_key = args[0], args[1]
    if service == "oddspapi":
        ODDSPAPI_KEY = new_key
        set_setting("oddspapi_key", new_key)
        await update.message.reply_text(f"✅ *OddsPapi key updated!*", parse_mode="Markdown")
        await load_market_ids()
    elif service == "gemini":
        GEMINI_API_KEY = new_key
        set_setting("gemini_key", new_key)
        gemini_client = genai.Client(api_key=new_key)
        await update.message.reply_text(f"✅ *Gemini key updated!*", parse_mode="Markdown")
    elif service == "oddsapi":
        set_setting("oddsapi_key", new_key)
        await update.message.reply_text(f"✅ *Odds-API.io key updated!*", parse_mode="Markdown")
    elif service == "openrouter":
        openrouter_api_key = new_key
        set_setting("openrouter_api_key", new_key)
        # Reinitialize DeepSeek client
        if new_key:
            deepseek_client = OpenAI(api_key=new_key, base_url="https://openrouter.ai/api/v1")
        else:
            deepseek_client = None
        await update.message.reply_text(f"✅ *OpenRouter API key updated!*", parse_mode="Markdown")

async def cmd_grantaccess(update, context):
    if not is_admin(update.effective_user.id): return
    a = context.args
    if len(a) < 2 or not a[0].isdigit() or a[1] not in ["d", "v", "both", "vip"]:
        await update.message.reply_text("Usage: `/grantaccess [uid] [d/v/both/vip]`", parse_mode="Markdown"); return
    t, at = int(a[0]), a[1]
    upsert_user(t)
    if at in ["d", "both"]: db_query("UPDATE users SET d_access=1 WHERE user_id=?", (t,))
    if at in ["v", "both"]: db_query("UPDATE users SET v_access=1 WHERE user_id=?", (t,))
    if at == "vip":         db_query("UPDATE users SET is_free_vip=1,d_access=1,v_access=1 WHERE user_id=?", (t,))
    await update.message.reply_text(f"✅ Granted `{at}` to `{t}`.", parse_mode="Markdown")
    try: await context.bot.send_message(t, "🎁 *You've been granted access!*\n\nSend /start to begin.", parse_mode="Markdown")
    except: pass

async def cmd_revokeaccess(update, context):
    if not is_admin(update.effective_user.id): return
    a = context.args
    if not a or not a[0].isdigit():
        await update.message.reply_text("Usage: `/revokeaccess [uid]`", parse_mode="Markdown"); return
    t = int(a[0])
    db_query("UPDATE users SET d_access=0,v_access=0,is_free_vip=0,trial_end=NULL WHERE user_id=?", (t,))
    await update.message.reply_text(f"✅ Revoked `{t}`.", parse_mode="Markdown")

async def cmd_ban(update, context):
    if not is_admin(update.effective_user.id): return
    a = context.args
    if not a or not a[0].isdigit():
        await update.message.reply_text("Usage: /ban [uid]"); return
    t = int(a[0])
    db_query("UPDATE users SET is_banned=1 WHERE user_id=?", (t,))
    await update.message.reply_text(f"🚫 User `{t}` banned.", parse_mode="Markdown")
    try: await context.bot.send_message(t, "🚫 You have been banned.")
    except: pass

async def cmd_broadcast(update, context):
    if not is_admin(update.effective_user.id): return
    msg = " ".join(context.args)
    if not msg: return
    users = db_query("SELECT user_id FROM users WHERE is_banned=0", (), True)
    sent = 0
    for u in (users or []):
        try:
            await context.bot.send_message(u[0], f"📢 *ANNOUNCEMENT*\n\n{msg}", parse_mode="Markdown")
            sent += 1; await asyncio.sleep(0.05)
        except: pass
    await update.message.reply_text(f"✅ Sent to {sent} users.")

async def cmd_scan(update, context):
    if not is_admin(update.effective_user.id): return
    args = context.args
    bot = context.bot
    if not args:
        await update.message.reply_text("Usage: `/scan d` | `/scan v` | `/scan both` | `/scan gift` | `/scan fixtures`", parse_mode="Markdown"); return
    mode = args[0].lower()
    if mode == "fixtures":
        await update.message.reply_text("🔄 Refreshing daily fixtures...")
        await refresh_daily_fixtures()
        n = db_query("SELECT COUNT(*) FROM daily_fixtures", (), True)[0][0]
        await update.message.reply_text(f"✅ `{n}` fixtures cached for today.", parse_mode="Markdown")
        return
    await update.message.reply_text(f"🔄 Running manual scan: `{mode}`...", parse_mode="Markdown")
    pd = mode in ["d", "both"]
    pv = mode in ["v", "both"]
    pg = mode == "gift"
    if pg: pd, pv = False, False
    await combined_analysis_job(bot, post_d=pd, post_v=pv, post_gift=pg)
    await update.message.reply_text("✅ Manual scan complete.")

async def cmd_scanprivate(update, context):
    if not is_admin(update.effective_user.id): return
    args = context.args
    bot = context.bot
    if not args:
        await update.message.reply_text("Usage: `/scanprivate d` | `/scanprivate v` | `/scanprivate both` | `/scanprivate gift`", parse_mode="Markdown"); return
    mode = args[0].lower()
    pd = mode in ["d", "both"]
    pv = mode in ["v", "both"]
    pg = mode == "gift"
    if pg: pd, pv = False, False
    await update.message.reply_text(f"🔄 Running private scan: `{mode}`...", parse_mode="Markdown")
    await combined_analysis_job(bot, post_d=pd, post_v=pv, post_gift=pg, target_chat_id=update.effective_chat.id)
    await update.message.reply_text("✅ Private scan complete.")

# ─────────────────────────────────────────────────────────────
# PAYMENTS (unchanged, with invite link)
# ─────────────────────────────────────────────────────────────
async def precheckout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.pre_checkout_query.answer(ok=True)

async def payment_success(update: Update, context: ContextTypes.DEFAULT_TYPE):
    p = update.message.successful_payment
    uid = update.message.chat_id
    pl = p.invoice_payload
    oid = f"ORD-{uid}-{int(datetime.now().timestamp())}"
    db_query("INSERT INTO payments (user_id,amount,item,date) VALUES (?,?,?,?)",
             (uid, p.total_amount, pl, datetime.now().strftime("%Y-%m-%d")))
    if pl == "d_access":
        db_query("UPDATE users SET d_access=1 WHERE user_id=?", (uid,))
        await update.message.reply_text(
            "✅ *D Analysis Unlocked!* 📉\n\n"
            f"👉 [Join Private Channel]({FREE_CHANNEL_LINK})",
            parse_mode="Markdown", reply_markup=main_menu_kb(uid))
    elif pl == "v_access":
        db_query("UPDATE users SET v_access=1 WHERE user_id=?", (uid,))
        await update.message.reply_text("✅ *V Analysis Unlocked!* 📊",
            parse_mode="Markdown", reply_markup=main_menu_kb(uid))
    await context.bot.send_message(uid,
        f"🧾 *Receipt*\n\n📦 `{pl}`\n⭐ `{p.total_amount}`\n🆔 `{oid}`\n"
        f"📅 `{datetime.now().strftime('%d %b %Y • %H:%M')}`", parse_mode="Markdown")
    try:
        await context.bot.send_message(ADMIN_ID,
            f"💰 *NEW PAYMENT*\n\n👤 `{uid}`\n📦 `{pl}`\n⭐ `{p.total_amount}`\n🆔 `{oid}`",
            parse_mode="Markdown")
    except: pass

# ─────────────────────────────────────────────────────────────
# SCHEDULER (unchanged)
# ─────────────────────────────────────────────────────────────
def setup_scheduler(bot):
    s = AsyncIOScheduler(timezone=LAGOS)
    grace = 3600
    def job(pd, pv, pg):
        async def runner():
            await combined_analysis_job(bot, post_d=pd, post_v=pv, post_gift=pg)
        return runner
    s.add_job(job(True, False, False), CronTrigger(hour=9, minute=30, timezone=LAGOS), misfire_grace_time=grace, id="j0930")
    s.add_job(job(True, False, False), CronTrigger(hour=11, minute=30, timezone=LAGOS), misfire_grace_time=grace, id="j1130")
    s.add_job(job(False, False, True), CronTrigger(hour=12, minute=0, timezone=LAGOS), misfire_grace_time=grace, id="j1200")
    s.add_job(job(True, False, False), CronTrigger(hour=13, minute=0, timezone=LAGOS), misfire_grace_time=grace, id="j1300")
    s.add_job(job(False, False, False), CronTrigger(hour=14, minute=30, timezone=LAGOS), misfire_grace_time=grace, id="j1430")
    s.add_job(job(True, False, False), CronTrigger(hour=15, minute=0, timezone=LAGOS), misfire_grace_time=grace, id="j1500")
    s.add_job(job(False, False, True), CronTrigger(hour=17, minute=0, timezone=LAGOS), misfire_grace_time=grace, id="j1700")
    s.add_job(job(False, False, False), CronTrigger(hour=19, minute=0, timezone=LAGOS), misfire_grace_time=grace, id="j1900")
    s.add_job(job(False, False, True), CronTrigger(hour=23, minute=0, timezone=LAGOS), misfire_grace_time=grace, id="j2300")
    return s

# ─────────────────────────────────────────────────────────────
# POST INIT (UPDATED)
# ─────────────────────────────────────────────────────────────
async def post_init(application):
    bot = application.bot
    saved_oddspapi = get_setting("oddspapi_key")
    if saved_oddspapi:
        global ODDSPAPI_KEY
        ODDSPAPI_KEY = saved_oddspapi
    saved_gemini = get_setting("gemini_key")
    if saved_gemini:
        global GEMINI_API_KEY, gemini_client
        GEMINI_API_KEY = saved_gemini
        gemini_client = genai.Client(api_key=saved_gemini)

    await load_market_ids()
    global V_BOOKMAKER_AVAILABLE, V_BOOKMAKER_SLUG, ai_buttons_public, openrouter_api_key, deepseek_client
    V_BOOKMAKER_AVAILABLE = False
    V_BOOKMAKER_SLUG = None
    ai_buttons_public = get_setting('ai_public') == '1'

    # Load OpenRouter API key
    saved_openrouter = get_setting("openrouter_api_key") or ""
    openrouter_api_key = saved_openrouter
    if saved_openrouter:
        deepseek_client = OpenAI(api_key=saved_openrouter, base_url="https://openrouter.ai/api/v1")
    else:
        deepseek_client = None

    await bot.set_my_commands([BotCommand("start", "Start the bot")])
    await bot.set_my_commands([
        BotCommand("start",        "Start"),
        BotCommand("stats",        "Bot statistics"),
        BotCommand("history",      "Win rate, ROI, signal history"),
        BotCommand("settle",       "Settle a signal result"),
        BotCommand("pause",        "Pause all jobs"),
        BotCommand("resume",       "Resume all jobs"),
        BotCommand("setprice",     "Set D/V prices"),
        BotCommand("changeapi",    "Change API keys (oddspapi/gemini/oddsapi/openrouter)"),
        BotCommand("singbetsource","Set SingBet source (oddspapi/oddsapi)"),
        BotCommand("toggle_ai_buttons","Toggle AI ACCA visibility"),
        BotCommand("grantaccess",  "Grant user access"),
        BotCommand("revokeaccess", "Revoke user access"),
        BotCommand("ban",          "Ban a user"),
        BotCommand("broadcast",    "Broadcast message"),
        BotCommand("scan",         "Manual scan (d/v/both/gift/fixtures)"),
        BotCommand("scanprivate",  "Private scan (results only to you)"),
    ], scope=BotCommandScopeChat(chat_id=ADMIN_ID))

    scheduler = setup_scheduler(bot)
    scheduler.start()
    logger.info("Scheduler started.")
    application.bot_data['scheduler'] = scheduler

# ─────────────────────────────────────────────────────────────
# MAIN (UPDATED with new command handler)
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("stats",        cmd_stats))
    app.add_handler(CommandHandler("history",      cmd_history))
    app.add_handler(CommandHandler("settle",       cmd_settle))
    app.add_handler(CommandHandler("pause",        cmd_pause))
    app.add_handler(CommandHandler("resume",       cmd_resume))
    app.add_handler(CommandHandler("setprice",     cmd_setprice))
    app.add_handler(CommandHandler("changeapi",    cmd_changeapi))
    app.add_handler(CommandHandler("singbetsource",cmd_singbetsource))
    app.add_handler(CommandHandler("toggle_ai_buttons", cmd_toggle_ai_buttons))
    app.add_handler(CommandHandler("grantaccess",  cmd_grantaccess))
    app.add_handler(CommandHandler("revokeaccess", cmd_revokeaccess))
    app.add_handler(CommandHandler("ban",          cmd_ban))
    app.add_handler(CommandHandler("broadcast",    cmd_broadcast))
    app.add_handler(CommandHandler("scan",         cmd_scan))
    app.add_handler(CommandHandler("scanprivate",  cmd_scanprivate))
    app.add_handler(CallbackQueryHandler(handle_callbacks))
    app.add_handler(PreCheckoutQueryHandler(precheckout))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, payment_success))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons), group=0)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, secret_handlers), group=1)
    logger.info("Devoul Bot starting...")
    app.run_polling()
