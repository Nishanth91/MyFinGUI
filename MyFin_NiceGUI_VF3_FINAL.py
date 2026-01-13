"""MyFin — NiceGUI (VF3_FINAL) | 1:1 functional port of Streamlit HF2 + premium UI

Includes:
- Login
- Dashboard (KPIs + charts + recurring auto-add)
- Add (tiles)
- Credit Bal (utilization + upcoming recurring)
- Trends
- Transactions (filter + edit/delete by row + export)
- Admin (Locks, Rules, Recurring templates CRUD, Accounts editor, Fix Mistakes)

Credentials:
- Set GOOGLE_SERVICE_ACCOUNT_JSON env var (full JSON) OR place service_account.json next to this file.

Run:
  pip install nicegui pandas gspread google-auth plotly python-dateutil
  python MyFin_NiceGUI_VF3_FINAL.py
"""

from __future__ import annotations

import os
import json
import time
import random
import uuid
import hmac
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

import pandas as pd
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
from dateutil.relativedelta import relativedelta

from nicegui import ui, app


# -------------------- APP CONFIG --------------------
APP_NAME = "MyFin"
APP_VERSION = "VF3_FINAL_NICEGUI"
SHEET_NAME = "nishanthfintrack_2026"

TAB_TRANSACTIONS = "transactions"
TAB_ACCOUNTS = "cards"
TAB_ADMIN = "admin"

TX_HEADERS = ["TxId", "Date", "Owner", "Type", "Amount", "Pay", "Account", "Category", "Notes", "CreatedAt", "AutoTag"]
ACCT_HEADERS = ["Account", "Emoji", "Limit", "BillingDay"]
ADMIN_HEADERS = ["Key", "Value"]

# Login (same simple auth as your Streamlit versions)
AUTH_USERNAME = "Ajay"
AUTH_PASSWORD = "1999"

ACCOUNT_EMOJI_DEFAULT = {
    "Canadian tire Mastercard - Grey": "🛒",
    "Canadian tire Mastercard - Black": "🛒",
    "RBC VISA": "🏦",
    "RBC Mastercard": "🏦",
    "Line of Credit": "📉",
}

ACCOUNT_ALIASES = {
    "Canadian Tire Grey Card": "Canadian tire Mastercard - Grey",
    "Canadian Tire Black Card": "Canadian tire Mastercard - Black",
    "Nishanth's RBC Card": "RBC VISA",
    "Indhu's RBC Card": "RBC Mastercard",
}

DEFAULT_CATEGORY_RULES = {
    "Salary": ["salary", "payroll", "pay", "direct deposit", "fis"],
    "Investment": ["tfsa", "fhsa", "rrsp", "investment", "contribution", "brokerage", "wealthsimple", "questrade"],
    "Rent": ["rent", "lease"],
    "Groceries": ["grocery", "superstore", "walmart", "costco", "freshco", "save on", "saveon", "no frills", "nofrills"],
    "Food/Coffee": ["restaurant", "pizza", "ubereats", "doordash", "tim hortons", "tims", "starbucks", "coffee", "cafe", "food"],
    "Fuel": ["fuel", "gas", "petro", "shell", "esso", "co-op", "coop", "costco gas"],
    "Car": ["lanpro", "service", "oil", "tire", "tyre", "alignment", "repair", "mercedes", "insurance"],
    "Utilities": ["hydro", "electric", "water", "internet", "wifi", "phone", "mobile", "bell", "rogers", "telus", "shaw"],
    "Shopping": ["amazon", "ikea", "bestbuy", "best buy", "mall", "shopping"],
    "Medical": ["pharmacy", "doctor", "clinic", "dental", "dentist", "hospital"],
    "Travel": ["flight", "hotel", "airbnb", "uber", "lyft", "taxi"],
    "India Transfer": ["wise", "remitly", "remit", "remittance", "money transfer", "india"],
    "Banking/Fees": ["fee", "charges", "interest", "bank fee", "nsf", "overdraft"],
    "Entertainment": ["netflix", "prime", "spotify", "movie", "theatre"],
    "Uncategorized": [],
}


# -------------------- GOOGLE SHEETS --------------------
_gclient: Optional[gspread.Client] = None
_spreadsheet: Optional[gspread.Spreadsheet] = None
_ws_map: Dict[str, gspread.Worksheet] = {{}}

def _load_service_account_info() -> dict:
    env = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if env:
        return json.loads(env)
    local = os.path.join(os.path.dirname(__file__), "service_account.json")
    if os.path.exists(local):
        with open(local, "r", encoding="utf-8") as f:
            return json.load(f)
    raise RuntimeError(
        "Missing Google service account credentials. "
        "Set GOOGLE_SERVICE_ACCOUNT_JSON env var OR place service_account.json next to this script."
    )

def gclient() -> gspread.Client:
    global _gclient
    if _gclient is None:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        info = _load_service_account_info()
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        _gclient = gspread.authorize(creds)
    return _gclient

def open_sheet() -> gspread.Spreadsheet:
    global _spreadsheet
    if _spreadsheet is None:
        _spreadsheet = gclient().open(SHEET_NAME)
    return _spreadsheet

def gs_call(fn, *args, **kwargs):
    last_err = None
    for attempt in range(8):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            last_err = e
            s = str(e)
            if ("429" in s) or ("Quota" in s) or ("Read requests" in s) or ("500" in s) or ("503" in s):
                time.sleep(min(30.0, (1.0 * (2 ** attempt)) + random.random()))
                continue
            raise
    raise last_err

def _ws(ss: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    if title in _ws_map:
        return _ws_map[title]
    w = ss.worksheet(title)
    _ws_map[title] = w
    return w

def ensure_ws(ss: gspread.Spreadsheet, title: str, headers: List[str], rows: int = 2000, cols: int = 30) -> gspread.Worksheet:
    try:
        ws = _ws(ss, title)
    except Exception:
        ws = gs_call(ss.add_worksheet, title=title, rows=rows, cols=cols)
        _ws_map[title] = ws

    values = gs_call(ws.get_all_values)
    if not values:
        gs_call(ws.update, "A1", [headers])
    else:
        hdr = values[0]
        if [h.strip() for h in hdr] != headers:
            gs_call(ws.update, "A1", [headers])
    return ws

def ensure_admin_defaults(ws_admin: gspread.Worksheet) -> None:
    data = gs_call(ws_admin.get_all_records)
    keys = {str(r.get("Key", "")).strip() for r in data}
    updates = []
    if "locked_months" not in keys:
        updates.append(["locked_months", ""])
    if "rules_locked" not in keys:
        updates.append(["rules_locked", "false"])
    if "rules_text" not in keys:
        rules_text = "\n".join([f"{k}: {', '.join(v)}" for k, v in DEFAULT_CATEGORY_RULES.items()])
        updates.append(["rules_text", rules_text])
    if "recurring_prefs_json" not in keys:
        updates.append(["recurring_prefs_json", "[]"])
    if updates:
        gs_call(ws_admin.append_rows, updates, value_input_option="USER_ENTERED")

def _admin_get(ws_admin: gspread.Worksheet, key: str) -> str:
    rows = gs_call(ws_admin.get_all_records)
    for r in rows:
        if str(r.get("Key", "")).strip() == key:
            return str(r.get("Value", "") or "")
    return ""

def _admin_set(ws_admin: gspread.Worksheet, key: str, value: str) -> None:
    values = gs_call(ws_admin.get_all_values)
    if not values:
        gs_call(ws_admin.update, "A1", [ADMIN_HEADERS])
        values = gs_call(ws_admin.get_all_values)

    for i, row in enumerate(values[1:], start=2):
        if len(row) >= 1 and str(row[0]).strip() == key:
            gs_call(ws_admin.update, f"B{i}", [[value]])
            return
    gs_call(ws_admin.append_row, [key, value], value_input_option="USER_ENTERED")


# -------------------- PORTED HF2 LOGIC --------------------
def normalize_account_name(v: str) -> str:
    if not v:
        return ""
    s = str(v).strip()

    # Strip a leading emoji/prefix token (e.g., "💳 Canadian Tire Grey Card")
    parts = s.split(" ", 1)
    if len(parts) == 2 and not any(c.isalnum() for c in parts[0]):
        s = parts[1].strip()

    # Apply backward-compatible renames
    s = ACCOUNT_ALIASES.get(s, s)
    return s


def build_month_options(today: dt.date | None = None, past_months: int = 12, future_months: int = 3) -> list[str]:
    """Return month strings YYYY-MM for (past_months) including current + (future_months)."""
    if today is None:
        today = dt.date.today()
    first = dt.date(today.year, today.month, 1) - relativedelta(months=past_months-1)
    last = dt.date(today.year, today.month, 1) + relativedelta(months=future_months)
    months = []
    cur = first
    while cur <= last:
        months.append(cur.strftime("%Y-%m"))
        cur = cur + relativedelta(months=1)
    return months






# =============================
# Config
# =============================
st.set_page_config(page_title="NishanthFinTrack 2026", page_icon="💸", layout="wide", initial_sidebar_state="collapsed")
st.set_option("client.showErrorDetails", False)
st.set_option("client.toolbarMode", "minimal")

APP_NAME = "NishanthFinTrack 2026"
APP_VERSION = "VF3_1A_HF2"
SHEET_NAME = "nishanthfintrack_2026"   # <-- change if your Sheet name differs

# Hardcoded auth as requested
AUTH_USERNAME = "Ajay"
AUTH_PASSWORD = "1999"

# Fixed accounts (E2) + emojis (E3)
ACCOUNT_EMOJI_DEFAULT = {
    "Canadian tire Mastercard - Grey": "🛒",
    "Canadian tire Mastercard - Black": "🛒",
    "RBC VISA": "🏦",
    "RBC Mastercard": "🏦",
    "Line of Credit": "📉",
}

# Backward-compatible aliases (old -> new). This keeps existing sheet rows working after renames.
ACCOUNT_ALIASES = {
    "Canadian Tire Grey Card": "Canadian tire Mastercard - Grey",
    "Canadian Tire Black Card": "Canadian tire Mastercard - Black",
    "Nishanth's RBC Card": "RBC VISA",
    "Indhu's RBC Card": "RBC Mastercard",
}
ALLOWED_ACCOUNTS = list(ACCOUNT_EMOJI_DEFAULT.keys())

# Types + emoji (C3 redesign)
TYPE_EMOJI = {"Debit": "🧾", "Credit": "💰", "Investment": "💹", "CC Repay": "💳", "International": "🌍", "LOC Draw": "🏦", "LOC Repay": "↩️"}
ENTRY_TYPES = list(TYPE_EMOJI.keys())
PAY_METHODS = ["Card", "Bank", "Cash"]  # C7: Salary uses Bank label


# Display labels (finance-style) — internal values remain the same in Sheets.
TYPE_DISPLAY = {
    "Debit": "Expense (−)",
    "Credit": "Income (+)",
    "Investment": "Invest",
    "CC Repay": "Pay Credit Card",
    "International": "Remit International",
}
DISPLAY_TO_TYPE = {v: k for k, v in TYPE_DISPLAY.items()}


def type_to_display(t: str) -> str:
    return TYPE_DISPLAY.get(t, t)


def display_to_type(lbl: str) -> str:
    return DISPLAY_TO_TYPE.get(lbl, lbl)


def build_account_maps(acct_df: pd.DataFrame):
    """Return (accounts_list, emoji_map) from Accounts sheet (fallback to defaults)."""
    accounts = []
    emoji_map = dict(ACCOUNT_EMOJI_DEFAULT)
    if isinstance(acct_df, pd.DataFrame) and not acct_df.empty and "Account" in acct_df.columns:
        accounts = [a for a in acct_df["Account"].astype(str).tolist() if a and a != "nan"]
        if "Emoji" in acct_df.columns:
            for _, r in acct_df.iterrows():
                a = str(r.get("Account", "")).strip()
                if a:
                    e = str(r.get("Emoji", "")).strip()
                    if e and e != "nan":
                        emoji_map[a] = e
    if not accounts:
        accounts = list(ACCOUNT_EMOJI_DEFAULT.keys())
    return accounts, emoji_map

# Categories (E4,E5)
CATEGORY_ICON = {
    "Salary": "💼",
    "Rent": "🏠",
    "Groceries": "🛒",
    "Food/Coffee": "☕",
    "Fuel": "⛽",
    "Car": "🚗",
    "Utilities": "💡",
    "Shopping": "🛍️",
    "Medical": "🩺",
    "Travel": "✈️",
    "Investment": "💹",
    "India Transfer": "🌍",
    "Banking/Fees": "🏦",
    "LOC Utilization": "🏦",
    "Repayment": "↩️",
    "Entertainment": "🎬",
    "Uncategorized": "❓",
}

DEFAULT_CATEGORY_RULES = {
    "Salary": ["salary", "payroll", "pay", "direct deposit", "fis"],
    "Investment": ["tfsa", "fhsa", "rrsp", "investment", "contribution", "brokerage", "wealthsimple", "questrade"],
    "Rent": ["rent", "lease"],
    "Groceries": ["grocery", "superstore", "walmart", "costco", "freshco", "save on", "saveon", "no frills", "nofrills"],
    "Food/Coffee": ["restaurant", "pizza", "ubereats", "doordash", "tim hortons", "tims", "starbucks", "coffee", "cafe", "food"],
    "Fuel": ["fuel", "gas", "petro", "shell", "esso", "co-op", "coop", "costco gas"],
    "Car": ["lanpro", "service", "oil", "tire", "tyre", "alignment", "repair", "mercedes", "insurance"],
    "Utilities": ["hydro", "electric", "water", "internet", "wifi", "phone", "mobile", "bell", "rogers", "telus", "shaw"],
    "Shopping": ["amazon", "ikea", "bestbuy", "best buy", "mall", "shopping"],
    "Medical": ["pharmacy", "doctor", "clinic", "dental", "dentist", "hospital"],
    "Travel": ["flight", "hotel", "airbnb", "uber", "lyft", "taxi"],
    "India Transfer": ["wise", "remitly", "remit", "remittance", "money transfer", "india"],
    "Banking/Fees": ["fee", "charges", "interest", "bank fee", "nsf", "overdraft"],
    "Entertainment": ["netflix", "prime", "spotify", "movie", "theatre"],
    "Uncategorized": [],
}

STOPWORDS = set("""
a an and are as at be but by for from has have he her hers him his i if in into is it its
me my of on or our ours she so than that the their them they this to up was we were what when where who why will with you your yours
""".split())


# =============================
# Premium Dark UI (final)
# =============================
DARK_CSS = """
<style>
:root{color-scheme:dark;} html,body{color-scheme:dark;}
:root{
  --bg0:#070B14;
  --bg1:#0B1220;
  --panel:rgba(255,255,255,0.050);
  --panel2:rgba(255,255,255,0.070);
  --border:rgba(255,255,255,0.10);
  --text:#E8EAED;
  --muted:rgba(232,234,237,0.66);
  --accent:rgba(99,102,241,1.0);
  --accent2:rgba(56,189,248,1.0);
  --good:rgba(34,197,94,1.0);
  --warn:rgba(245,158,11,1.0);
  --bad:rgba(239,68,68,1.0);
}
.block-container{ padding-top: 1rem; max-width: 1260px; }
.stApp{
  background:
    radial-gradient(900px 650px at 15% 10%, rgba(56,189,248,0.12), transparent 60%),
    radial-gradient(900px 650px at 80% 10%, rgba(99,102,241,0.12), transparent 60%),
    radial-gradient(900px 650px at 80% 85%, rgba(34,197,94,0.08), transparent 60%),
    linear-gradient(180deg, var(--bg0) 0%, var(--bg1) 100%);
  color: var(--text);
}
section[data-testid="stSidebar"]{
  background: linear-gradient(180deg, rgba(7,11,20,0.97), rgba(7,11,20,0.86));
  border-right: 1px solid var(--border);
}
section[data-testid="stSidebar"] *{ color: var(--text); }

h1,h2,h3,h4{ letter-spacing: -0.25px; color: var(--text); }
p, label, .stMarkdown{ color: var(--text); }

@keyframes mfFadeIn { from {opacity:0; transform: translateY(6px);} to {opacity:1; transform: translateY(0);} }
.mf-anim{ animation: mfFadeIn 160ms ease-out; }

.mf-topbrand{
  display:flex; align-items:center; justify-content:space-between;
  gap:12px; padding: 14px 14px; border-radius: 18px;
  background: var(--panel);
  border: 1px solid var(--border);
  box-shadow: 0 18px 46px rgba(0,0,0,0.40);
}
.mf-pill{
  display:inline-block; padding:4px 10px; border-radius:999px;
  background: rgba(99,102,241,0.14);
  border:1px solid rgba(99,102,241,0.22);
  color: rgba(199,210,254,1);
  font-size:12px; font-weight:900;
}
.mf-card{
  border-radius:18px; padding:16px;
  border:1px solid var(--border);
  background: var(--panel);
  box-shadow:0 18px 50px rgba(0,0,0,0.40);
}
.mf-card.tight{ padding:14px; }
.mf-card:hover{ border-color: rgba(99,102,241,0.28); box-shadow:0 20px 60px rgba(0,0,0,0.45); }
.mf-card h4{ margin:0 0 8px 0; font-weight:950; }
.mf-kpi{ font-size:30px; font-weight:950; margin:0; line-height:1.1; }
.mf-sub{ margin:6px 0 0 0; font-size:13px; color: var(--muted); }

.mf-tile{
  border-radius:18px; padding:14px;
  border:1px solid var(--border);
  background: var(--panel);
  box-shadow:0 18px 50px rgba(0,0,0,0.40);
}
.mf-tile-title{ font-weight:950; font-size:14px; margin:0; color: var(--text); }
.mf-tile-num{ font-weight:950; font-size:22px; margin:6px 0 0 0; color: var(--text); }
.mf-tile-sub{ font-size:12px; color: var(--muted); margin:8px 0 0 0; }
.mf-hr{ height:1px; background: var(--border); margin:10px 0; }

.mf-sync{
  display:flex; align-items:center; justify-content:space-between; gap:10px;
  padding:10px 12px; border-radius: 16px;
  background: rgba(255,255,255,0.045);
  border: 1px solid var(--border);
}
.mf-sync small{ color: var(--muted); }

.stButton > button{
  border-radius: 14px !important;
  border: 1px solid rgba(99,102,241,0.35) !important;
  background: rgba(99,102,241,0.16) !important;
  color: rgba(224,231,255,1) !important;
  font-weight: 950 !important;
  padding: 0.62rem 0.95rem !important;
}
.stButton > button:hover{
  background: rgba(99,102,241,0.24) !important;
  border-color: rgba(99,102,241,0.55) !important;
}
.stButton > button:active{ transform: scale(0.99); }

div[data-baseweb="input"] input,
div[data-baseweb="textarea"] textarea{
  background: rgba(255,255,255,0.06) !important;
  color: var(--text) !important;
  border: 1px solid var(--border) !important;
}
div[data-baseweb="select"] > div{
  background: rgba(255,255,255,0.06) !important;
  border: 1px solid var(--border) !important;
}

div[data-testid="stDataFrame"]{
  border-radius:14px;
  border:1px solid var(--border);
  overflow:hidden;
  background: rgba(255,255,255,0.03);
}

.mf-login-wrap{ max-width: 420px; margin: 5vh auto 0 auto; }
.mf-login-title{ font-size: 26px; font-weight: 950; margin: 0; color: var(--text); }
.mf-login-sub{ margin-top: 6px; color: var(--muted); font-size: 13px; }

/* ===== VF2.2: Bigger sidebar pages navigation ===== */
section[data-testid="stSidebar"] { min-width: 280px !important; max-width: 320px !important; }
section[data-testid="stSidebar"] .stSidebarContent { padding-top: 0.5rem; }
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] ul { gap: 6px; }
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] li a {
  font-size: 1.05rem !important;
  line-height: 1.35rem !important;
  padding: 12px 14px !important;
  border-radius: 14px !important;
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] li a svg {
  width: 20px !important; height: 20px !important;
}


/* --- Force dark mode / high contrast (even when OS/browser is light) --- */
:root, html, body {
  color-scheme: dark;
}
[data-testid="stAppViewContainer"], .stApp, body {
  background: radial-gradient(1200px 600px at 15% 10%, rgba(66,99,255,0.18), rgba(0,0,0,0) 55%),
              radial-gradient(900px 500px at 85% 20%, rgba(0,180,140,0.14), rgba(0,0,0,0) 55%),
              #0b0f14 !important;
  color: #e9eef7 !important;
}
h1,h2,h3,h4,h5,h6,p,span,div,label,small,li {
  color: #e9eef7 !important;
}
::placeholder { color: rgba(233,238,247,0.55) !important; opacity: 1 !important; }
input, textarea, [data-baseweb="input"] input, [data-baseweb="textarea"] textarea, [data-baseweb="select"] > div {
  background-color: rgba(255,255,255,0.06) !important;
  color: #e9eef7 !important;
  border-color: rgba(255,255,255,0.14) !important;
}
[data-baseweb="select"] svg { fill: rgba(233,238,247,0.7) !important; }
[data-testid="stSidebar"], section[data-testid="stSidebar"] {
  background: rgba(10,13,18,0.92) !important;
}

/* tiny pill */
.pill { display:inline-block; padding:6px 10px; border-radius:999px; font-weight:600;
  background: rgba(255,255,255,0.08); border:1px solid rgba(255,255,255,0.12); }
.pill-auto { box-shadow: 0 0 0 1px rgba(66,99,255,0.25) inset; }

/* Quick actions layout: keep as 2-column grid on mobile instead of a long list */
.quick-actions div[data-testid="stHorizontalBlock"] { gap: 10px !important; }
@media (max-width: 768px){
  .quick-actions div[data-testid="stHorizontalBlock"] > div[data-testid="column"]{
    flex: 1 1 calc(50% - 10px) !important;
    width: calc(50% - 10px) !important;
    min-width: calc(50% - 10px) !important;
  }
  .quick-actions button{
    height: 44px !important;
    font-size: 16px !important;
  }
}

/* --- Force dark controls even when OS/browser prefers light --- */
@media (prefers-color-scheme: light) {
  :root, body, .stApp { background: #0b0f14 !important; color: #e9eef7 !important; }
}

/* Segmented controls (Streamlit) */
div[data-testid="stSegmentedControl"] button {
  background: rgba(255,255,255,0.06) !important;
  color: #e9eef7 !important;
  border: 1px solid rgba(255,255,255,0.18) !important;
}
div[data-testid="stSegmentedControl"] button[aria-checked="true"] {
  background: rgba(99, 145, 255, 0.35) !important;
  border-color: rgba(120,160,255,0.55) !important;
}

/* Radio/checkbox label text (avoid washed-out light theme) */
label, [data-testid="stMarkdownContainer"] { color: #e9eef7 !important; }

/* BaseWeb (used by selectbox etc.) */
*[data-baseweb] { color-scheme: dark; }

/* Quick actions: force grid-like layout on small screens (prevent full vertical stacking) */
@media (max-width: 740px) {
  .quick-actions [data-testid="stHorizontalBlock"] { flex-wrap: wrap !important; }
  .quick-actions [data-testid="column"] {
    flex: 0 0 calc(50% - 0.75rem) !important;
    width: calc(50% - 0.75rem) !important;
    min-width: 0 !important;
  }
}


/* Force dark native controls on mobile even if OS is light */
input, select, textarea, button { color-scheme: dark; }

/* Fix light (white) segmented controls / radios on mobile */
div[data-testid="stSegmentedControl"] button,
div[data-testid="stRadio"] label,
div[data-testid="stRadio"] div[role="radiogroup"] label,
div[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
div[data-testid="stSelectbox"] div[data-baseweb="select"] * {
  background-color: rgba(255,255,255,0.06) !important;
  color: #e9eefc !important;
  border-color: rgba(255,255,255,0.18) !important;
}

div[data-testid="stSegmentedControl"] button[aria-pressed="true"]{
  background-color: rgba(255,255,255,0.12) !important;
  border-color: rgba(255,90,90,0.55) !important;
  color: #ffffff !important;
}

/* Some mobile browsers render native <select> with light palette unless color-scheme is dark */
select { background-color: #0f1622 !important; color: #e9eefc !important; }

/* --- Mobile: keep 2-column grids instead of collapsing to single column --- */
@media (max-width: 768px){
  /* Force Streamlit columns to remain in a 2-column grid where possible */
  div[data-testid="stHorizontalBlock"]{
    gap: 0.6rem !important;
  }
  div[data-testid="stHorizontalBlock"] > div[data-testid="column"]{
    flex: 1 1 calc(50% - 0.6rem) !important;
    width: calc(50% - 0.6rem) !important;
    min-width: calc(50% - 0.6rem) !important;
  }
  /* If there is only one column, let it take full width */
  div[data-testid="stHorizontalBlock"] > div[data-testid="column"]:only-child{
    flex-basis: 100% !important;
    width: 100% !important;
    min-width: 100% !important;
  }
}

/* --- Darken radio/segmented controls on mobile (Safari sometimes forces light styles) --- */
div[data-testid="stSegmentedControl"] div[role="radiogroup"]{
  background: rgba(16,22,36,0.70) !important;
  border: 1px solid rgba(255,255,255,0.12) !important;
}
div[data-testid="stSegmentedControl"] label{
  color: rgba(235,240,255,0.92) !important;
}
div[data-testid="stSegmentedControl"] label[data-checked="true"]{
  background: rgba(255,255,255,0.08) !important;
  border: 1px solid rgba(255,255,255,0.20) !important;
}

/* Fallback for st.radio (pill-style) */
div[data-testid="stRadio"] div[role="radiogroup"]{
  background: rgba(16,22,36,0.70) !important;
  border: 1px solid rgba(255,255,255,0.12) !important;
  border-radius: 12px !important;
  padding: 2px !important;
}
div[data-testid="stRadio"] div[role="radiogroup"] label{
  background: transparent !important;
  color: rgba(235,240,255,0.92) !important;
}
div[data-testid="stRadio"] div[role="radiogroup"] label:hover{
  background: rgba(255,255,255,0.05) !important;
}
div[data-testid="stRadio"] div[role="radiogroup"] label[data-checked="true"]{
  background: rgba(255,255,255,0.08) !important;
  border-radius: 10px !important;
  border: 1px solid rgba(255,255,255,0.20) !important;
}

</style>
"""
st.markdown(DARK_CSS, unsafe_allow_html=True)
# Mobile responsiveness (applies only on small screens)
MOBILE_CSS = r"""
<style>
@media (max-width: 768px) {
  /* HF5: hide sidebar overlay on mobile (we use in-page nav) */
  section[data-testid="stSidebar"], div[data-testid="stSidebar"]{display:none !important;}
  div[data-testid="collapsedControl"]{display:none !important;}

  /* tighter page padding */
  .block-container { padding-left: 0.85rem !important; padding-right: 0.85rem !important; padding-top: 0.75rem !important; }
  /* full-width buttons */
  .stButton>button { width: 100% !important; }

  /* sidebar width on phones (if opened) */
  section[data-testid="stSidebar"] { width: 82vw !important; max-width: 82vw !important; }

  /* sidebar stays available on phones */
  /* ensure main area uses full width */
  div[data-testid="stAppViewContainer"] > .main { margin-left: 0 !important; }

  /* make segmented controls wrap instead of clipping */
  div[data-testid="stSegmentedControl"] div[data-baseweb="button-group"] { flex-wrap: wrap !important; }
  div[data-testid="stSegmentedControl"] button { flex: 1 1 auto !important; min-width: 32% !important; }

  /* prevent tables from breaking layout */
  div[data-testid="stDataFrame"] { overflow-x: auto !important; }
  /* slightly smaller headings */
  h1 { font-size: 1.45rem !important; }
  h2 { font-size: 1.20rem !important; }
  h3 { font-size: 1.05rem !important; }

  /* iOS Safari: ensure typed text is visible (fix white-input + white-text) */
  input, textarea, select {
    background-color: rgba(18,18,20,0.96) !important;
    color: rgba(240,240,245,0.98) !important;
    -webkit-text-fill-color: rgba(240,240,245,0.98) !important;
    caret-color: rgba(240,240,245,0.98) !important;
  }
  /* Streamlit internal input wrappers */
  div[data-baseweb="input"] input,
  div[data-baseweb="textarea"] textarea,
  div[data-baseweb="select"] input {
    background-color: rgba(18,18,20,0.96) !important;
    color: rgba(240,240,245,0.98) !important;
    -webkit-text-fill-color: rgba(240,240,245,0.98) !important;
  }
  /* placeholder */
  input::placeholder, textarea::placeholder {
    color: rgba(240,240,245,0.55) !important;
    -webkit-text-fill-color: rgba(240,240,245,0.55) !important;
  }
}
</style>
"""
st.markdown(MOBILE_CSS, unsafe_allow_html=True)

# --- Auto mobile hint (client-side) ---
# Streamlit can't reliably detect screen size server-side. For phones, we set a query param once using JS.
st.components.v1.html("""
<script>
(function() {
  try {
    const isSmall = window.innerWidth && window.innerWidth <= 768;
    const url = new URL(window.location.href);
    if (isSmall && !url.searchParams.has('mobile')) {
      url.searchParams.set('mobile','1');
      window.location.replace(url.toString());
    }
  } catch (e) {}
})();
</script>
""", height=0)
# If the URL indicates mobile, default view mode to Mobile.
try:
    if st.query_params.get("mobile") in ("1", ["1"]):
        st.session_state["view_mode"] = "Mobile"
except Exception:
    pass


# View mode override (lets you force card-layout on phones)
if "view_mode" not in st.session_state:
    st.session_state["view_mode"] = "Auto"  # Auto | Desktop | Mobile


def money(n: float) -> str:
    sign = "-" if float(n or 0) < 0 else ""
    n = abs(float(n or 0))
    return f"{sign}${n:,.2f}"


def clamp_day(year: int, month: int, day: int) -> int:
    last = calendar.monthrange(year, month)[1]
    return max(1, min(int(day), last))


def parse_amount(text: str) -> Optional[float]:
    t = (text or "").strip().replace(",", "")
    if not t:
        return None
    try:
        v = float(t)
        if v < 0:
            return None
        return v
    except Exception:
        return None


def normalize_merchant(notes: str) -> str:
    t = (notes or "").lower().strip()
    t = re.sub(r"\d+", " ", t)
    t = re.sub(r"[^a-z\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return ""
    words = [w for w in t.split() if w not in STOPWORDS and len(w) > 2]
    return " ".join(words[:2]).strip() if words else ""


def parse_rules_text(text: str) -> Dict[str, List[str]]:
    rules: Dict[str, List[str]] = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        cat, rest = line.split(":", 1)
        keys = [x.strip() for x in rest.split(",") if x.strip()]
        rules[cat.strip()] = keys
    if not rules:
        rules = DEFAULT_CATEGORY_RULES.copy()
    if "Uncategorized" not in rules:
        rules["Uncategorized"] = []
    return rules


# =============================
# Session-state DATA STORE (performance)
# =============================


def classify(notes: str, rules: Dict[str, List[str]]) -> str:
    t = (notes or "").lower()
    for label, keys in rules.items():
        if label == "Uncategorized":
            continue
        for k in keys:
            if k and k.lower() in t:
                return label
    return "Uncategorized"


def prev_month_str(m: str) -> str:
    try:
        p = pd.Period(m, freq="M")
        return str(p - 1)
    except Exception:
        return m


def compute_balance_events(tx: pd.DataFrame) -> pd.DataFrame:
    df = tx[tx["Account"].isin(allowed_accounts_live)].copy()
    charges = df[((df["Type"] == "Debit") & (df["Pay"] == "Card")) | (df["Type"] == "LOC Draw")].copy()
    charges["Delta"] = charges["Amount"]
    charges["Kind"] = "Charge"

    pays = df[(df["Type"] == "CC Repay") | (df["Type"] == "LOC Repay")].copy()
    pays["Delta"] = -pays["Amount"]
    pays["Kind"] = "Payment"

    use = pd.concat([charges, pays], ignore_index=True)
    if use.empty:
        return pd.DataFrame(columns=["Date","Month","Account","Delta","Balance","Kind"])

    use["_sort"] = use["CreatedAt"].fillna("")
    use.sort_values(["Date","_sort"], inplace=True)

    bal = {a: 0.0 for a in allowed_accounts_live}
    out = []
    for _, r in use.iterrows():
        a = r.get("Account", "")
        d = float(r.get("Delta", 0.0))
        bal[a] = max(0.0, bal.get(a, 0.0) + d)
        out.append({"Date": r.get("Date"), "Month": r.get("Month"), "Account": a, "Delta": d, "Balance": bal[a], "Kind": r.get("Kind", "")})
    return pd.DataFrame(out)


def cycle_bounds(month_str: str, billing_day: int) -> Tuple[date, date]:
    p = pd.Period(month_str, freq="M")
    y, m = p.year, p.month
    this_bill = date(y, m, clamp_day(y, m, billing_day))
    prev_p = p - 1
    prev_bill = date(prev_p.year, prev_p.month, clamp_day(prev_p.year, prev_p.month, billing_day))
    return prev_bill, this_bill


def next_bill_date(month_str: str, billing_day: int) -> date:
    p = pd.Period(month_str, freq="M")
    y, m = p.year, p.month
    return date(y, m, clamp_day(y, m, billing_day))


def upcoming_recurring_total(month_str: str, bill_day: int, account: str) -> float:
    prefs = st.session_state["prefs_list"]
    p = pd.Period(month_str, freq="M")
    nb = next_bill_date(month_str, bill_day)
    today = date.today()
    total = 0.0
    for r in prefs:
        if not r.get("IsRecurring", False):
            continue
        if str(r.get("Account","")).strip() != account:
            continue
        amt = r.get("Amount", None)
        if amt is None:
            continue
        try:
            amt = float(amt)
        except Exception:
            continue
        dom = r.get("DayOfMonth", 1)
        try:
            dom = int(float(dom)) if dom is not None else 1
        except Exception:
            dom = 1
        due = date(p.year, p.month, clamp_day(p.year, p.month, dom))
        if today <= due <= nb:
            total += amt
    return float(total)


def util_table(balance_events: pd.DataFrame, month: str, acct_df: pd.DataFrame) -> pd.DataFrame:
    limits = {r["Account"]: float(r["Limit"]) for _, r in acct_df.iterrows()}
    billing = {r["Account"]: int(r["BillingDay"]) for _, r in acct_df.iterrows()}
    emoji = {r["Account"]: str(r["Emoji"]) for _, r in acct_df.iterrows()}

    out = []
    for acct in allowed_accounts_live:
        lim = float(limits.get(acct, 0.0))
        bill = int(billing.get(acct, 1))
        ev = balance_events[balance_events["Account"] == acct] if not balance_events.empty else pd.DataFrame()

        in_m = ev[ev["Month"] == month] if not ev.empty else pd.DataFrame()
        before = ev[ev["Month"] < month] if not ev.empty else pd.DataFrame()
        opening = float(before["Balance"].iloc[-1]) if not before.empty else 0.0
        closing = float(in_m["Balance"].iloc[-1]) if not in_m.empty else opening
        peak = float(in_m["Balance"].max()) if not in_m.empty else opening
        charges = float(in_m.loc[in_m["Kind"] == "Charge", "Delta"].sum()) if not in_m.empty else 0.0
        payments = float(-in_m.loc[in_m["Kind"] == "Payment", "Delta"].sum()) if not in_m.empty else 0.0

        util_pct = (closing / lim * 100.0) if lim > 0 else None

        c_start, c_end = cycle_bounds(month, bill)
        cyc = ev[(ev["Date"] >= pd.Timestamp(c_start)) & (ev["Date"] < pd.Timestamp(c_end))] if not ev.empty else pd.DataFrame()
        cyc_charges = float(cyc.loc[cyc["Kind"] == "Charge", "Delta"].sum()) if not cyc.empty else 0.0
        cyc_pay = float(-cyc.loc[cyc["Kind"] == "Payment", "Delta"].sum()) if not cyc.empty else 0.0
        cyc_peak = float(cyc["Balance"].max()) if not cyc.empty else closing

        upcoming = upcoming_recurring_total(month, bill, acct)
        safe_to_spend = None if lim <= 0 else max(0.0, lim - closing - upcoming)

        out.append({
            "Account": acct,
            "Emoji": emoji.get(acct, ACCOUNT_EMOJI_DEFAULT.get(acct,"💳")),
            "BillingDay": bill,
            "BillDate": str(next_bill_date(month, bill)),
            "Limit": lim,
            "Balance": closing,
            "UtilPct": util_pct,
            "SafeToSpend": safe_to_spend,
            "MonthCharges": charges,
            "MonthPayments": payments,
            "MonthPeak": peak,
            "CycleStart": str(c_start),
            "CycleEnd": str(c_end),
            "CycleCharges": cyc_charges,
            "CyclePayments": cyc_pay,
            "CyclePeak": cyc_peak,
            "UpcomingRecurringToBill": upcoming,
        })
    return pd.DataFrame(out)


# =============================
# Undo stack
# =============================
@dataclass
class UndoAction:
    kind: str  # add/edit/delete
    txid: Optional[str] = None
    row_num: Optional[int] = None
    old_row: Optional[List[object]] = None


def _is_nonexpense_movement(df: pd.DataFrame) -> pd.Series:
    """Rows that should NOT be counted as 'Expense' even if Type is Debit.

    HF9: Exclude LOC utilization and transfers from expense charts while still tracking them.
    """
    if df is None or df.empty:
        return pd.Series([], dtype=bool)

    # normalize helpers
    cat = df.get("Category", "").astype(str).fillna("").str.strip().str.lower()
    acc = df.get("Account", "").astype(str).fillna("").str.strip().str.lower()
    pay = df.get("Pay", "").astype(str).fillna("").str.strip().str.lower()
    typ = df.get("Type", "").astype(str).fillna("").str.strip()

    # Non-expense by Type
    is_move = (typ == "International") | (typ == "CC Repay") | (typ == "LOC Draw") | (typ == "LOC Repay")

    # LOC utilization: any Debit 'Card' charge posted to LOC account
    is_loc = acc.str.contains(r"\bline\s*of\s*credit\b|\bloc\b", regex=True, na=False) & (pay == "card") & (typ == "Debit")

    # Remittance / transfer categories (these are movements, not consumption)
    is_remit = cat.str.contains(r"india|remit|remittance|international\s*transfer|transfer\s*abroad|send\s*home", regex=True, na=False)

    # Repayments are not expenses
    is_repay_cat = cat.str.contains(r"repay|repayment", regex=True, na=False)

    return is_move | is_loc | is_remit | is_repay_cat


def monthly_summary(df: pd.DataFrame) -> Dict[str, float]:
    if df is None or df.empty:
        return {"Credit": 0.0, "Debit": 0.0, "Investment": 0.0, "CC Repay": 0.0, "International": 0.0}

    t = _dash_type_series(df)
    amt = pd.to_numeric(df.get("Amount", 0), errors="coerce").fillna(0.0)

    return {
        "Credit": float(amt[t == "Credit"].sum()),
        "Debit": float(amt[(t == "Debit") & (~_is_nonexpense_movement(df))].sum()),
        "Investment": float(amt[t == "Investment"].sum()),
        "CC Repay": float(amt[t == "CC Repay"].sum()),
        "International": float(amt[t == "International"].sum()),
    }


def hero_insight(df_all: pd.DataFrame, month: str) -> str:
    mdf = df_all[df_all["Month"] == month].copy()
    if mdf.empty:
        return "No transactions yet. Add your first one ✨"

    t = _dash_type_series(mdf)
    ddf = mdf[t == "Debit"].copy()

    if not ddf.empty:
        by_cat = ddf.groupby("Category", as_index=False)["Amount"].sum().sort_values("Amount", ascending=False)
        top = by_cat.iloc[0]
        return f"Highest debit spend: **{cat_label(top['Category'])}** ({money(float(top['Amount']))})"

    return f"Transactions captured: **{len(mdf)}**"


# =============================
# Pages
# =============================


def render_debit_categories_chart(month_df: pd.DataFrame) -> None:
    st.markdown("### 🧩 Debit categories (this month)")
    _t = _dash_type_series(month_df)
    # Only true expenses (exclude Income even if mis-typed as Debit)
    ddf = month_df[_t == "Debit"].copy()
    if ddf.empty:
        st.caption("No debit transactions this month.")
    else:
        by_cat = ddf.groupby("Category", as_index=False)["Amount"].sum().sort_values("Amount", ascending=False)
        by_cat["CategoryLabel"] = by_cat["Category"].apply(cat_label)
        fig_cat = px.bar(by_cat, x="CategoryLabel", y="Amount", title="Debit by Category", height=420, template="plotly_dark", color_discrete_sequence=px.colors.qualitative.Set2)
        fig_cat.update_layout(bargap=0.35, margin=dict(l=10,r=10,t=50,b=10))
        st.plotly_chart(fig_cat, width="stretch")



# -------------------- APP STATE --------------------
@dataclass
class State:
    tx_df: pd.DataFrame
    acct_df: pd.DataFrame
    rules: Dict[str, List[str]]
    rules_locked: bool
    locked_months: List[str]
    prefs_list: List[Dict[str, Any]]
    last_refresh: dt.datetime

STATE: Optional[State] = None
allowed_accounts_live: List[str] = []
emoji_map_live: Dict[str, str] = {}


# -------------------- DATA LAYER --------------------
def _parse_date_any(v: str) -> dt.date:
    s = (str(v or "")).strip()
    if not s:
        return dt.date.today()
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        try:
            return pd.to_datetime(s).date()
        except Exception:
            return dt.date.today()

def _month_of(d: dt.date) -> str:
    return f"{d.year:04d}-{d.month:02d}"

def refresh_all() -> None:
    global STATE, allowed_accounts_live, emoji_map_live

    ss = open_sheet()
    ws_tx = ensure_ws(ss, TAB_TRANSACTIONS, TX_HEADERS, rows=20000)
    ws_ac = ensure_ws(ss, TAB_ACCOUNTS, ACCT_HEADERS, rows=2000)
    ws_ad = ensure_ws(ss, TAB_ADMIN, ADMIN_HEADERS, rows=400)
    ensure_admin_defaults(ws_ad)

    locked_raw = _admin_get(ws_ad, "locked_months").strip()
    locked_months = [x.strip() for x in locked_raw.split(",") if x.strip()]

    rules_text = _admin_get(ws_ad, "rules_text")
    rules = parse_rules_text(rules_text)
    rules_locked = (_admin_get(ws_ad, "rules_locked").strip().lower() == "true")

    prefs_raw = _admin_get(ws_ad, "recurring_prefs_json").strip() or "[]"
    try:
        prefs = json.loads(prefs_raw)
        if not isinstance(prefs, list):
            prefs = []
    except Exception:
        prefs = []

    ac_vals = gs_call(ws_ac.get_all_values)
    if not ac_vals or len(ac_vals) < 2:
        acct_df = pd.DataFrame(columns=ACCT_HEADERS)
    else:
        hdr = ac_vals[0]
        rows = ac_vals[1:]
        acct_df = pd.DataFrame(rows, columns=hdr[:len(rows[0])] if rows else hdr)
        for c in ACCT_HEADERS:
            if c not in acct_df.columns:
                acct_df[c] = ""
        acct_df = acct_df[ACCT_HEADERS]
        acct_df["Limit"] = pd.to_numeric(acct_df["Limit"], errors="coerce").fillna(0.0)
        acct_df["BillingDay"] = pd.to_numeric(acct_df["BillingDay"], errors="coerce").fillna(0).astype(int)

    allowed_accounts_live, emoji_map_live = build_account_maps(acct_df)

    tx_vals = gs_call(ws_tx.get_all_values)
    if not tx_vals or len(tx_vals) < 2:
        tx_df = pd.DataFrame(columns=TX_HEADERS + ["_row", "DateParsed", "Month"])
    else:
        hdr = tx_vals[0]
        rows = tx_vals[1:]
        tx_df = pd.DataFrame(rows, columns=hdr[:len(rows[0])] if rows else hdr)
        for c in TX_HEADERS:
            if c not in tx_df.columns:
                tx_df[c] = ""
        tx_df = tx_df[TX_HEADERS]
        tx_df["_row"] = list(range(2, 2 + len(tx_df)))
        tx_df["Amount"] = pd.to_numeric(tx_df["Amount"], errors="coerce").fillna(0.0)
        tx_df["DateParsed"] = tx_df["Date"].apply(_parse_date_any)
        tx_df["Month"] = tx_df["DateParsed"].apply(_month_of)
        tx_df["Account"] = tx_df["Account"].astype(str).apply(normalize_account_name)

    STATE = State(
        tx_df=tx_df,
        acct_df=acct_df,
        rules=rules,
        rules_locked=rules_locked,
        locked_months=locked_months,
        prefs_list=prefs,
        last_refresh=dt.datetime.now(),
    )

def _assert_month_unlocked(month_str: str) -> None:
    if STATE and month_str in (STATE.locked_months or []):
        raise ValueError(f"Month {month_str} is locked. Unlock in Admin → Locks.")

def _append_transaction_row(row: List[Any]) -> None:
    ss = open_sheet()
    ws = ensure_ws(ss, TAB_TRANSACTIONS, TX_HEADERS, rows=20000)
    gs_call(ws.append_row, row, value_input_option="USER_ENTERED")

def add_transaction(entry_date: dt.date, owner: str, entry_type: str, amount: float, pay: str,
                    account: str, category: str, notes: str, auto_tag: str = "") -> str:
    if STATE is None:
        refresh_all()
    month_str = _month_of(entry_date)
    _assert_month_unlocked(month_str)

    txid = str(uuid.uuid4())
    _append_transaction_row([
        txid,
        entry_date.isoformat(),
        owner,
        entry_type,
        f"{float(amount):.2f}",
        pay,
        normalize_account_name(account),
        category,
        notes,
        dt.datetime.now().isoformat(timespec="seconds"),
        auto_tag or "",
    ])
    refresh_all()
    return txid

def delete_transaction(row_num: int) -> None:
    ss = open_sheet()
    ws = ensure_ws(ss, TAB_TRANSACTIONS, TX_HEADERS, rows=20000)
    if int(row_num) < 2:
        raise ValueError("Invalid row number")
    gs_call(ws.delete_rows, int(row_num))
    refresh_all()

def update_transaction(row_num: int, updates: Dict[str, Any]) -> None:
    ss = open_sheet()
    ws = ensure_ws(ss, TAB_TRANSACTIONS, TX_HEADERS, rows=20000)
    if int(row_num) < 2:
        raise ValueError("Invalid row number")
    col_index = {h: i+1 for i, h in enumerate(TX_HEADERS)}
    for k, v in updates.items():
        if k in col_index:
            cell = gspread.utils.rowcol_to_a1(int(row_num), col_index[k])
            gs_call(ws.update_acell, cell, str(v))
    refresh_all()

def save_accounts(acct_df: pd.DataFrame) -> None:
    ss = open_sheet()
    ws = ensure_ws(ss, TAB_ACCOUNTS, ACCT_HEADERS, rows=2000)
    rows = [ACCT_HEADERS]
    for _, r in acct_df.iterrows():
        rows.append([str(r.get("Account","")), str(r.get("Emoji","")), str(r.get("Limit","")), str(r.get("BillingDay",""))])
    gs_call(ws.update, "A1", rows, value_input_option="USER_ENTERED")
    refresh_all()

def save_admin(locked_months: List[str], rules_text: str, rules_locked: bool, prefs_list: List[Dict[str, Any]]) -> None:
    ss = open_sheet()
    ws = ensure_ws(ss, TAB_ADMIN, ADMIN_HEADERS, rows=400)
    ensure_admin_defaults(ws)
    _admin_set(ws, "locked_months", ", ".join([m.strip() for m in locked_months if m.strip()]))
    _admin_set(ws, "rules_text", rules_text)
    _admin_set(ws, "rules_locked", "true" if rules_locked else "false")
    _admin_set(ws, "recurring_prefs_json", json.dumps(prefs_list))
    refresh_all()

def ensure_recurring_for_month(month_str: str) -> int:
    if STATE is None:
        refresh_all()
    tx = STATE.tx_df
    prefs_list = STATE.prefs_list
    try:
        p = pd.Period(month_str, freq="M")
    except Exception:
        return 0
    year, month = p.year, p.month

    existing_tags = set(tx.loc[(tx["Month"] == month_str) & (tx["AutoTag"].astype(str).str.startswith("AUTO:")), "AutoTag"].astype(str).tolist())
    created = 0

    for pref in prefs_list:
        if not bool(pref.get("IsRecurring", False)):
            continue
        mk = str(pref.get("MerchantKey", "")).strip()
        if not mk:
            continue

        try:
            dom = int(float(pref.get("DayOfMonth", 1) or 1))
        except Exception:
            dom = 1
        dom = clamp_day(dom)
        due = dt.date(year, month, dom)

        tag = f"AUTO:{month_str}:{mk}"
        if tag in existing_tags:
            continue

        entry_type = str(pref.get("Type", "Debit")).strip() or "Debit"
        amount = float(pref.get("Amount", 0) or 0)
        pay = str(pref.get("Pay", "")).strip() or ("Card" if entry_type == "Debit" else "")
        account = str(pref.get("Account", "")).strip() or (allowed_accounts_live[0] if allowed_accounts_live else "")
        category = str(pref.get("Category", "")).strip() or "Uncategorized"
        notes = str(pref.get("Notes", mk)).strip() or mk
        owner = str(pref.get("Owner", "Family")).strip() or "Family"

        try:
            add_transaction(due, owner, entry_type, amount, pay, account, category, notes, auto_tag=tag)
            created += 1
        except Exception:
            continue

    return created


# -------------------- PREMIUM THEME --------------------
def ui_theme() -> None:
    css = """
    <style>
      :root {
        --bg: #070B12;
        --card: #0E1625;
        --card2:#0B1220;
        --text: #E9EEF7;
        --muted: rgba(233,238,247,.68);
        --border: rgba(233,238,247,.12);
      }
      body { background: var(--bg); color: var(--text); }
      .q-page { background: var(--bg) !important; }
      .my-topbar {
        background: linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,0));
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: 12px 14px;
        box-shadow: 0 10px 30px rgba(0,0,0,.35);
      }
      .my-card {
        background: radial-gradient(1200px 600px at 0% 0%, rgba(121,132,255,.16), transparent 60%),
                    radial-gradient(900px 400px at 100% 0%, rgba(62, 255, 202, .10), transparent 55%),
                    var(--card);
        border: 1px solid var(--border);
        border-radius: 18px;
        box-shadow: 0 10px 28px rgba(0,0,0,.38);
      }
      .my-card.flat { background: var(--card2); }
      .my-pill { border-radius: 999px; }
      .my-muted { color: var(--muted); }
      .my-title { font-weight: 900; font-size: 18px; letter-spacing:.2px; }
      .my-sub { font-size: 13px; }
      .kpi { font-size: 22px; font-weight: 900; }
      .chip {
        display:inline-flex; align-items:center; gap:8px;
        padding:6px 10px; border-radius:999px;
        border:1px solid rgba(233,238,247,.14);
        background: rgba(255,255,255,.04);
      }
      .my-grid { display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:12px; }
      @media (min-width: 980px) { .my-grid { grid-template-columns: repeat(3, minmax(0,1fr)); } }
      .tile { cursor:pointer; transition: transform .08s ease, box-shadow .08s ease, border-color .08s ease; }
      .tile:hover { transform: translateY(-1px); box-shadow: 0 14px 34px rgba(0,0,0,.44); border-color: rgba(233,238,247,.22); }
      .my-table .q-table__container { background: var(--card2) !important; border: 1px solid var(--border); border-radius: 16px; }
      .q-table__top, .q-table__bottom { background: var(--card2) !important; }
      .q-field__control { border-radius: 14px !important; }
      .q-btn { text-transform:none !important; }
      .q-tabs { border-radius: 16px; overflow:hidden; border:1px solid var(--border); }
      .q-tab__label { font-weight: 700; }
    </style>
    """
    ui.add_head_html(css)

def auth_ok(u: str, p: str) -> bool:
    return hmac.compare_digest(u or "", AUTH_USERNAME) and hmac.compare_digest(p or "", AUTH_PASSWORD)

def notify_error(e: Exception, prefix: str = "") -> None:
    ui.notify(f"{prefix}{e}", type="negative")

def current_rules_text_from_state() -> str:
    if STATE is None:
        return ""
    lines = []
    for k, v in (STATE.rules or {}).items():
        if isinstance(v, list):
            vv = ", ".join([str(x) for x in v if str(x).strip()])
            lines.append(f"{k}: {vv}")
    return "\n".join(lines)


# -------------------- ROUTES --------------------
@ui.page("/")
def login_page():
    ui_theme()
    if app.storage.user.get("logged_in"):
        ui.navigate.to("/app")
        return

    with ui.column().classes("w-full items-center justify-center").style("min-height: 100vh;"):
        with ui.card().classes("my-card w-[420px] max-w-[92vw] p-6"):
            ui.label(APP_NAME).classes("my-title")
            ui.label("Sign in to continue").classes("my-muted my-sub")

            u = ui.input("Username").classes("w-full")
            p = ui.input("Password", password=True, password_toggle_button=True).classes("w-full")
            msg = ui.label("").classes("text-red-400 text-sm")

            def do_login():
                if auth_ok(u.value, p.value):
                    app.storage.user["logged_in"] = True
                    ui.navigate.to("/app")
                else:
                    msg.text = "Invalid username or password"

            ui.button("Login", on_click=do_login).classes("w-full my-pill")


@ui.page("/app")
def main_page():
    ui_theme()
    if not app.storage.user.get("logged_in"):
        ui.navigate.to("/")
        return

    global STATE
    if STATE is None:
        try:
            refresh_all()
        except Exception as e:
            ui.label(f"Error loading Google Sheet: {e}").classes("text-red-400")
            return

    with ui.row().classes("w-full items-center justify-between my-topbar"):
        ui.label(f"{APP_NAME} • {APP_VERSION}").classes("my-title")
        with ui.row().classes("items-center gap-2"):
            ui.button("Refresh", on_click=lambda: (refresh_all(), ui.notify("Refreshed", type="positive"))).classes("my-pill")
            ui.button("Logout", on_click=lambda: (app.storage.user.clear(), ui.navigate.to("/"))).classes("my-pill")

    tabs = ui.tabs().classes("w-full mt-3")
    with tabs:
        ui.tab("Dashboard")
        ui.tab("Add")
        ui.tab("Credit Bal")
        ui.tab("Trends")
        ui.tab("Transactions")
        ui.tab("Admin")

    with ui.tab_panels(tabs, value="Dashboard").classes("w-full mt-3"):

        # Dashboard
        with ui.tab_panel("Dashboard"):
            months = build_month_options(past_months=12, future_months=3)
            msel = ui.select(months, value=months[-1], label="Month").classes("w-[220px]")
            badge = ui.label("").classes("chip my-muted")

            def render_dashboard():
                try:
                    created = ensure_recurring_for_month(msel.value)
                    badge.text = f"Recurring added: {created}" if created else "Recurring: up to date"
                except Exception:
                    badge.text = "Recurring: skipped"

                tx = STATE.tx_df.copy()
                txm = tx[tx["Month"] == msel.value] if not tx.empty else tx
                if txm.empty:
                    ui.label("No transactions for this month.").classes("my-muted")
                    return

                summ = monthly_summary(tx, msel.value)
                with ui.row().classes("w-full gap-3"):
                    for k, v in summ.items():
                        with ui.card().classes("my-card p-4"):
                            ui.label(k).classes("my-muted my-sub")
                            ui.label(money(v)).classes("kpi")

                insight = hero_insight(tx, msel.value)
                if insight:
                    with ui.card().classes("my-card flat p-4"):
                        ui.label("Insight").classes("my-muted my-sub")
                        ui.label(insight).classes("text-base")

                g = txm.groupby("Type")["Amount"].sum().reset_index()
                g["TypeLabel"] = g["Type"].apply(type_to_display)
                fig = px.bar(g, x="TypeLabel", y="Amount", title="Totals by Type")
                fig.update_layout(paper_bgcolor="#070B12", plot_bgcolor="#070B12", font_color="#E9EEF7")
                ui.plotly(fig).classes("w-full")

                fig2 = render_debit_categories_chart(txm)
                if fig2 is not None:
                    try:
                        fig2.update_layout(paper_bgcolor="#070B12", plot_bgcolor="#070B12", font_color="#E9EEF7")
                    except Exception:
                        pass
                    ui.plotly(fig2).classes("w-full")

            render_dashboard()

        # Add
        with ui.tab_panel("Add"):
            owners = sorted([o for o in STATE.tx_df["Owner"].astype(str).unique().tolist() if o and o != "nan"]) or ["Abhi", "Indhu"]
            accounts = allowed_accounts_live or ["RBC VISA"]
            categories = sorted([c for c in STATE.tx_df["Category"].astype(str).unique().tolist() if c and c != "nan"]) or ["Uncategorized"]
            pay_opts = ["Card", "Cash", "Interac", "Bank", "Online", "Other"]

            ui.label("Add Transaction").classes("my-title")
            ui.label("Tap a tile. Notes will auto-fill Category using Admin → Rules.").classes("my-muted my-sub")

            def open_add_dialog(entry_type: str):
                dlg = ui.dialog()
                with dlg, ui.card().classes("my-card p-5 w-[580px] max-w-[95vw]"):
                    ui.label(type_to_display(entry_type)).classes("text-lg font-bold")

                    d_owner = ui.select(owners, value=owners[0], label="Owner").classes("w-full")
                    d_date = ui.date(value=dt.date.today().isoformat(), label="Date").classes("w-full")
                    d_amount = ui.number(label="Amount", value=0.0, format="%.2f").classes("w-full")
                    d_pay = ui.select(pay_opts, value="Card", label="Pay / Method").classes("w-full")
                    d_acct = ui.select(accounts, value=accounts[0], label="Account").classes("w-full")
                    d_cat = ui.select(categories, value="Uncategorized", label="Category").classes("w-full")
                    d_notes = ui.textarea(label="Notes", value="").classes("w-full")

                    def autofill():
                        try:
                            d_cat.value = classify(d_notes.value or "", STATE.rules)
                        except Exception:
                            pass

                    d_notes.on("blur", lambda e: autofill())

                    with ui.row().classes("w-full justify-end gap-2"):
                        ui.button("Cancel", on_click=dlg.close).classes("my-pill")

                        def save():
                            try:
                                date_parsed = dt.date.fromisoformat(d_date.value)
                                amt = float(d_amount.value or 0)
                                if amt <= 0:
                                    raise ValueError("Amount must be > 0")

                                _assert_month_unlocked(_month_of(date_parsed))

                                cat = d_cat.value or "Uncategorized"
                                if cat == "Uncategorized":
                                    cat = classify(d_notes.value or "", STATE.rules)

                                add_transaction(
                                    entry_date=date_parsed,
                                    owner=str(d_owner.value),
                                    entry_type=entry_type,
                                    amount=amt,
                                    pay=str(d_pay.value or ""),
                                    account=str(d_acct.value or ""),
                                    category=str(cat),
                                    notes=str(d_notes.value or ""),
                                    auto_tag="",
                                )
                                ui.notify("Saved", type="positive")
                                dlg.close()
                                ui.navigate.to("/app")
                            except Exception as e:
                                notify_error(e, "Save failed: ")

                        ui.button("Save", on_click=save).classes("my-pill")
                dlg.open()

            with ui.element("div").classes("my-grid w-full"):
                tiles = [
                    ("Debit", "shopping_bag", "Expense (−)"),
                    ("Credit", "payments", "Income (+)"),
                    ("Investment", "trending_up", "Invest"),
                    ("CC Repay", "credit_score", "Pay Credit Card"),
                    ("International", "public", "Remit"),
                ]
                for t, icon, sub in tiles:
                    with ui.card().classes("my-card p-4 tile").on("click", lambda e, tt=t: open_add_dialog(tt)):
                        with ui.row().classes("items-center gap-3"):
                            ui.icon(icon).classes("text-2xl")
                            with ui.column().classes("gap-0"):
                                ui.label(type_to_display(t)).classes("font-semibold")
                                ui.label(sub).classes("my-muted my-sub")

        # Credit Bal
        with ui.tab_panel("Credit Bal"):
            ui.label("Credit Balances & Utilization").classes("my-title")
            months = build_month_options(past_months=12, future_months=0)
            msel = ui.select(months, value=months[-1], label="Month").classes("w-[220px]")

            tx = STATE.tx_df.copy()
            if tx.empty:
                ui.label("No data yet.").classes("my-muted")
            else:
                try:
                    bal_events = compute_balance_events(tx)
                    ut = util_table(bal_events, msel.value, STATE.acct_df) if not STATE.acct_df.empty else pd.DataFrame()
                    if ut is None or ut.empty:
                        ui.label("No utilization data (check cards tab limits/billing day).").classes("my-muted")
                    else:
                        ui.table(
                            columns=[{"name": c, "label": c, "field": c} for c in ut.columns],
                            rows=ut.to_dict("records"),
                            row_key=ut.columns[0],
                        ).classes("w-full my-table")
                except Exception as e:
                    notify_error(e, "Utilization failed: ")

                try:
                    upcoming = upcoming_recurring_total(STATE.prefs_list, msel.value)
                    with ui.card().classes("my-card flat p-4 mt-3"):
                        ui.label("Upcoming recurring (month)").classes("my-muted my-sub")
                        ui.label(money(float(upcoming))).classes("kpi")
                except Exception:
                    pass

        # Trends
        with ui.tab_panel("Trends"):
            ui.label("Trends").classes("my-title")
            tx = STATE.tx_df.copy()
            if tx.empty:
                ui.label("No data yet.").classes("my-muted")
            else:
                g = tx.groupby(["Month", "Type"])["Amount"].sum().reset_index()
                g["TypeLabel"] = g["Type"].apply(type_to_display)
                fig = px.line(g, x="Month", y="Amount", color="TypeLabel", markers=True, title="Monthly Trends")
                fig.update_layout(paper_bgcolor="#070B12", plot_bgcolor="#070B12", font_color="#E9EEF7")
                ui.plotly(fig).classes("w-full")

        # Transactions
        with ui.tab_panel("Transactions"):
            ui.label("Transactions").classes("my-title")
            if STATE.tx_df.empty:
                ui.label("No transactions yet.").classes("my-muted")
            else:
                months = build_month_options(past_months=12, future_months=3)
                msel = ui.select(["All"] + months, value="All", label="Month").classes("w-[220px]")
                owners = sorted([o for o in STATE.tx_df["Owner"].astype(str).unique().tolist() if o and o != "nan"])
                osel = ui.select(["All"] + owners, value="All", label="Owner").classes("w-[220px]")

                df = STATE.tx_df.copy()
                if msel.value != "All":
                    df = df[df["Month"] == msel.value]
                if osel.value != "All":
                    df = df[df["Owner"] == osel.value]
                df = df.sort_values(["DateParsed", "CreatedAt"], ascending=[False, False])

                show = df[["Date","Owner","Type","Amount","Pay","Account","Category","Notes","AutoTag","_row"]].copy()
                show["Type"] = show["Type"].apply(type_to_display)
                show["Amount"] = show["Amount"].apply(money)

                ui.table(
                    columns=[{"name": c, "label": c, "field": c} for c in show.columns if c != "_row"],
                    rows=show.drop(columns=["_row"]).to_dict("records"),
                    row_key="Date",
                ).classes("w-full my-table")

                row_in = ui.number(label="Row # (_row)", value=0, format="%.0f").classes("w-[200px]")

                def do_delete():
                    try:
                        rn = int(row_in.value or 0)
                        if rn < 2:
                            raise ValueError("Enter a valid row number")
                        row = STATE.tx_df[STATE.tx_df["_row"] == rn]
                        if not row.empty:
                            d = _parse_date_any(row.iloc[0]["Date"])
                            _assert_month_unlocked(_month_of(d))
                        delete_transaction(rn)
                        ui.notify(f"Deleted row {rn}", type="positive")
                        ui.navigate.to("/app")
                    except Exception as e:
                        notify_error(e, "Delete failed: ")

                def do_export():
                    try:
                        out = df.copy()
                        out = out[TX_HEADERS] if all(c in out.columns for c in TX_HEADERS) else out
                        csv_bytes = out.to_csv(index=False).encode("utf-8")
                        ui.download(csv_bytes, filename=f"myfin_export_{dt.date.today().isoformat()}.csv")
                    except Exception as e:
                        notify_error(e, "Export failed: ")

                def do_edit():
                    try:
                        rn = int(row_in.value or 0)
                        if rn < 2:
                            raise ValueError("Enter a valid row number")
                        row = STATE.tx_df[STATE.tx_df["_row"] == rn]
                        if row.empty:
                            raise ValueError("Row not found. Refresh.")
                        r = row.iloc[0].to_dict()

                        dlg = ui.dialog()
                        with dlg, ui.card().classes("my-card p-5 w-[680px] max-w-[95vw]"):
                            ui.label(f"Edit Row {rn}").classes("text-lg font-bold")

                            d_date = ui.date(value=str(r.get("Date",""))[:10] or dt.date.today().isoformat(), label="Date").classes("w-full")
                            d_owner = ui.input("Owner", value=str(r.get("Owner",""))).classes("w-full")
                            d_type = ui.input("Type", value=str(r.get("Type",""))).classes("w-full")
                            d_amount = ui.number("Amount", value=float(r.get("Amount",0) or 0), format="%.2f").classes("w-full")
                            d_pay = ui.input("Pay", value=str(r.get("Pay",""))).classes("w-full")
                            d_acct = ui.input("Account", value=str(r.get("Account",""))).classes("w-full")
                            d_cat = ui.input("Category", value=str(r.get("Category",""))).classes("w-full")
                            d_notes = ui.textarea("Notes", value=str(r.get("Notes",""))).classes("w-full")
                            d_tag = ui.input("AutoTag", value=str(r.get("AutoTag",""))).classes("w-full")

                            with ui.row().classes("w-full justify-end gap-2"):
                                ui.button("Cancel", on_click=dlg.close).classes("my-pill")
                                def save():
                                    try:
                                        date_parsed = dt.date.fromisoformat(d_date.value)
                                        _assert_month_unlocked(_month_of(date_parsed))
                                        update_transaction(rn, {
                                            "Date": date_parsed.isoformat(),
                                            "Owner": d_owner.value,
                                            "Type": d_type.value,
                                            "Amount": f"{float(d_amount.value or 0):.2f}",
                                            "Pay": d_pay.value,
                                            "Account": normalize_account_name(d_acct.value),
                                            "Category": d_cat.value,
                                            "Notes": d_notes.value,
                                            "AutoTag": d_tag.value,
                                        })
                                        ui.notify("Updated", type="positive")
                                        dlg.close()
                                        ui.navigate.to("/app")
                                    except Exception as e:
                                        notify_error(e, "Update failed: ")
                                ui.button("Save", on_click=save).classes("my-pill")
                        dlg.open()
                    except Exception as e:
                        notify_error(e, "Edit failed: ")

                with ui.row().classes("w-full gap-2 items-end mt-2"):
                    ui.button("Edit", on_click=do_edit).classes("my-pill")
                    ui.button("Delete", on_click=do_delete).classes("my-pill")
                    ui.button("Export CSV", on_click=do_export).classes("my-pill")

        # Admin
        with ui.tab_panel("Admin"):
            ui.label("Admin").classes("my-title")
            ui.label("Locks • Rules • Recurring • Accounts • Fix Mistakes").classes("my-muted my-sub")

            sub = ui.tabs().classes("w-full mt-2")
            with sub:
                ui.tab("Locks")
                ui.tab("Rules")
                ui.tab("Recurring")
                ui.tab("Accounts")
                ui.tab("Fix Mistakes")

            with ui.tab_panels(sub, value="Locks").classes("w-full mt-3"):
                with ui.tab_panel("Locks"):
                    lock_in = ui.input("Locked months (comma separated YYYY-MM)", value=", ".join(STATE.locked_months)).classes("w-full")
                    def save_locks():
                        try:
                            locks = [x.strip() for x in (lock_in.value or "").split(",") if x.strip()]
                            save_admin(locks, current_rules_text_from_state(), STATE.rules_locked, STATE.prefs_list)
                            ui.notify("Saved locks", type="positive")
                            ui.navigate.to("/app")
                        except Exception as e:
                            notify_error(e, "Save locks failed: ")
                    ui.button("Save locks", on_click=save_locks).classes("my-pill")

                with ui.tab_panel("Rules"):
                    ta = ui.textarea("Rules (Category: keyword1, keyword2)", value=current_rules_text_from_state()).classes("w-full")
                    lock_sw = ui.switch("Lock rules editing", value=STATE.rules_locked)
                    def save_rules():
                        try:
                            save_admin(STATE.locked_months, ta.value or "", bool(lock_sw.value), STATE.prefs_list)
                            ui.notify("Saved rules", type="positive")
                            ui.navigate.to("/app")
                        except Exception as e:
                            notify_error(e, "Save rules failed: ")
                    ui.button("Save rules", on_click=save_rules).classes("my-pill")

                with ui.tab_panel("Recurring"):
                    dfp = pd.DataFrame(STATE.prefs_list or [])
                    if not dfp.empty:
                        ui.table(columns=[{"name": c, "label": c, "field": c} for c in dfp.columns],
                                 rows=dfp.to_dict("records"),
                                 row_key=(dfp.columns[0] if len(dfp.columns) else "MerchantKey")).classes("w-full my-table")
                    else:
                        ui.label("No recurring templates yet.").classes("my-muted")

                    with ui.card().classes("my-card p-4 mt-3"):
                        ui.label("Add / Update template").classes("font-semibold")
                        isrec = ui.switch("IsRecurring", value=True)
                        mk = ui.input("MerchantKey (unique)", value="").classes("w-full")
                        dom = ui.number("DayOfMonth", value=1, format="%.0f").classes("w-full")
                        owner = ui.input("Owner", value="Family").classes("w-full")
                        rtype = ui.select(["Debit","Credit","Investment","CC Repay","International"], value="Debit", label="Type").classes("w-full")
                        amt = ui.number("Amount", value=0.0, format="%.2f").classes("w-full")
                        pay = ui.input("Pay", value="Card").classes("w-full")
                        acct = ui.select(allowed_accounts_live or [""], value=(allowed_accounts_live[0] if allowed_accounts_live else ""), label="Account").classes("w-full")
                        cat = ui.input("Category", value="Uncategorized").classes("w-full")
                        notes = ui.input("Notes", value="").classes("w-full")

                        def upsert_template():
                            try:
                                plist = list(STATE.prefs_list or [])
                                key = (mk.value or "").strip()
                                if not key:
                                    raise ValueError("MerchantKey required")
                                item = {
                                    "IsRecurring": bool(isrec.value),
                                    "MerchantKey": key,
                                    "DayOfMonth": int(dom.value or 1),
                                    "Owner": owner.value or "Family",
                                    "Type": rtype.value or "Debit",
                                    "Amount": float(amt.value or 0),
                                    "Pay": pay.value or "",
                                    "Account": acct.value or "",
                                    "Category": cat.value or "Uncategorized",
                                    "Notes": notes.value or key,
                                }
                                for i, p in enumerate(plist):
                                    if str(p.get("MerchantKey", "")).strip() == key:
                                        plist[i] = item
                                        break
                                else:
                                    plist.append(item)
                                save_admin(STATE.locked_months, current_rules_text_from_state(), STATE.rules_locked, plist)
                                ui.notify("Saved template", type="positive")
                                ui.navigate.to("/app")
                            except Exception as e:
                                notify_error(e, "Save template failed: ")

                        def delete_template():
                            try:
                                key = (mk.value or "").strip()
                                if not key:
                                    raise ValueError("MerchantKey required")
                                plist = [p for p in (STATE.prefs_list or []) if str(p.get("MerchantKey","")).strip() != key]
                                save_admin(STATE.locked_months, current_rules_text_from_state(), STATE.rules_locked, plist)
                                ui.notify("Deleted template", type="positive")
                                ui.navigate.to("/app")
                            except Exception as e:
                                notify_error(e, "Delete template failed: ")

                        with ui.row().classes("w-full gap-2"):
                            ui.button("Save template", on_click=upsert_template).classes("my-pill")
                            ui.button("Delete template", on_click=delete_template).classes("my-pill")

                with ui.tab_panel("Accounts"):
                    df = STATE.acct_df.copy()
                    if not df.empty:
                        ui.table(columns=[{"name": c, "label": c, "field": c} for c in ACCT_HEADERS],
                                 rows=df.to_dict("records"),
                                 row_key="Account").classes("w-full my-table")
                    else:
                        ui.label("No accounts found.").classes("my-muted")

                    with ui.card().classes("my-card p-4 mt-3"):
                        ui.label("Add / Update account").classes("font-semibold")
                        a = ui.input("Account", value="").classes("w-full")
                        e = ui.input("Emoji", value="").classes("w-full")
                        lim = ui.number("Limit", value=0.0, format="%.2f").classes("w-full")
                        bd = ui.number("BillingDay", value=1, format="%.0f").classes("w-full")

                        def save_account():
                            try:
                                acc = (a.value or "").strip()
                                if not acc:
                                    raise ValueError("Account required")
                                d = STATE.acct_df.copy()
                                if d.empty:
                                    d = pd.DataFrame(columns=ACCT_HEADERS)
                                row = {"Account": acc, "Emoji": (e.value or "").strip(), "Limit": float(lim.value or 0), "BillingDay": int(bd.value or 1)}
                                if (d["Account"].astype(str) == acc).any():
                                    d.loc[d["Account"].astype(str) == acc, ["Emoji","Limit","BillingDay"]] = [row["Emoji"], row["Limit"], row["BillingDay"]]
                                else:
                                    d = pd.concat([d, pd.DataFrame([row])], ignore_index=True)
                                save_accounts(d)
                                ui.notify("Saved account", type="positive")
                                ui.navigate.to("/app")
                            except Exception as ex:
                                notify_error(ex, "Save account failed: ")
                        ui.button("Save account", on_click=save_account).classes("my-pill")

                with ui.tab_panel("Fix Mistakes"):
                    ui.label("Edit/delete any row by row number (_row).").classes("my-muted my-sub")
                    rn = ui.number("Row # (_row)", value=0, format="%.0f").classes("w-[240px]")

                    def open_edit():
                        try:
                            rownum = int(rn.value or 0)
                            if rownum < 2:
                                raise ValueError("Enter a valid row number")
                            row = STATE.tx_df[STATE.tx_df["_row"] == rownum]
                            if row.empty:
                                raise ValueError("Row not found. Refresh.")
                            r = row.iloc[0].to_dict()

                            dlg = ui.dialog()
                            with dlg, ui.card().classes("my-card p-5 w-[680px] max-w-[95vw]"):
                                ui.label(f"Edit Row {rownum}").classes("text-lg font-bold")
                                d_date = ui.date(value=str(r.get("Date",""))[:10] or dt.date.today().isoformat(), label="Date").classes("w-full")
                                d_owner = ui.input("Owner", value=str(r.get("Owner",""))).classes("w-full")
                                d_type = ui.input("Type", value=str(r.get("Type",""))).classes("w-full")
                                d_amount = ui.number("Amount", value=float(r.get("Amount",0) or 0), format="%.2f").classes("w-full")
                                d_pay = ui.input("Pay", value=str(r.get("Pay",""))).classes("w-full")
                                d_acct = ui.input("Account", value=str(r.get("Account",""))).classes("w-full")
                                d_cat = ui.input("Category", value=str(r.get("Category",""))).classes("w-full")
                                d_notes = ui.textarea("Notes", value=str(r.get("Notes",""))).classes("w-full")
                                d_tag = ui.input("AutoTag", value=str(r.get("AutoTag",""))).classes("w-full")

                                with ui.row().classes("w-full justify-end gap-2"):
                                    ui.button("Cancel", on_click=dlg.close).classes("my-pill")
                                    def save():
                                        try:
                                            date_parsed = dt.date.fromisoformat(d_date.value)
                                            _assert_month_unlocked(_month_of(date_parsed))
                                            update_transaction(rownum, {
                                                "Date": date_parsed.isoformat(),
                                                "Owner": d_owner.value,
                                                "Type": d_type.value,
                                                "Amount": f"{float(d_amount.value or 0):.2f}",
                                                "Pay": d_pay.value,
                                                "Account": normalize_account_name(d_acct.value),
                                                "Category": d_cat.value,
                                                "Notes": d_notes.value,
                                                "AutoTag": d_tag.value,
                                            })
                                            ui.notify("Updated", type="positive")
                                            dlg.close()
                                            ui.navigate.to("/app")
                                        except Exception as e:
                                            notify_error(e, "Update failed: ")
                                    ui.button("Save", on_click=save).classes("my-pill")
                            dlg.open()
                        except Exception as e:
                            notify_error(e, "Edit failed: ")

                    def do_delete():
                        try:
                            rownum = int(rn.value or 0)
                            if rownum < 2:
                                raise ValueError("Enter a valid row number")
                            row = STATE.tx_df[STATE.tx_df["_row"] == rownum]
                            if not row.empty:
                                d = _parse_date_any(row.iloc[0]["Date"])
                                _assert_month_unlocked(_month_of(d))
                            delete_transaction(rownum)
                            ui.notify("Deleted", type="positive")
                            ui.navigate.to("/app")
                        except Exception as e:
                            notify_error(e, "Delete failed: ")

                    with ui.row().classes("gap-2"):
                        ui.button("Edit row", on_click=open_edit).classes("my-pill")
                        ui.button("Delete row", on_click=do_delete).classes("my-pill")


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), reload=False, title=APP_NAME)
