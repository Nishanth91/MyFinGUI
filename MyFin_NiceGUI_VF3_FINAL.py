# ==============================
# MyFin App – Phase 4.3
# Base: Phase 4.2.1 (Stable)
# Notes: UI/UX refinement phase
# ==============================

from __future__ import annotations
#"""
#NishanthFinTrack 2026 — Premium Dark • Fast • Google Sheets
#FINALIZED single-file app.py (end-to-end)
#
#Includes (fully):
#- All Corrections (C1–C8) + Enhancements (E1–E10) you listed
#- All additional enhancements suggested: Quick Add defaults, keyboard-first, soft #toasts, bill-cycle utilization,
#  safe-to-spend, utilization warnings, hero insight, MoM deltas, undo, confidence #tags, Admin Insights & Rules lock,
#  backup export, smooth-ish UX (no chip multiselect; Apply filters).
#- Major performance improvements: session-state data store + explicit Refresh + #Apply filters + isolated forms.
#  No heavy reruns on every selection; Sheets is re-read only on explicit refresh or #after write operations.
#
#Prereqs:
#- .streamlit/secrets.toml must contain:
#  [gcp_service_account]
#  type="service_account"
#  project_id="..."
#  private_key_id="..."
#  private_key="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
#  client_email="...@....iam.gserviceaccount.com"
#  client_id="..."
#  token_uri="https://oauth2.googleapis.com/token"
#  ...
#
#- Create a Google Sheet named SHEET_NAME (or change it) and share it with #client_email.
#Run:
#  streamlit run app.py
#"""

import calendar
import hmac
import json
import re
import uuid
import time
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials
from dateutil.relativedelta import relativedelta
import datetime as dt

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

def is_mobile_view():
    # Mobile-specific view is disabled for now to avoid navigation issues.
    return False

# =============================
# Helpers
# =============================
def money(n: float) -> str:
    sign = "-" if float(n or 0) < 0 else ""
    n = abs(float(n or 0))
    return f"{sign}${n:,.2f}"

def cat_label(name: str) -> str:
    return f"{CATEGORY_ICON.get(name,'•')} {name}"

def clamp_day(year: int, month: int, day: int) -> int:
    last = calendar.monthrange(year, month)[1]
    return max(1, min(int(day), last))

def auth_ok(u: str, p: str) -> bool:
    return hmac.compare_digest(u or "", AUTH_USERNAME) and hmac.compare_digest(p or "", AUTH_PASSWORD)

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

def classify(notes: str, rules: Dict[str, List[str]]) -> str:
    t = (notes or "").lower()
    for label, keys in rules.items():
        if label == "Uncategorized":
            continue
        for k in keys:
            if k and k.lower() in t:
                return label
    return "Uncategorized"

def segmented(label: str, options: List[str], default: str, key: str):
    if hasattr(st, "segmented_control"):
        return st.segmented_control(label, options, default=default, key=key)
    idx = options.index(default) if default in options else 0
    return st.radio(label, options, index=idx, horizontal=True, key=key)

def prev_month_str(m: str) -> str:
    try:
        p = pd.Period(m, freq="M")
        return str(p - 1)
    except Exception:
        return m

def delta_badge(delta: float) -> str:
    if abs(delta) < 0.005:
        return "—"
    return ("▲ " if delta > 0 else "▼ ") + money(abs(delta))


# =============================
# Google Sheets schema
# =============================
TAB_TRANSACTIONS = "transactions"
TAB_ACCOUNTS = "cards"
TAB_ADMIN = "admin"

TX_HEADERS = ["TxId", "Date", "Owner", "Type", "Amount", "Pay", "Account", "Category", "Notes", "CreatedAt", "AutoTag"]
ACCT_HEADERS = ["Account", "Emoji", "Limit", "BillingDay"]
ADMIN_HEADERS = ["Key", "Value"]  # locked_months, rules_text, rules_locked, recurring_prefs_json


# =============================
# Google auth
# =============================
@st.cache_resource
def gclient() -> gspread.Client:
    if "gcp_service_account" not in st.secrets:
        st.error("Missing Streamlit secrets: [gcp_service_account].")
        st.stop()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource
def open_sheet() -> gspread.Spreadsheet:
    return gclient().open(SHEET_NAME)


def gs_call(fn, *args, **kwargs):
    """Google Sheets API call with exponential backoff on 429/5xx errors.

    HF8: Streamlit reruns can spike read requests; this wrapper reduces crashes by retrying
    with jittered exponential backoff.
    """
    last_err = None
    for attempt in range(8):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            last_err = e
            s = str(e)
            # Quota / rate limit or transient backend errors
            if ("[429]" in s) or ("429" in s) or ("Quota exceeded" in s) or ("Read requests" in s) or ("[500]" in s) or ("[503]" in s):
                # jittered exponential backoff, capped
                sleep_s = min(30.0, (1.0 * (2 ** attempt)) + random.random())
                time.sleep(sleep_s)
                continue
            raise
    raise last_err

def _get_ws_map(ss: gspread.Spreadsheet) -> Dict[str, gspread.Worksheet]:
    """HF8: Cache worksheet objects to avoid repeated spreadsheet metadata reads."""
    ws_map = st.session_state.get("_ws_map")
    if ws_map is None or st.session_state.get("_ws_map_id") != ss.id:
        wss = gs_call(ss.worksheets)  # one metadata call
        ws_map = {w.title: w for w in wss}
        st.session_state["_ws_map"] = ws_map
        st.session_state["_ws_map_id"] = ss.id
    return ws_map


def ensure_ws(ss: gspread.Spreadsheet, title: str, headers: List[str], rows: int = 2000) -> gspread.Worksheet:
    """Ensure worksheet exists and headers are correct (header check only once per session).

    HF8: Uses cached worksheet map to reduce Google Sheets read requests.
    """
    ws_map = _get_ws_map(ss)

    if title in ws_map:
        ws = ws_map[title]
        created = False
    else:
        ws = gs_call(ss.add_worksheet, title=title, rows=rows, cols=max(25, len(headers) + 10))
        created = True
        # refresh map once after creation
        st.session_state.pop("_ws_map", None)
        ws_map = _get_ws_map(ss)
        ws_map[title] = ws
        st.session_state["_ws_map"] = ws_map

    ensured = st.session_state.setdefault("_ensured_headers", set())
    if created or title not in ensured:
        row1 = gs_call(ws.row_values, 1)
        if row1 != headers:
            gs_call(ws.update, "A1", [headers])
        ensured.add(title)
    return ws



# =============================
# Admin storage
# =============================
def ensure_admin_defaults(ws_admin: gspread.Worksheet) -> None:
    data = ws_admin.get_all_records()
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
        ws_admin.append_rows(updates, value_input_option="USER_ENTERED")

def get_admin_value(ws_admin: gspread.Worksheet, key: str) -> str:
    for r in ws_admin.get_all_records():
        if str(r.get("Key", "")).strip() == key:
            return str(r.get("Value", "") or "")
    return ""

def set_admin_value(ws_admin: gspread.Worksheet, key: str, value: str) -> None:
    rows = ws_admin.get_all_records()
    idx = None
    for i, r in enumerate(rows, start=2):
        if str(r.get("Key", "")).strip() == key:
            idx = i
            break
    if idx is None:
        ws_admin.append_row([key, value], value_input_option="USER_ENTERED")
    else:
        ws_admin.update(f"A{idx}:B{idx}", [[key, value]])

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
def ss_get(key: str, default):
    if key not in st.session_state:
        st.session_state[key] = default
    return st.session_state[key]

def set_last_sync(ts: datetime):
    st.session_state["last_sync_at"] = ts

def last_sync_text() -> str:
    ts = st.session_state.get("last_sync_at")
    if not ts:
        return "Not synced yet"
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def refresh_admin_from_sheets() -> None:
    """Read Admin sheet (small)."""
    ss = open_sheet()
    ws_admin = ensure_ws(ss, TAB_ADMIN, ADMIN_HEADERS, rows=400)
    ensure_admin_defaults(ws_admin)

    locked_raw = get_admin_value(ws_admin, "locked_months").strip()
    locked_months = [x.strip() for x in locked_raw.split(",") if x.strip()]

    rules_text = get_admin_value(ws_admin, "rules_text")
    rules = parse_rules_text(rules_text)

    rules_locked = (get_admin_value(ws_admin, "rules_locked").strip().lower() == "true")

    prefs_raw = get_admin_value(ws_admin, "recurring_prefs_json").strip() or "[]"
    try:
        prefs = json.loads(prefs_raw)
        if not isinstance(prefs, list):
            prefs = []
    except Exception:
        prefs = []

    st.session_state["rules"] = rules
    st.session_state["locked_months"] = locked_months
    st.session_state["rules_locked"] = rules_locked
    st.session_state["prefs_list"] = prefs
    st.session_state["admin_dirty"] = False
    set_last_sync(datetime.utcnow())


def refresh_accounts_from_sheets() -> None:
    """Read Accounts sheet (small)."""
    ss = open_sheet()
    ws_acct = ensure_ws(ss, TAB_ACCOUNTS, ACCT_HEADERS, rows=200)
    rows = gs_call(ws_acct.get_all_records)
    acct_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=ACCT_HEADERS)

    acct_df["Account"] = acct_df["Account"].astype(str) if "Account" in acct_df.columns else ""
    acct_df["Emoji"] = acct_df["Emoji"].astype(str).replace({"": "💳"}) if "Emoji" in acct_df.columns else "💳"
    acct_df["Limit"] = pd.to_numeric(acct_df.get("Limit", 0), errors="coerce").fillna(0.0)
    acct_df["BillingDay"] = pd.to_numeric(acct_df.get("BillingDay", 1), errors="coerce").fillna(1).astype(int)

    st.session_state["acct_df"] = acct_df
    st.session_state["accounts_dirty"] = False
    set_last_sync(datetime.utcnow())


def refresh_transactions_from_sheets() -> None:
    """Read Transactions sheet (largest).

    HF7: Make header handling robust.
    - If sheet headers differ by casing/spacing or common synonyms, map them.
    - If headers are unrecognized but column counts match, assume the sheet is already in TX_HEADERS order.
    """
    ss = open_sheet()
    ws_tx = ensure_ws(ss, TAB_TRANSACTIONS, TX_HEADERS, rows=10000)
    values = gs_call(ws_tx.get_all_values)

    def _norm(h: str) -> str:
        return re.sub(r"\s+", "", str(h or "").strip().lower())

    # Common header synonyms from older versions / user-edits
    synonyms = {
        "txid": "TxId",
        "transactionid": "TxId",
        "transaction_id": "TxId",
        "id": "TxId",
        "created": "CreatedAt",
        "createdat": "CreatedAt",
        "created_at": "CreatedAt",
        "autotag": "AutoTag",
        "auto_tag": "AutoTag",
        "merchant": "Notes",
        "reason": "Notes",
        "description": "Notes",
        "payment": "Pay",
        "paymenttype": "Pay",
        "paymentmethod": "Pay",
    }
    norm_to_canon = {_norm(k): v for k, v in synonyms.items()}
    for c in TX_HEADERS:
        norm_to_canon[_norm(c)] = c  # exact canonical

    if not values or len(values) < 2:
        tx_df = pd.DataFrame(columns=TX_HEADERS + ["_row", "Month"])
    else:
        raw_hdr = values[0]
        rows = values[1:]

        # Build mapping from sheet columns -> canonical TX_HEADERS
        sheet_norm = [_norm(h) for h in raw_hdr]
        mapped_cols = [norm_to_canon.get(n, "") for n in sheet_norm]

        # Case 1: We can map at least Date/Amount (minimum viable)
        can_map = ("Date" in mapped_cols) and ("Amount" in mapped_cols)

        if can_map:
            df_raw = pd.DataFrame(rows, columns=raw_hdr)
            df = pd.DataFrame()
            for canon in TX_HEADERS:
                try:
                    src_idx = mapped_cols.index(canon)
                    df[canon] = df_raw[raw_hdr[src_idx]]
                except ValueError:
                    df[canon] = ""
        else:
            # Case 2: headers unrecognized; fall back to positional if widths align
            if len(raw_hdr) >= len(TX_HEADERS):
                df = pd.DataFrame([r[:len(TX_HEADERS)] for r in rows], columns=TX_HEADERS)
            else:
                padded = [r + [""] * (len(TX_HEADERS) - len(r)) for r in rows]
                df = pd.DataFrame(padded, columns=TX_HEADERS)

        df["_row"] = [i + 2 for i in range(len(df))]
        tx_df = df

        tx_df["Date"] = pd.to_datetime(tx_df["Date"], errors="coerce").dt.normalize()
        tx_df["Amount"] = pd.to_numeric(tx_df["Amount"], errors="coerce").fillna(0.0)

        if "Month" not in tx_df.columns:
            tx_df["Month"] = ""
        tx_df["Month"] = tx_df["Date"].dt.to_period("M").astype(str)

    st.session_state["tx_df"] = tx_df
    st.session_state["tx_dirty"] = False
    set_last_sync(datetime.utcnow())


def refresh_all_from_sheets() -> None:
    """Explicit heavy read."""
    refresh_admin_from_sheets()
    refresh_accounts_from_sheets()
    refresh_transactions_from_sheets()


def admin_update(key: str, value: str) -> None:
    ss = open_sheet()
    ws_admin = ensure_ws(ss, TAB_ADMIN, ADMIN_HEADERS, rows=400)
    ensure_admin_defaults(ws_admin)
    set_admin_value(ws_admin, key, value)

def admin_update_and_refresh(key: str, value: str) -> None:
    admin_update(key, value)
    st.session_state["admin_dirty"] = True
    refresh_admin_from_sheets()

def save_accounts(df: pd.DataFrame) -> None:
    ss = open_sheet()
    ws = ensure_ws(ss, TAB_ACCOUNTS, ACCT_HEADERS, rows=200)
    gs_call(ws.clear)
    gs_call(ws.update, "A1", [ACCT_HEADERS])
    gs_call(ws.append_rows, df[ACCT_HEADERS].values.tolist(), value_input_option="USER_ENTERED")
    st.session_state["accounts_dirty"] = True
    refresh_accounts_from_sheets()

def append_transaction(entry_date: date, entry_type: str, amount: float, pay: str, account: str,
                       category: str, notes: str, auto_tag: str = "") -> str:
    ss = open_sheet()
    ws = ensure_ws(ss, TAB_TRANSACTIONS, TX_HEADERS, rows=10000)
    txid = str(uuid.uuid4())
    ws.append_row(
        [
            txid,
            entry_date.isoformat(),
            "Family",
            entry_type,
            float(amount),
            pay,
            account or "",
            category,
            notes or "",
            datetime.utcnow().isoformat(timespec="seconds"),
            auto_tag or "",
        ],
        value_input_option="USER_ENTERED",
    )
    refresh_all_from_sheets()
    return txid

def delete_transaction_by_row(row_num: int) -> None:
    ss = open_sheet()
    ws = ensure_ws(ss, TAB_TRANSACTIONS, TX_HEADERS, rows=10000)
    gs_call(ws.delete_rows, row_num)
    st.session_state["tx_dirty"] = True
    refresh_transactions_from_sheets()

def update_transaction_by_row(row_num: int, values: Dict[str, object]) -> None:
    ss = open_sheet()
    ws = ensure_ws(ss, TAB_TRANSACTIONS, TX_HEADERS, rows=10000)
    row = [
        str(values.get("TxId", "")),
        str(values.get("Date", "")),
        "Family",
        str(values.get("Type", "")),
        float(values.get("Amount", 0.0) or 0.0),
        str(values.get("Pay", "")),
        str(values.get("Account", "")),
        str(values.get("Category", "")),
        str(values.get("Notes", "")),
        str(values.get("CreatedAt", "")),
        str(values.get("AutoTag", "")),
    ]
    ws.update(f"A{row_num}:K{row_num}", [row], value_input_option="USER_ENTERED")
    st.session_state["tx_dirty"] = True
    refresh_transactions_from_sheets()


# =============================
# Recurring
# =============================
def upsert_pref(prefs_list: List[dict], pref: dict) -> List[dict]:
    mk = str(pref.get("MerchantKey", "")).strip()
    if not mk:
        return prefs_list
    out = []
    replaced = False
    for r in prefs_list:
        if str(r.get("MerchantKey", "")).strip() == mk:
            out.append(pref)
            replaced = True
        else:
            out.append(r)
    if not replaced:
        out.append(pref)
    return out

def ensure_recurring_for_month(month_str: str) -> int:
    """Auto-add recurring items for month_str if missing. Runs when app opens or month changes."""
    tx = st.session_state["tx_df"]
    prefs_list = st.session_state["prefs_list"]
    try:
        p = pd.Period(month_str, freq="M")
    except Exception:
        return 0

    year, month = p.year, p.month
    existing_tags = set(tx.loc[(tx["Month"] == month_str) & (tx["AutoTag"].str.startswith("AUTO:")), "AutoTag"].tolist())
    created = 0

    for pref in prefs_list:
        if not bool(pref.get("IsRecurring", False)):
            continue
        mk = str(pref.get("MerchantKey", "")).strip()
        if not mk:
            continue
        dom = pref.get("DayOfMonth", 1)
        try:
            dom = int(float(dom)) if dom is not None and str(dom).strip() != "" else 1
        except Exception:
            dom = 1
        dom = clamp_day(year, month, dom)

        tag = f"AUTO:{mk}:{month_str}"
        if tag in existing_tags:
            continue

        amt = pref.get("Amount", None)
        try:
            amt = float(amt) if amt is not None and str(amt).strip() != "" else None
        except Exception:
            amt = None
        if amt is None or amt <= 0:
            continue

        cat = str(pref.get("Category", "")).strip() or "Uncategorized"
        pay = str(pref.get("Pay", "")).strip() or "Bank"
        acct = str(pref.get("Account", "")).strip()
        nick = str(pref.get("Nickname", "")).strip() or mk.title()

        due = date(year, month, dom)
        note = f"[AUTO] {nick}"

        append_transaction(due, "Debit", amt, pay, acct, cat, note, auto_tag=tag)
        created += 1

    return created


# =============================
# Credit cycle / utilization
# =============================
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

def push_undo(action: UndoAction):
    st.session_state["undo"] = action

def confidence_tag(auto_tag: str) -> str:
    tag = (auto_tag or "").strip()
    if tag.startswith("AUTO:"):
        return "🔁 Auto-recurring"
    if tag.startswith("RULE:"):
        return "🧠 Auto-categorized"
    if tag.startswith("MANUAL:"):
        return "✍️ Manual"
    return "—"

def render_undo():
    ua: Optional[UndoAction] = st.session_state.get("undo")
    if not ua:
        return
    c1, c2 = st.columns([1, 5])
    with c1:
        if st.button("Undo", key="undo_btn"):
            try:
                if ua.kind == "add" and ua.txid:
                    df = st.session_state["tx_df"]
                    hit = df[df["TxId"] == ua.txid]
                    if not hit.empty:
                        delete_transaction_by_row(int(hit.iloc[0]["_row"]))
                elif ua.kind == "edit" and ua.row_num and ua.old_row:
                    r = ua.old_row
                    update_transaction_by_row(ua.row_num, {
                        "TxId": r[0], "Date": r[1], "Type": r[3], "Amount": float(r[4]),
                        "Pay": r[5], "Account": r[6], "Category": r[7], "Notes": r[8],
                        "CreatedAt": r[9], "AutoTag": r[10],
                    })
                elif ua.kind == "delete" and ua.row_num and ua.old_row:
                    # insert row back at position
                    ss = open_sheet()
                    ws = ensure_ws(ss, TAB_TRANSACTIONS, TX_HEADERS, rows=10000)
                    ws.insert_row(ua.old_row, index=ua.row_num)
                    refresh_all_from_sheets()
                st.session_state["undo"] = None
                st.toast("Undone ✅", icon="↩️")
                st.rerun()
            except Exception as e:
                st.error(f"Undo failed: {e}")
    with c2:
        st.caption("Undo last action (until the next action).")


# =============================
# Login
# =============================
def require_login():
    if st.session_state.get("authed", False):
        return

    # Lightweight login UI (mobile-friendly)
    st.markdown(f"## 🔐 {APP_NAME}  ·  {APP_VERSION}")
    st.caption("Sign in to continue.")

    with st.form("login_form", clear_on_submit=False, border=True):
        u = st.text_input("Username", value="", autocomplete="username")
        p = st.text_input("Password", value="", type="password", autocomplete="current-password")
        ok = st.form_submit_button("Sign in", width="stretch")

    if ok:
        if auth_ok(u, p):
            st.session_state["authed"] = True
            st.toast("Signed in ✅", icon="✅")
            st.rerun()
        else:
            st.error("Invalid username or password.")

    st.stop()

require_login()




# =============================
# First sync (only once per session)
# =============================
if "tx_df" not in st.session_state and not st.session_state.get("did_initial_refresh"):
    # Mark immediately to prevent multiple rapid reruns from re-triggering a full refresh.
    st.session_state["did_initial_refresh"] = True
    try:
        refresh_all_from_sheets()
    except gspread.exceptions.APIError as e:
        # If Google throttles reads (429), show a friendly message instead of crashing.
        s = str(e)
        if ("[429]" in s) or ("429" in s) or ("Quota exceeded" in s) or ("Read requests" in s):
            st.error("Google Sheets rate limit reached (HTTP 429). Please wait ~60 seconds and click Refresh.")
            # Allow the user to retry manually.
            st.session_state["tx_df"] = pd.DataFrame(columns=TX_HEADERS + ["_row", "Month"])
            st.session_state["accounts_df"] = pd.DataFrame(columns=ACCT_HEADERS + ["_row"])
            st.session_state["admin_df"] = pd.DataFrame(columns=ADMIN_HEADERS + ["_row"])
            st.stop()
        raise

# Dirty-flag refreshes (avoid full re-read after small mutations)
if st.session_state.get("admin_dirty"):
    refresh_admin_from_sheets()
if st.session_state.get("accounts_dirty"):
    refresh_accounts_from_sheets()
if st.session_state.get("tx_dirty"):
    refresh_transactions_from_sheets()

rules = st.session_state["rules"]
locked_months = st.session_state["locked_months"]
rules_locked = st.session_state["rules_locked"]
prefs_list = st.session_state["prefs_list"]
acct_df = st.session_state["acct_df"]
tx_df = st.session_state["tx_df"]

allowed_accounts_live, emoji_map_live = build_account_maps(acct_df)


# =============================
# Sidebar nav + fast filters
# =============================
def current_month_default(df: pd.DataFrame) -> str:
    # Robust default: handle empty frames or missing derived columns
    cur = str(pd.Period(date.today(), freq="M"))
    if df is None or df.empty:
        return cur
    if "Month" not in df.columns:
        # Derive on the fly from Date if possible
        if "Date" in df.columns:
            d = pd.to_datetime(df["Date"], errors="coerce")
            months = sorted(d.dropna().dt.to_period("M").astype(str).unique().tolist())
            return months[-1] if months else cur
        return cur
    months = sorted(pd.Series(df["Month"]).dropna().unique().tolist())
    return months[-1] if months else cur

ss_get("flt_month", current_month_default(tx_df))
ss_get("flt_types", ENTRY_TYPES.copy())
ss_get("admin_section", "Monthly Lock")
ss_get("auto_recurring_done_for", "")

# Quick add defaults (suggestion #1)
ss_get("quick_defaults", {"Type": "Debit", "Pay": "Card", "Account": (allowed_accounts_live[0] if allowed_accounts_live else ALLOWED_ACCOUNTS[0]), "Category": "Uncategorized"})
ss_get("undo", None)

with st.sidebar:
    st.markdown(
        f"""
        <div class="mf-topbrand mf-anim">
          <div>
            <div style="font-size:18px; font-weight:950;">{APP_NAME}</div>
            <div style="font-size:12px; color:rgba(232,234,237,0.65); margin-top:2px;">Family ledger • 2026</div>
          </div>
          <div style="text-align:right;">
            <span class="mf-pill">Sheets</span>
            <div style="height:6px;"></div>
            <span class="mf-pill">Fast</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.write("")

    # sync bar (explicit, premium)
    st.markdown(
        f"""
        <div class="mf-sync mf-anim">
          <div>
            <div style="font-weight:950;">Sync</div>
            <small>Last: {last_sync_text()} UTC</small>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    cA, cB, cC = st.columns([1, 1, 1])
    with cA:
        if st.button("Refresh", width="stretch"):
            refresh_all_from_sheets()
            st.toast("Refreshed ✓", icon="🔄")
            st.rerun()
    with cB:
        if st.button("Logout", width="stretch"):
            st.session_state["authed"] = False
            st.rerun()
    with cC:
        view_mode = "Auto"  # locked to Auto (mobile toggle removed)
        st.markdown('<span class="pill pill-auto">Auto</span>', unsafe_allow_html=True)


    st.divider()
    months = sorted(tx_df["Month"].unique().tolist()) if not tx_df.empty else [str(pd.Period(date.today(), freq="M"))]

    # PERFORMANCE: Filters in form + Apply
    with st.form("sidebar_filters", border=False):
        st.markdown("### Filters")
        month_sel = st.selectbox("📅 Month", months, index=months.index(st.session_state["flt_month"]) if st.session_state["flt_month"] in months else len(months) - 1)

        st.markdown("### Focus (Types)")
        # C3 redesigned: premium toggles, no chips, no red
        selected = []
        cols = st.columns(2)
        for i, t in enumerate(ENTRY_TYPES):
            with cols[i % 2]:
                on = st.toggle(f"{TYPE_EMOJI[t]} {t}", value=(t in st.session_state["flt_types"]), key=f"tg_{t}")
            if on:
                selected.append(t)

        apply = st.form_submit_button("Apply")

    if apply:
        st.session_state["flt_month"] = month_sel
        st.session_state["flt_types"] = selected if selected else ENTRY_TYPES.copy()
        st.toast("Filters applied", icon="🎛️")
        st.rerun()

month_sel = st.session_state["flt_month"]
type_filter = st.session_state["flt_types"]

locked_now = month_sel in set(locked_months)
if locked_now:
    st.warning(f"🔒 **{month_sel} is LOCKED** — Add/Edit/Delete disabled for this month.")

# Auto-add recurring for chosen month (C6 + suggestion)
if st.session_state["auto_recurring_done_for"] != month_sel:
    created = ensure_recurring_for_month(month_sel)
    st.session_state["auto_recurring_done_for"] = month_sel
    if created > 0:
        st.toast(f"Auto-added {created} recurring item(s)", icon="🔁")
        st.rerun()

# Filtered views (cheap, in-memory)
view_df = tx_df[tx_df["Type"].isin(type_filter)].copy() if not tx_df.empty else tx_df.copy()
month_df = view_df[view_df["Month"] == month_sel].copy() if not view_df.empty else view_df.copy()


# =============================
# Dashboard helpers
# =============================
def _dash_type_series(df: pd.DataFrame) -> pd.Series:
    """Dashboard-only normalization of Type.

    We treat Salary/Payroll-like categories as Income even if they were entered as Debit by mistake,
    so the dashboard does not show Salary under Outflow / Highest debit spend.
    """
    if df is None or df.empty or "Type" not in df.columns:
        return pd.Series([], dtype=str)

    t = df["Type"].astype(str)

    if "Category" in df.columns:
        # Normalize category text so emojis / punctuation don't break matching.
        raw_cat = df["Category"].astype(str).fillna("").str.strip().str.lower()
        # Keep only letters/spaces for robust matching (e.g., "💼 Salary" -> "salary").
        norm_cat = raw_cat.str.replace(r"[^a-z\s]", " ", regex=True).str.replace(r"\s+", " ", regex=True).str.strip()

        # If category/notes look like common income labels, force as Credit for dashboard purposes.
        # We check both Category and Notes/Reason text because some rows may store "Salary" in notes.
        search_text = norm_cat

        # Try common note columns
        for note_col in ("Notes", "Reason/Notes", "Reason", "Description"):
            if note_col in df.columns:
                raw_note = df[note_col].astype(str).fillna("").str.strip().str.lower()
                norm_note = raw_note.str.replace(r"[^a-z\s]", " ", regex=True).str.replace(r"\s+", " ", regex=True).str.strip()
                search_text = (search_text + " " + norm_note).str.strip()
                break

        income_tokens = ("salary", "payroll", "pay cheque", "paycheck", "paycheque", "wages", "bonus")
        is_income = pd.Series(False, index=df.index)
        for tok in income_tokens:
            is_income = is_income | search_text.str.contains(rf"\b{re.escape(tok)}\b", regex=True, na=False)

        t = t.mask(is_income, "Credit")

    return t


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


def page_dashboard():
    # V3_1A Dashboard (visual-only)
    st.markdown("## 🧭 Dashboard")
    # Assume globals: tx_df, month_df, month_sel, acct_df
    # Friendly insight card
    try:
        insight = hero_insight(tx_df, month_sel)
    except Exception:
        insight = "Overview for the selected month."

    # Compute month summaries
    cur = monthly_summary(tx_df[tx_df["Month"] == month_sel]) if not tx_df.empty else monthly_summary(tx_df)
    pm = prev_month_str(month_sel)
    prev = monthly_summary(tx_df[tx_df["Month"] == pm]) if (not tx_df.empty and pm) else {"Credit":0,"Debit":0,"Investment":0,"CC Repay":0,"International":0}

    outflow = float(cur.get("Debit",0)) + float(cur.get("Investment",0)) + float(cur.get("CC Repay",0)) + float(cur.get("International",0))
    prev_outflow = float(prev.get("Debit",0)) + float(prev.get("Investment",0)) + float(prev.get("CC Repay",0)) + float(prev.get("International",0))
    incoming = float(cur.get("Credit",0))
    prev_incoming = float(prev.get("Credit",0))
    net = incoming - outflow
    prev_net = prev_incoming - prev_outflow

    def _delta(a, b):
        d = float(a) - float(b)
        sign = "+" if d >= 0 else "−"
        return f"{sign}{money(abs(d))}"


    net_color = "#00e676" if net >= 0 else "#ff5252"
    # HERO
    left, right = st.columns([1.35, 1.0], gap="large")
    with left:
        st.markdown(
            f"""<div class='mf-card mf-anim'>
            <div style='display:flex;justify-content:space-between;align-items:flex-start;gap:16px;'>
              <div>
                <div class='mf-sub'>This month • {month_sel}</div>
                <div style='font-size:34px;font-weight:800;line-height:1.15;margin-top:6px;'>Net: <span style='color:{net_color};'>{money(net)}</span></div>
                <div class='mf-sub' style='margin-top:6px;'>Δ {_delta(net, prev_net)} vs last month</div>
              </div>
              <div style='text-align:right;'>
                <div class='mf-sub'>Incoming</div>
                <div style='font-size:22px;font-weight:750;line-height:1.2;'>{money(incoming)}</div>
                <div class='mf-sub'>Outflow</div>
                <div style='font-size:22px;font-weight:750;line-height:1.2;'>{money(outflow)}</div>
              </div>
            </div>
            <div style='margin-top:12px;'>
              <div class='mf-pill'>Spend focus</div>
              <span class='mf-sub' style='margin-left:10px;'>{insight}</span>
            </div>
            </div>""",
            unsafe_allow_html=True,
        )

    with right:
        # Spend composition / health
        debit = float(cur.get("Debit",0))
        invest = float(cur.get("Investment",0))
        repay = float(cur.get("CC Repay",0))
        remit = float(cur.get("International",0))
        st.markdown(
            f"""<div class='mf-card mf-anim'>
            <div style='display:flex;justify-content:space-between;align-items:center;'>
              <div>
                <div class='mf-sub'>Spending (Expenses only)</div>
                <div style='font-size:26px;font-weight:800;margin-top:4px;'>{money(debit)}</div>
                <div class='mf-sub'>Δ {_delta(debit, float(prev.get("Debit",0)))} vs last month</div>
              </div>
              <div class='mf-pill'>Clean view</div>
            </div>
            <div style='margin-top:12px;display:grid;grid-template-columns:1fr 1fr;gap:10px;'>
              <div style='padding:10px;border-radius:14px;background:rgba(255,255,255,0.04);'>
                <div class='mf-sub'>Invest</div>
                <div style='font-weight:750;font-size:18px;'>{money(invest)}</div>
              </div>
              <div style='padding:10px;border-radius:14px;background:rgba(255,255,255,0.04);'>
                <div class='mf-sub'>Repay</div>
                <div style='font-weight:750;font-size:18px;'>{money(repay)}</div>
              </div>
              <div style='padding:10px;border-radius:14px;background:rgba(255,255,255,0.04);'>
                <div class='mf-sub'>Remit</div>
                <div style='font-weight:750;font-size:18px;'>{money(remit)}</div>
              </div>
              <div style='padding:10px;border-radius:14px;background:rgba(255,255,255,0.04);'>
                <div class='mf-sub'>Safe-to-spend*</div>
                <div style='font-weight:750;font-size:18px;'>{money(max(0.0, incoming - (invest + repay + remit)))}</div>
              </div>
            </div>
            <div class='mf-sub' style='margin-top:10px;'>*Excludes essential bills if you categorize them under Expenses.</div>
            </div>""",
            unsafe_allow_html=True,
        )


    # Interactive spending snapshot (V3_1A_HF2)
    st.markdown("### 🧾 Spending snapshot")
    ddf = tx_df[(tx_df["Month"] == month_sel) & (tx_df["Type"] == "Debit")].copy() if not tx_df.empty else pd.DataFrame()
    if ddf.empty:
        st.caption("No expense (Debit) transactions for this month.")
    else:
        by_cat = (
            ddf.groupby("Category", as_index=False)["Amount"]
            .sum()
            .sort_values("Amount", ascending=False)
        )
        by_cat["CategoryLabel"] = by_cat["Category"].apply(cat_label)
        top = by_cat.head(8).copy()

        # Keep selection sticky across reruns
        if "dash_spend_cat" not in st.session_state or st.session_state["dash_spend_cat"] not in by_cat["Category"].tolist():
            st.session_state["dash_spend_cat"] = top.iloc[0]["Category"]

        cL, cR = st.columns([1, 1], gap="large")

        with cL:
            st.markdown("<div class='mf-card mf-anim'><div class='mf-sub'>Top categories (tap to view details)</div></div>", unsafe_allow_html=True)
            fig1 = px.bar(
                top.iloc[::-1],
                x="Amount",
                y="CategoryLabel",
                orientation="h",
            )
            fig1.update_layout(
                height=320,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title=None,
                yaxis_title=None,
                showlegend=False,
            )
            st.plotly_chart(fig1, width="stretch")

            # Category 'list' selector (premium-feeling, compact)
            for _, row in top.iterrows():
                cat = row["Category"]
                is_sel = (cat == st.session_state["dash_spend_cat"])
                label = f"{cat_label(cat)}  •  {money(float(row['Amount']))}"
                if st.button(label, key=f"dash_catbtn_{cat}", use_container_width=True, type=("primary" if is_sel else "secondary")):
                    st.session_state["dash_spend_cat"] = cat

        with cR:
            sel = st.session_state["dash_spend_cat"]
            st.markdown(
                f"""<div class='mf-card mf-anim'>
                <div class='mf-sub'>Breakdown</div>
                <div style='font-size:20px;font-weight:850;margin-top:2px;'>{cat_label(sel)}</div>
                </div>""",
                unsafe_allow_html=True,
            )
            sub = ddf[ddf["Category"] == sel].copy()

            # Use AutoTag when present; otherwise Notes
            label_col = "AutoTag" if ("AutoTag" in sub.columns and sub["AutoTag"].astype(str).str.strip().ne("").any()) else "Notes"
            sub[label_col] = sub[label_col].astype(str).str.strip()
            sub.loc[sub[label_col] == "", label_col] = "(Unlabeled)"

            by_lbl = (
                sub.groupby(label_col, as_index=False)["Amount"]
                .sum()
                .sort_values("Amount", ascending=False)
            )

            fig2 = px.bar(
                by_lbl.head(12).iloc[::-1],
                x="Amount",
                y=label_col,
                orientation="h",
            )
            fig2.update_layout(
                height=420,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title=None,
                yaxis_title=None,
                showlegend=False,
            )
            st.plotly_chart(fig2, width="stretch")
    st.markdown("### 💳 Credit health")
    try:
        ev = compute_balance_events(tx_df)
        util = util_table(ev, month_sel, acct_df)
    except Exception:
        util = None

    if util is None or getattr(util, "empty", True):
        st.info("No credit accounts found. Add cards/limits in **Admin → Accounts**.")
    else:
        # Render as 2-column grid of compact cards
        cards = [util.iloc[i] for i in range(len(util))]
        rows = [cards[i:i+2] for i in range(0, len(cards), 2)]
        for rset in rows:
            cols = st.columns(len(rset), gap="large")
            for col, row in zip(cols, rset):
                with col:
                    lim = float(row.get("Limit", 0) or 0)
                    bal = float(row.get("Balance", 0) or 0)
                    pct = None if lim <= 0 else (bal/lim*100.0)
                    if pct is None:
                        status = "Set limit in Admin"
                        badge = "<span class='mf-pill'>Needs limit</span>"
                        bar = ""
                    else:
                        if pct < 30:
                            status = "Healthy"
                            badge = "<span class='mf-pill'>Healthy</span>"
                        elif pct < 70:
                            status = "Watch"
                            badge = "<span class='mf-pill'>Watch</span>"
                        else:
                            status = "High"
                            badge = "<span class='mf-pill'>High</span>"
                        bar = f"""<div style='height:8px;border-radius:999px;background:rgba(255,255,255,0.08);overflow:hidden;margin-top:10px;'>
                                  <div style='height:8px;width:{min(100,max(0,pct)):.0f}%;background:rgba(99,102,241,0.95);'></div>
                                </div>"""
                    st.markdown(
                        f"""<div class='mf-card mf-anim'>
                        <div style='display:flex;justify-content:space-between;align-items:flex-start;gap:12px;'>
                          <div>
                            <div style='font-weight:800;font-size:18px;'>{row.get("Account","")}</div>
                            <div class='mf-sub'>{status}</div>
                          </div>
                          {badge}
                        </div>
                        <div style='display:flex;justify-content:space-between;margin-top:10px;'>
                          <div>
                            <div class='mf-sub'>Balance</div>
                            <div style='font-weight:800;font-size:20px;'>{money(bal)}</div>
                          </div>
                          <div style='text-align:right;'>
                            <div class='mf-sub'>Limit</div>
                            <div style='font-weight:800;font-size:20px;'>{money(lim)}</div>
                          </div>
                        </div>
                        {bar}
                        <div class='mf-sub' style='margin-top:8px;'>Utilization: {"—" if pct is None else f"{pct:.0f}%"} </div>
                        </div>""",
                        unsafe_allow_html=True
                    )

    st.markdown("### 📈 Spending snapshot")
    # Default to showing the debit categories chart in a compact expander
    with st.expander("View expense breakdown (expenses only)", expanded=False):
        try:
            render_debit_categories_chart(month_df)
        except Exception as e:
            st.warning("Could not render chart for this month.")

def page_add():
    if is_mobile_view():
        return page_add_mobile()
    st.markdown("## ➕ Add")
    # --- Premium Tiles: quick type selection (UI-only, logic unchanged) ---
    if "add_type_pick" not in st.session_state:
        st.session_state["add_type_pick"] = None

    st.markdown('<div class="quick-actions">', unsafe_allow_html=True)
    st.markdown("### Quick actions")
    _tile_primary = [("Expense (−)", "Debit"), ("Income (+)", "Credit"), ("Invest", "Investment")]
    _tile_actions = [("Pay Credit Card", "CC Repay"), ("Remit International", "International"), ("LOC Draw", "LOC Draw"), ("LOC Repay", "LOC Repay")]

    def _render_tiles(_items, _cols=3, _key_prefix="tile"):
        cols = st.columns(_cols, gap="small")
        for i, (label, value) in enumerate(_items):
            with cols[i % _cols]:
                if st.button(label, use_container_width=True, key=f"{_key_prefix}_{i}"):
                    st.session_state["add_type_pick"] = value

                    st.rerun()
    _render_tiles(_tile_primary, _cols=2, _key_prefix="tile_p")
    _render_tiles(_tile_actions, _cols=2, _key_prefix="tile_a")

    _pref_type = st.session_state.get("add_type_pick")

    if locked_now:
        st.info("This month is locked. Switch month in sidebar or unlock in Admin.")

    # Keyboard-first autofocus amount
    st.components.v1.html("""
    <script>
    setTimeout(() => {
      const inputs = parent.document.querySelectorAll('input');
      for (const el of inputs) {
        if (el.getAttribute('aria-label') === 'Amount ($)') { el.focus(); break; }
      }
    }, 250);
    </script>
    """, height=0)

    qd = st.session_state["quick_defaults"]
    categories = sorted(set(list(rules.keys()) + list(CATEGORY_ICON.keys())))
    allowed_accounts, emoji_map = build_account_maps(acct_df)
    acct_options = [f"{emoji_map.get(a,'💳')} {a}" for a in allowed_accounts]
    acct_map = {f"{emoji_map.get(a,'💳')} {a}": a for a in allowed_accounts}

    st.markdown('</div>', unsafe_allow_html=True)

# (VF2.3) Add page: use normal widgets (not st.form) so Pay changes can hide/show Account instantly
    c1, c2, c3 = st.columns([1.05, 1.2, 1.2])
    with c1:
        entry_date = st.date_input("Date", value=date.today(), disabled=locked_now)
    with c2:
        # Primary selector = Quick actions tiles (top). Dropdown is kept in a collapsed expander as a fallback.
        entry_type = st.session_state.get("add_type_pick")
        if not entry_type:
            entry_type = qd.get("Type", "Debit")
            st.session_state["add_type_pick"] = entry_type
            st.session_state["add_type_select"] = type_to_display(entry_type)
        st.markdown("**Type**")
        st.markdown(f"<div style=\"padding:8px 12px;border:1px solid rgba(255,255,255,.12);border-radius:10px;background:rgba(255,255,255,.03);display:inline-block;\">{type_to_display(entry_type)}</div>", unsafe_allow_html=True)
        # Type is selected via Quick action tiles above.

    with c3:
        # Pay rules:
        # - Income/Invest/Remit International => Bank only
        # - Pay Credit Card => Bank only
        # - Expense => Card/Bank/Cash
        if entry_type in ("Credit", "Investment", "International"):
            pay = st.selectbox("Pay", options=["Bank"], index=0, disabled=True, key="add_pay_fixed")
        elif entry_type in ("CC Repay", "LOC Repay"):
            pay = st.selectbox("Pay", options=["Bank"], index=0, disabled=True, key="add_pay_fixed_cc")
        elif entry_type == "LOC Draw":
            pay = st.selectbox("Pay", options=["Card"], index=0, disabled=True, key="add_pay_fixed_loc")
        else:
            default_pay = qd.get("Pay", "Card")
            pay = segmented("Pay", PAY_METHODS, default=default_pay, key="add_pay")
    amt_text = st.text_input("Amount ($)", value="", placeholder="e.g. 120 or 120.50", disabled=locked_now)
    notes = st.text_area("Reason / Notes", value="", height=85, disabled=locked_now)

    auto_cat = classify(notes, rules) if notes.strip() else "Uncategorized"
    auto_idx = categories.index(auto_cat) if auto_cat in categories else categories.index("Uncategorized")

    # --- Category chips (top spend) ---
    if "add_cat_pick" not in st.session_state:
        st.session_state["add_cat_pick"] = None

    try:
        _tx = st.session_state.get("tx_df")
        _top_cats = []
        if _tx is not None and isinstance(_tx, pd.DataFrame) and len(_tx) > 0:
            _tmp = _tx.copy()
            # Focus on expense categories for chips
            if "Type" in _tmp.columns:
                _tmp = _tmp[_tmp["Type"].astype(str).str.lower().isin(["debit", "expense", "debit (-)"])]
            if "Date" in _tmp.columns:
                _cut = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=90)
                _tmp = _tmp[_tmp["Date"] >= _cut]
            if "Category" in _tmp.columns and "Amount" in _tmp.columns:
                _agg = _tmp.groupby("Category", dropna=False)["Amount"].sum().sort_values(ascending=False)
                _top_cats = [c for c in _agg.head(6).index.tolist() if str(c).strip() in categories]
        if _top_cats:
            st.markdown("**Quick categories**")
            _cols = st.columns(min(6, len(_top_cats)), gap="small")
            for i, c in enumerate(_top_cats):
                with _cols[i]:
                    if st.button(cat_label(str(c)), use_container_width=True, key=f"qc_{i}", disabled=locked_now):
                        st.session_state["add_cat_pick"] = str(c)
    except Exception:
        pass

    _picked_cat = st.session_state.get("add_cat_pick")
    if _picked_cat in categories:
        auto_idx = categories.index(_picked_cat)

    # Account selection
    loc_candidates = [a for a in allowed_accounts if re.search(r"\bline\s*of\s*credit\b|\bloc\b", str(a), flags=re.I)]
    loc_account = str(loc_candidates[0]).strip() if loc_candidates else ""

    if entry_type == "LOC Draw":
        pay = "Card"
        if not loc_account:
            st.error("No Line of Credit account found in Admin → Accounts. Add 'RBC Line of Credit' and try again.")
            account = ""
        else:
            loc_label = f"{emoji_map.get(loc_account, '🏦')} {loc_account}"
            st.selectbox("Account", options=[loc_label], index=0, disabled=True, key="add_loc_account_draw")
            account = loc_account

    elif entry_type == "LOC Repay":
        pay = "Bank"
        if not loc_account:
            st.error("No Line of Credit account found in Admin → Accounts. Add 'RBC Line of Credit' and try again.")
            account = ""
        else:
            loc_label = f"{emoji_map.get(loc_account, '🏦')} {loc_account}"
            st.selectbox("Repayment to which account?", options=[loc_label], index=0, disabled=True, key="add_loc_account_repay")
            account = loc_account

    elif entry_type == "CC Repay":
        default_acct = qd.get("Account", allowed_accounts[0])
        default_label = f"{emoji_map.get(default_acct, '💳')} {default_acct}"
        default_idx = acct_options.index(default_label) if default_label in acct_options else 0
        acct_pick = st.selectbox("Repayment to which account?", acct_options, index=default_idx, disabled=locked_now)
        account = acct_map[acct_pick]
        pay = "Bank"
    else:
        if pay == "Card" and entry_type in ("Debit", "Investment", "International"):
            default_acct = qd.get("Account", allowed_accounts[0])
            default_label = f"{emoji_map.get(default_acct, '💳')} {default_acct}"
            default_idx = acct_options.index(default_label) if default_label in acct_options else 0
            acct_pick = st.selectbox("Account", acct_options, index=default_idx, disabled=locked_now, key="add_account")
            account = acct_map[acct_pick]
        else:
            st.session_state.pop("add_account", None)
            account = ""  # No Account for non-card expenses OR for Credit/Income
    if entry_type == "LOC Draw":
        fixed_cat = "LOC Utilization"
        fixed_label = cat_label(fixed_cat)
        st.selectbox("Category", options=[fixed_label], index=0, disabled=True, key="add_cat_loc_draw")
        category = fixed_cat
        used_auto = False
    elif entry_type == "LOC Repay":
        fixed_cat = "Repayment"
        fixed_label = cat_label(fixed_cat)
        st.selectbox("Category", options=[fixed_label], index=0, disabled=True, key="add_cat_loc_repay")
        category = fixed_cat
        used_auto = False
    else:
        cat_pick = st.selectbox("Category", [cat_label(c) for c in categories], index=auto_idx, disabled=locked_now)
        category = categories[[cat_label(c) for c in categories].index(cat_pick)]
        used_auto = (category == auto_cat and auto_cat != "Uncategorized" and notes.strip() != "")

    with st.expander("🔁 Recurring (optional)", expanded=False):
        st.caption("Turn ON once. Future months auto-add when you open the app.")
        mark_rec = st.toggle("Mark as recurring", value=False, disabled=locked_now)
        dom = st.number_input("Day of month (1–31)", min_value=1, max_value=31, value=1, step=1, disabled=(locked_now or not mark_rec))
        nick = st.text_input("Display name", value="", placeholder="e.g. Rent / Netflix", disabled=(locked_now or not mark_rec))

    amt_preview = parse_amount(amt_text) if amt_text.strip() else None
    amt_invalid = (amt_text.strip() == "") or (amt_preview is None)
    if amt_text.strip() != "" and amt_preview is None:
        st.error("Amount must be a number (e.g., 120 or 120.50).")

    # --- Review before Save (premium confirm) ---
    if "pending_add" not in st.session_state:
        st.session_state["pending_add"] = None
    if "show_add_review" not in st.session_state:
        st.session_state["show_add_review"] = False

    save = st.button("Save Entry", disabled=(locked_now or amt_invalid))

    # Stage transaction for review instead of writing immediately
    if save:
        st.session_state["pending_add"] = {
            "entry_date": entry_date,
            "entry_type": entry_type,
            "amt_text": amt_text,
            "pay": pay,
            "account": account,
            "category": category,
            "notes": notes,
            "used_auto": used_auto,
            "auto_cat": auto_cat,
            "mark_rec": mark_rec,
            "dom": dom if "dom" in locals() else 1,
            "nick": nick if "nick" in locals() else "",
        }
        st.session_state["show_add_review"] = True

    if st.session_state.get("show_add_review") and st.session_state.get("pending_add"):
        p = st.session_state["pending_add"]
        st.markdown("### ✅ Review")
        _amount_preview = parse_amount(p.get("amt_text","")) or 0.0
        _notes_preview = (p['notes'][:120] + '…') if p.get('notes') and len(p['notes']) > 120 else (p.get('notes') or '—')
        _review_md = (
            f"**{p['entry_type']}** • **{p['category']}** • **${_amount_preview:,.2f}**\n\n"
            f"**Pay:** {p['pay']} • **Account:** {p['account']}\n\n"
            f"**Notes:** {_notes_preview}"
        )
        st.markdown(_review_md)

def page_add_mobile():
    # Mobile variant: keep behavior identical to desktop for now (avoids undefined reference).
    return page_add()

def page_creditbal():
    st.markdown("## 💳 CreditBal")
    st.caption("Billing-cycle view • Utilization • Safe-to-spend • Upcoming recurring before bill date.")
    ev = compute_balance_events(tx_df)
    util = util_table(ev, month_sel, acct_df)

    for _, r in util.iterrows():
        lim = float(r["Limit"])
        bal = float(r["Balance"])
        pct = None if lim <= 0 else bal/lim*100.0

        st.markdown(
            f"""
            <div class="mf-card mf-anim">
              <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:14px;">
                <div>
                  <div style="font-size:18px; font-weight:950;">{r['Emoji']} {r['Account']}</div>
                  <div style="color:rgba(232,234,237,0.70); font-size:13px; margin-top:4px;">
                    Bill date: <b>{r['BillDate']}</b> • Cycle: <b>{r['CycleStart']}</b> → <b>{r['CycleEnd']}</b>
                  </div>
                  <div style="margin-top:10px; font-size:13px; color:rgba(232,234,237,0.70);">
                    Cycle charges: <b>{money(float(r['CycleCharges']))}</b> • Cycle payments: <b>{money(float(r['CyclePayments']))}</b> • Upcoming recurring: <b>{money(float(r['UpcomingRecurringToBill']))}</b>
                  </div>
                </div>
                <div style="text-align:right;">
                  <div style="font-size:12px; color:rgba(232,234,237,0.70);">Current Balance</div>
                  <div style="font-size:28px; font-weight:950;">{money(bal)}</div>
                  <div style="margin-top:6px;">
                    <span class="mf-pill">{month_sel}</span>
                  </div>
                </div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if lim > 0 and pct is not None:
            safe = r["SafeToSpend"]
            st.progress(min(max(pct, 0.0), 100.0)/100.0, text=f"{pct:.1f}% utilized of {money(lim)} • Safe-to-spend: {money(float(safe)) if safe is not None else '—'}")
def page_trends():
    st.markdown("## 📈 Trends")
    st.caption("Trends, top categories, top merchants, weekday spend pattern.")

    spend = tx_df[(tx_df["Type"] == "Debit") & (~_is_nonexpense_movement(tx_df))].copy()
    if spend.empty:
        st.info("No debit transactions yet.")
    else:
        by_m = spend.groupby("Month", as_index=False)["Amount"].sum().sort_values("Month")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=by_m["Month"], y=by_m["Amount"], mode="lines+markers", name="Debit"))
        fig.update_layout(height=260, margin=dict(l=10,r=10,t=40,b=10), title="Debit Trend (monthly)",
                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font=dict(color="#E8EAED"))
        st.plotly_chart(fig, width="stretch")

        c1, c2 = st.columns([1, 1])
        with c1:
            by_cat = spend.groupby("Category", as_index=False)["Amount"].sum().sort_values("Amount", ascending=False).head(12)
            by_cat["Category"] = by_cat["Category"].apply(cat_label)
            st.plotly_chart(px.bar(by_cat, x="Category", y="Amount", title="Top categories", template="plotly_dark", color_discrete_sequence=px.colors.qualitative.Set2), width="stretch")
        with c2:
            spend["MerchantKey"] = spend["Notes"].apply(normalize_merchant)
            by_mer = spend[spend["MerchantKey"] != ""].groupby("MerchantKey", as_index=False)["Amount"].sum().sort_values("Amount", ascending=False).head(12)
            by_mer["MerchantKey"] = by_mer["MerchantKey"].str.title()
            st.plotly_chart(px.bar(by_mer, x="MerchantKey", y="Amount", title="Top merchants", template="plotly_dark", color_discrete_sequence=px.colors.qualitative.Set2), width="stretch")

        spend["Weekday"] = spend["Date"].dt.day_name()
        wd = spend.groupby(["Weekday"], as_index=False)["Amount"].sum()
        order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        wd["Weekday"] = pd.Categorical(wd["Weekday"], categories=order, ordered=True)
        wd.sort_values("Weekday", inplace=True)
        st.plotly_chart(px.bar(wd, x="Weekday", y="Amount", title="Spend by weekday", template="plotly_dark", color_discrete_sequence=px.colors.qualitative.Set2), width="stretch")
def page_transactions():
    st.markdown("## 🧾 Transactions")
    st.caption("Fast search + export. Edit/Delete in Admin → Fix Mistakes.")
    df = view_df.copy()
    if df.empty:
        st.info("No transactions.")
    else:
        with st.form("tx_search", border=False):
            a, b, c = st.columns([1.2, 1.3, 1.0])
            with a:
                q = st.text_input("Search notes", "")
            with b:
                cats = sorted(df["Category"].dropna().unique().tolist())
                chosen = st.multiselect("Category", cats, default=cats)
            with c:
                m = st.selectbox("Month", sorted(df["Month"].unique()), index=0)
            run = st.form_submit_button("Search")

        if not run:
            m = month_sel
            chosen = sorted(df["Category"].dropna().unique().tolist())
            q = ""

        out = df[df["Month"] == m].copy()
        if chosen:
            out = out[out["Category"].isin(chosen)]
        if q.strip():
            out = out[out["Notes"].fillna("").str.contains(re.escape(q.strip()), case=False, na=False)]

        show = out.sort_values("Date", ascending=False).copy()
        show["Type"] = show["Type"].apply(lambda t: f"{TYPE_EMOJI.get(t,'')} {t}")
        show["Category"] = show["Category"].apply(cat_label)
        show["Account"] = show["Account"].apply(lambda a: f"{ACCOUNT_EMOJI_DEFAULT.get(a,'💳')} {a}" if a in ACCOUNT_EMOJI_DEFAULT else a)
        show["Confidence"] = show["AutoTag"].apply(confidence_tag)
        show = show[["Date","Type","Amount","Pay","Account","Category","Confidence","Notes"]]

        # Mobile-friendly cards (optional)
        vm = st.session_state.get("view_mode", "Auto")
        default_cards = (vm == "Mobile")
        card_view = st.toggle("Card view", value=default_cards, key="tx_card_view")

        if card_view:
            max_n = min(300, len(show))
            n = st.select_slider("Show last", options=[25, 50, 100, 200, max_n], value=min(50, max_n), key="tx_card_n")
            sdf = show.head(n).copy()
            for i, r in sdf.iterrows():
                title = f"{r['Date']} • {r['Category']} • {r['Amount']}"
                with st.expander(title, expanded=False):
                    st.write(f"**Type:** {r['Type']}")
                    st.write(f"**Pay:** {r['Pay']}")
                    st.write(f"**Account:** {r['Account']}")
                    st.write(f"**Confidence:** {r['Confidence']}")
                    if str(r.get('Notes','')).strip():
                        st.write(f"**Notes:** {r['Notes']}")
        else:
            st.dataframe(show, width="stretch", hide_index=True)

        st.download_button("Download CSV", data=show.to_csv(index=False).encode("utf-8"),
                           file_name=f"{APP_NAME.replace(' ','_')}_{m}.csv", mime="text/csv")
def page_admin():
    st.markdown("## 🛡️ Admin")
    st.caption("Lock months • Accounts • Fix mistakes • Rules • Recurring • Insights • Backup")

    sections = ["Monthly Lock", "Accounts", "Fix Mistakes", "Rules", "Recurring", "Insights"]
    st.session_state["admin_section"] = segmented("Section", sections, default=st.session_state["admin_section"], key="admin_seg")
    section = st.session_state["admin_section"]

    if section == "Monthly Lock":
        st.markdown("### 🔒 Monthly lock")
        months = sorted(tx_df["Month"].unique().tolist()) if not tx_df.empty else []
        if not months:
            now = pd.Period(date.today(), freq="M")
            months = sorted({str(now + i) for i in range(-2, 8)})
            months = sorted(set(months) | set(build_month_options()))

        safe_default = [m for m in sorted(set(locked_months)) if m in months]
        selected = st.multiselect("Locked months", options=months, default=safe_default)
        if st.button("Save Locks"):
            admin_update_and_refresh("locked_months", ", ".join(sorted(set(selected))))
            st.toast("Locks updated ✓", icon="🔒")
            st.rerun()

    elif section == "Accounts":
        st.markdown("### 💳 Account limits + billing day")
        st.caption("Set limits for utilization %. BillingDay = bill generation day (1–31).")

        # Show table
        view = acct_df.copy()
        st.dataframe(view, width="stretch", hide_index=True)

        allowed_accounts, emoji_map = build_account_maps(acct_df)

        st.markdown("#### Edit an account")
        acct_opts = [f"{emoji_map.get(a,'💳')} {a}" for a in allowed_accounts]
        pick = st.selectbox("Account", acct_opts, index=0, key="admin_acct_pick")
        acct = pick.split(" ", 1)[1]
        row = acct_df[acct_df["Account"] == acct].iloc[0]

        new_emoji = st.text_input("Emoji", value=str(row.get("Emoji","")).strip() or emoji_map.get(acct, "💳"))
        new_limit = st.number_input("Limit ($)", min_value=0.0, step=100.0, value=float(row.get("Limit", 0) or 0))
        new_bill = st.number_input("Billing day (1–31)", min_value=1, max_value=31, step=1, value=int(row.get("BillingDay", 1) or 1))

        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("Save Account", width="stretch"):
                df2 = acct_df.copy()
                df2.loc[df2["Account"] == acct, "Emoji"] = new_emoji.strip() or emoji_map.get(acct, "💳")
                df2.loc[df2["Account"] == acct, "Limit"] = float(new_limit)
                df2.loc[df2["Account"] == acct, "BillingDay"] = int(new_bill)
                save_accounts(df2)
                st.toast("Saved ✓", icon="✅")
                st.rerun()
        with c2:
            # Remove account (card)
            if st.button("Remove Account", width="stretch"):
                df2 = acct_df.copy()
                df2 = df2[df2["Account"] != acct].copy()
                # Safety: ensure at least one account remains
                if df2.empty:
                    st.error("You must keep at least one account.")
                else:
                    save_accounts(df2)
                    # Clear any stale selections (e.g., Add page)
                    for k in ["add_account", "add_repay_account", "admin_acct_pick"]:
                        if k in st.session_state:
                            st.session_state.pop(k, None)
                    st.toast("Removed ✓", icon="🗑️")
                    st.rerun()

        st.divider()
        st.markdown("#### Add a new account")
        with st.form("admin_add_account", border=True):
            new_name = st.text_input("Account name", value="", placeholder="e.g. Canadian Tire Card")
            add_emoji = st.text_input("Emoji (optional)", value="💳")
            add_limit = st.number_input("Limit ($)", min_value=0.0, step=100.0, value=0.0)
            add_bill = st.number_input("Billing day (1–31)", min_value=1, max_value=31, step=1, value=1)
            submit_add = st.form_submit_button("Add Account")
        if submit_add:
            nm = (new_name or "").strip()
            if not nm:
                st.error("Account name is required.")
            elif nm in set(acct_df["Account"].astype(str).tolist()):
                st.error("That account already exists.")
            else:
                df2 = acct_df.copy()
                df2 = pd.concat([df2, pd.DataFrame([{
                    "Account": nm,
                    "Emoji": (add_emoji or "💳").strip() or "💳",
                    "Limit": float(add_limit),
                    "BillingDay": int(add_bill),
                }])], ignore_index=True)
                save_accounts(df2)
                # Clear stale widget keys
                for k in ["admin_acct_pick", "add_account", "add_repay_account"]:
                    if k in st.session_state:
                        st.session_state.pop(k, None)
                st.toast("Account added ✓", icon="✅")
                st.rerun()

    elif section == "Fix Mistakes":
        st.markdown("### 🧰 Fix mistakes")
        st.caption("Edit (preferred) or delete. Set Amount=0 to neutralize. Undo available.")
        if tx_df.empty:
            st.info("No transactions yet.")
        else:
            months = sorted(tx_df["Month"].unique().tolist())
            m = st.selectbox("Month", months, index=len(months) - 1)
            locked_sel = m in set(locked_months)
            if locked_sel:
                st.warning(f"🔒 {m} is locked — edit/delete disabled.")
            subset = tx_df[tx_df["Month"] == m].copy().sort_values("Date", ascending=False)

            temp = subset.copy()
            temp["Type"] = temp["Type"].apply(lambda t: f"{TYPE_EMOJI.get(t,'')} {t}")
            temp["Category"] = temp["Category"].apply(cat_label)
            temp["Account"] = temp["Account"].apply(lambda a: f"{emoji_map_live.get(a,'💳')} {a}" if a in allowed_accounts_live else a)
            temp["Confidence"] = temp["AutoTag"].apply(confidence_tag)
            st.dataframe(temp[["Date","Type","Amount","Pay","Account","Category","Confidence","Notes","TxId","_row","AutoTag"]],
                         width="stretch", hide_index=True)

            if not subset.empty:
                choices = subset.apply(lambda r: f"{r['Date'].date()} | {r['Type']} | {money(r['Amount'])} | {r['Account']} | {r['Category']} | {r['TxId']}", axis=1).tolist()
                pick = st.selectbox("Select transaction", choices)
                txid = pick.split("|")[-1].strip()
                r = subset.loc[subset["TxId"] == txid].iloc[0]
                row_num = int(r["_row"])

                old_row = [
                    r["TxId"], str(pd.to_datetime(r["Date"]).date()), "Family", r["Type"], float(r["Amount"]), r["Pay"],
                    r["Account"], r["Category"], r["Notes"], r["CreatedAt"], r.get("AutoTag","")
                ]

                st.markdown("#### Edit selected")
                ed_date = st.date_input("Date", value=pd.to_datetime(r["Date"]).date(), disabled=locked_sel)
                ed_type = st.selectbox("Type", ENTRY_TYPES, index=ENTRY_TYPES.index(r["Type"]) if r["Type"] in ENTRY_TYPES else 0, disabled=locked_sel,
                                       format_func=lambda t: f"{TYPE_EMOJI.get(t,'')} {t}")
                ed_amt = st.text_input("Amount ($)", value=str(float(r["Amount"])), disabled=locked_sel)
                ed_amount = parse_amount(ed_amt)
                ed_pay = st.selectbox("Pay", PAY_METHODS, index=(PAY_METHODS.index(r["Pay"]) if r["Pay"] in PAY_METHODS else 0), disabled=locked_sel)

                acct_opts = [f"{emoji_map_live.get(a,'💳')} {a}" for a in allowed_accounts_live]
                acct_map = {f"{emoji_map_live.get(a,'💳')} {a}": a for a in allowed_accounts_live}

                if ed_type == "CC Repay":
                    ap = st.selectbox("Account", acct_opts, index=0, disabled=locked_sel)
                    ed_account = acct_map[ap]
                    ed_pay = "Bank"
                else:
                    if ed_pay == "Card":
                        ap = st.selectbox("Account", acct_opts, index=0, disabled=locked_sel)
                        ed_account = acct_map[ap]
                    else:
                        ed_account = ed_pay

                cats = sorted(set(list(rules.keys()) + list(CATEGORY_ICON.keys())))
                ed_cat = st.selectbox("Category", [cat_label(c) for c in cats], index=cats.index(r["Category"]) if r["Category"] in cats else 0, disabled=locked_sel)
                ed_category = cats[[cat_label(c) for c in cats].index(ed_cat)]
                ed_notes = st.text_area("Notes", value=str(r["Notes"] or ""), height=80, disabled=locked_sel)

                cA, cB = st.columns([1,1])
                with cA:
                    if st.button("Save Edit", disabled=locked_sel):
                        if ed_amount is None:
                            st.error("Enter a valid amount.")
                            st.stop()
                        update_transaction_by_row(row_num, {
                            "TxId": r["TxId"],
                            "Date": pd.to_datetime(ed_date).date().isoformat(),
                            "Type": ed_type,
                            "Amount": float(ed_amount),
                            "Pay": ed_pay,
                            "Account": ed_account,
                            "Category": ed_category,
                            "Notes": ed_notes,
                            "CreatedAt": r["CreatedAt"] or datetime.utcnow().isoformat(timespec="seconds"),
                            "AutoTag": r.get("AutoTag","") or "",
                        })
                        push_undo(UndoAction(kind="edit", row_num=row_num, old_row=old_row))
                        st.toast("Updated ✓", icon="✅")
                        st.rerun()
                with cB:
                    if st.button("Delete", disabled=locked_sel):
                        delete_transaction_by_row(row_num)
                        push_undo(UndoAction(kind="delete", row_num=row_num, old_row=old_row, txid=None))
                        st.toast("Deleted", icon="🗑️")
                        st.rerun()
    elif section == "Rules":
        st.markdown("### 🧠 Rules")
        st.caption("Case-insensitive keyword rules used for auto-categorization.")
        st.toggle("Lock rules (prevent edits)", value=rules_locked, key="rules_lock")
        if st.button("Save lock"):
            admin_update_and_refresh("rules_locked", "true" if bool(st.session_state["rules_lock"]) else "false")
            st.toast("Rules lock updated", icon="🔒")
            st.rerun()

        current = "\n".join([f"{k}: {', '.join(v)}" for k, v in rules.items()])
        txt = st.text_area("Rules text", value=current, height=280, disabled=rules_locked, help="Format: Category: keyword1, keyword2")
        if st.button("Save Rules", disabled=rules_locked):
            admin_update_and_refresh("rules_text", txt)
            st.toast("Rules saved ✓", icon="✅")
            st.rerun()

    elif section == "Recurring":
        st.markdown("### 🔁 Recurring manager")
        prefs = st.session_state["prefs_list"]
        if not prefs:
            st.info("No recurring items yet. Set one from Add page.")
        else:
            pdf = pd.DataFrame(prefs)
            pdf["Amount"] = pd.to_numeric(pdf.get("Amount", None), errors="coerce")
            pdf["DayOfMonth"] = pd.to_numeric(pdf.get("DayOfMonth", None), errors="coerce").fillna(1).astype(int)

            show = pdf.copy()
            show["Account"] = show["Account"].apply(lambda a: f"{emoji_map_live.get(a,'💳')} {a}")
            show["Category"] = show["Category"].apply(lambda c: cat_label(c) if c else cat_label("Uncategorized"))
            show["Amount"] = show["Amount"].apply(lambda x: money(x) if pd.notna(x) else "—")
            st.dataframe(show[["Nickname","Amount","Account","Category","DayOfMonth"]], width="stretch", hide_index=True)

            st.markdown("#### Edit one")
            mk = st.selectbox("MerchantKey", pdf["MerchantKey"].astype(str).tolist(), index=0)
            row = pdf[pdf["MerchantKey"].astype(str) == str(mk)].iloc[0]

            nick = st.text_input("Display name", value=str(row.get("Nickname","")))
            amt = st.text_input("Amount ($)", value=str(float(row["Amount"])) if pd.notna(row["Amount"]) else "")
            amt_v = parse_amount(amt)
            dom = st.number_input("Day of month (1–31)", min_value=1, max_value=31, value=int(row["DayOfMonth"]), step=1)

            cats = sorted(set(list(rules.keys()) + list(CATEGORY_ICON.keys())))
            cat_pick = st.selectbox("Category", [cat_label(c) for c in cats], index=cats.index(str(row.get("Category","Uncategorized"))) if str(row.get("Category","Uncategorized")) in cats else 0)
            cat = cats[[cat_label(c) for c in cats].index(cat_pick)]

            pay = st.selectbox("Pay", PAY_METHODS, index=PAY_METHODS.index(str(row.get("Pay","Bank"))) if str(row.get("Pay","Bank")) in PAY_METHODS else 1)
            acct_opts = [f"{emoji_map_live.get(a,'💳')} {a}" for a in allowed_accounts_live]
            acct_map = {f"{emoji_map_live.get(a,'💳')} {a}": a for a in allowed_accounts_live}
            acct_pick = st.selectbox("Account", acct_opts, index=0)
            acct = acct_map[acct_pick]

            if st.button("Save Recurring Item"):
                if amt_v is None:
                    st.error("Enter a valid amount.")
                    st.stop()
                pref = {"MerchantKey": str(mk), "Nickname": nick.strip() or str(mk).title(), "IsRecurring": True,
                        "DayOfMonth": int(dom), "Category": cat, "Pay": pay,
                        "Account": acct if pay == "Card" else pay, "Amount": float(amt_v)}
                prefs2 = upsert_pref(prefs, pref)
                admin_update_and_refresh("recurring_prefs_json", json.dumps(prefs2, ensure_ascii=False))
                st.toast("Saved ✓", icon="✅")
                st.rerun()

    else:  # Insights
        st.markdown("### ✨ Insights")
        st.caption("Quality checks that keep your ledger clean and powerful.")

        issues = []
        missing_limits = acct_df[acct_df["Limit"] <= 0]["Account"].tolist()
        if missing_limits:
            issues.append(f"Set limits for: {', '.join(missing_limits)}")

        unc = tx_df[tx_df["Category"].isin(["", "Uncategorized"])].copy()
        if not unc.empty:
            top = unc["Notes"].fillna("").apply(normalize_merchant)
            by = top[top != ""].value_counts().head(8)
            if not by.empty:
                issues.append("Frequently uncategorized merchants: " + ", ".join([f"{k.title()} ({v})" for k, v in by.items()]))

        empty_notes = int((tx_df["Notes"].fillna("").str.strip() == "").sum()) if not tx_df.empty else 0
        if empty_notes > 0:
            issues.append(f"{empty_notes} transaction(s) have empty notes (harder to auto-categorize).")

        score = 100
        score -= len(missing_limits) * 7
        score -= min(30, empty_notes * 2)
        score -= 10 if (not unc.empty) else 0
        score = max(0, score)

        st.markdown(f"<div class='mf-card mf-anim'><h4>Ledger Health</h4><p class='mf-kpi'>{score}/100</p><p class='mf-sub'>Higher score = cleaner data + better automation.</p></div>", unsafe_allow_html=True)
        st.progress(score/100.0, text="Health")

        if issues:
            st.markdown("#### Recommendations")
            for i in issues:
                st.info(i)
        else:
            st.success("Everything looks clean ✅")

        st.markdown("#### Backup")
        export_month = st.selectbox("Export month", ["All"] + sorted(tx_df["Month"].unique().tolist()))
        export_df = tx_df.copy()
        if export_month != "All":
            export_df = export_df[export_df["Month"] == export_month]
        st.download_button("Download Backup CSV", data=export_df[TX_HEADERS].to_csv(index=False).encode("utf-8"),
                           file_name="nishanthfintrack_backup.csv", mime="text/csv")
# =============================
# Pages (subpages)
# =============================
# =============================
# Pages (subpages) + Navigation
# =============================

def detect_mobile_mode() -> bool:
    # Explicit override via query params
    try:
        qp = st.query_params
        mv = str(qp.get("mobile", "")).strip().lower()
        if mv in ("1", "true", "yes", "y", "on"):
            return True
        if mv in ("0", "false", "no", "n", "off"):
            return False
    except Exception:
        pass

    # Best-effort user-agent detection (works on Streamlit Cloud)
    ua = ""
    try:
        ua = (st.context.headers.get("User-Agent") or "")
    except Exception:
        try:
            # older streamlit fallbacks
            ua = (st.runtime.scriptrunner.get_script_run_ctx().request.headers.get("User-Agent") or "")
        except Exception:
            ua = ""

    ua_l = ua.lower()
    return any(k in ua_l for k in ["iphone", "ipad", "android", "mobile"])

MOBILE_MODE = detect_mobile_mode()

# Use mobile Add page when on phone
def _page_add_router():
    if MOBILE_MODE:
        page_add_mobile()
    else:
        page_add()

pages = [
    st.Page(page_dashboard, title="🏠 Dashboard", url_path="dashboard"),
    st.Page(_page_add_router, title="➕ Add", url_path="add"),
    st.Page(page_creditbal, title="💳 CreditBal", url_path="creditbal"),
    st.Page(page_trends, title="📈 Trends", url_path="trends"),
    st.Page(page_transactions, title="🧾 Transactions", url_path="transactions"),
    st.Page(page_admin, title="🛡️ Admin", url_path="admin"),
]

if MOBILE_MODE:
    # On phones, avoid relying on the sidebar (it overlays the content).
    # Provide an in-page navigation bar so the user never needs to open the sidebar.
    page_keys = ["dashboard", "add", "creditbal", "trends", "transactions", "admin"]
    page_labels = {
        "dashboard": "🏠",
        "add": "➕",
        "creditbal": "💳",
        "trends": "📈",
        "transactions": "🧾",
        "admin": "🛡️",
    }
    # Read current page from query params (fallback to dashboard)
    try:
        cur = str(st.query_params.get("p", "dashboard"))
    except Exception:
        cur = "dashboard"
    if cur not in page_keys:
        cur = "dashboard"

    sel = st.radio(
        "Navigate",
        options=page_keys,
        index=page_keys.index(cur),
        format_func=lambda k: f"{page_labels.get(k,'•')} {k.title()}",
        horizontal=True,
        label_visibility="collapsed",
        key="mobile_top_nav",
    )

    if sel != cur:
        st.query_params["p"] = sel
        st.rerun()

    # Execute the selected page directly (no sidebar needed)
    page_map = {
        "dashboard": page_dashboard,
        "add": _page_add_router,
        "creditbal": page_creditbal,
        "trends": page_trends,
        "transactions": page_transactions,
        "admin": page_admin,
    }
    page_map[sel]()
else:
    nav = st.navigation(pages, position="sidebar")
    nav.run()
st.caption("NishanthFinTrack 2026 • Premium Dark • Fast • Sheets-backed • Apply filters for speed • Refresh only when needed")
