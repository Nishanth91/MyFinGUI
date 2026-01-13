"""MyFin — NiceGUI (VF3_FINAL_CLEAN)

Goal: stable, deployable NiceGUI web app with HF2 feature parity:
- Login
- Dashboard (monthly totals + charts)
- Add (tile actions, no redundant dropdowns)
- Credit Bal (basic utilization & upcoming recurring)
- Trends
- Transactions (filter, export, edit/update by row, delete)
- Admin (Locks, Rules, Recurring templates, Accounts, Fix Mistakes)

Google Sheets:
- Spreadsheet name: nishanthfintrack_2026
- Tabs: transactions, cards, admin

Credentials:
- Provide Google service account via env var GOOGLE_SERVICE_ACCOUNT_JSON (full JSON) OR service_account.json file next to this script.
"""

from __future__ import annotations

import os, json, time, random, uuid, hmac
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple

import pandas as pd
import plotly.express as px
import gspread
from google.oauth2.service_account import Credentials
from dateutil.relativedelta import relativedelta
from nicegui import ui, app
# --- Bank-style dark theme overrides (NiceGUI/Quasar) ---
ui.add_css(r'''
:root {
  --mf-bg: #0b1220;
  --mf-card: #0f1b2d;
  --mf-card-2: #12233b;
  --mf-border: rgba(255,255,255,0.08);
  --mf-text: rgba(255,255,255,0.92);
  --mf-muted: rgba(255,255,255,0.65);
  --mf-accent: #3b82f6;
}

body, .q-layout, .q-page, .nicegui-content {
  background: var(--mf-bg) !important;
  color: var(--mf-text) !important;
}

.my-card, .q-card, .q-dialog__inner > .q-card {
  background: linear-gradient(180deg, var(--mf-card), var(--mf-card-2)) !important;
  border: 1px solid var(--mf-border) !important;
  border-radius: 16px !important;
  color: var(--mf-text) !important;
  box-shadow: 0 10px 30px rgba(0,0,0,0.35);
}

.q-toolbar, header.q-header {
  background: rgba(15, 27, 45, 0.92) !important;
  backdrop-filter: blur(10px);
  border-bottom: 1px solid var(--mf-border);
}

.q-tab, .q-toolbar__title, .q-btn__content, .q-item__label, .q-field__label, .q-field__native, .q-field__prefix, .q-field__suffix {
  color: var(--mf-text) !important;
}

.q-field--outlined .q-field__control,
.q-field--filled .q-field__control {
  background: rgba(255,255,255,0.04) !important;
  border: 1px solid var(--mf-border) !important;
  border-radius: 12px !important;
}

.q-field__bottom, .q-field__hint, .q-field__messages, .text-grey, .text-grey-7 {
  color: var(--mf-muted) !important;
}

.q-table__container, .q-table__middle, .q-table__bottom {
  background: transparent !important;
  color: var(--mf-text) !important;
}

.q-table thead tr th {
  background: rgba(255,255,255,0.03) !important;
  color: var(--mf-muted) !important;
  border-bottom: 1px solid var(--mf-border) !important;
}

.q-table tbody tr td {
  border-bottom: 1px solid rgba(255,255,255,0.06) !important;
}

.q-btn {
  border-radius: 999px !important;
}

.q-btn--standard, .q-btn--unelevated, .q-btn--push {
  background: rgba(59,130,246,0.18) !important;
  border: 1px solid rgba(59,130,246,0.35) !important;
}

.q-btn--standard .q-btn__content,
.q-btn--unelevated .q-btn__content,
.q-btn--push .q-btn__content {
  color: var(--mf-text) !important;
}

.q-menu, .q-list, .q-date, .q-time {
  background: var(--mf-card) !important;
  border: 1px solid var(--mf-border) !important;
  color: var(--mf-text) !important;
}

.q-separator {
  background: rgba(255,255,255,0.06) !important;
}

.tile {
  cursor: pointer;
  transition: transform .08s ease, border-color .15s ease, background .15s ease;
}
.tile:hover {
  transform: translateY(-2px);
  border-color: rgba(59,130,246,0.5) !important;
}
''')
# --- end theme overrides ---


# -------------------- APP CONFIG --------------------
APP_NAME = "MyFin"
APP_VERSION = "VF3_FINAL_CLEAN"
SHEET_NAME = "nishanthfintrack_2026"

TAB_TRANSACTIONS = "transactions"
TAB_ACCOUNTS = "cards"
TAB_ADMIN = "admin"

TX_HEADERS = ["TxId", "Date", "Owner", "Type", "Amount", "Pay", "Account", "Category", "Notes", "CreatedAt", "AutoTag"]
ACCT_HEADERS = ["Account", "Emoji", "Limit", "BillingDay"]
ADMIN_HEADERS = ["Key", "Value"]

# Simple login (same as earlier)
AUTH_USERNAME = "Ajay"
AUTH_PASSWORD = "1999"

# Defaults
DEFAULT_CATEGORY_RULES: Dict[str, List[str]] = {
    "Groceries": ["walmart", "superstore", "costco", "grocery"],
    "Dining": ["restaurant", "swiggy", "uber eats", "doordash", "tim hortons", "starbucks"],
    "Fuel": ["gas", "petro", "shell", "esso"],
    "Rent": ["rent"],
    "Utilities": ["hydro", "internet", "phone"],
    "Insurance": ["insurance", "manulife"],
    "Travel": ["air canada", "flight", "hotel", "uber"],
    "Shopping": ["amazon", "bestbuy", "store"],
    "Uncategorized": [],
}


# -------------------- GOOGLE SHEETS --------------------
_gclient: Optional[gspread.Client] = None
_spreadsheet: Optional[gspread.Spreadsheet] = None
_ws_map: Dict[str, gspread.Worksheet] = {}


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
            if any(x in s for x in ["429", "Quota", "Read requests", "500", "503"]):
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


# -------------------- HELPERS --------------------
def money(x: float) -> str:
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "$0.00"


def normalize_account_name(s: str) -> str:
    return " ".join(str(s or "").strip().split())


def type_to_display(t: str) -> str:
    m = {
        "Debit": "Expense (−)",
        "Credit": "Income (+)",
        "Investment": "Investment",
        "CC Repay": "Pay Credit Card",
        "International": "Remit International",
        "LOC Draw": "LOC Draw",
        "LOC Repay": "LOC Repay",
    }
    return m.get(str(t).strip(), str(t).strip() or "Debit")


def build_month_options(past_months: int = 12, future_months: int = 0) -> List[str]:
    base = dt.date.today().replace(day=1)
    out = []
    for i in range(past_months, 0, -1):
        d = base - relativedelta(months=i)
        out.append(f"{d.year:04d}-{d.month:02d}")
    out.append(f"{base.year:04d}-{base.month:02d}")
    for i in range(1, future_months + 1):
        d = base + relativedelta(months=i)
        out.append(f"{d.year:04d}-{d.month:02d}")
    return out


def parse_rules_text(text: str) -> Dict[str, List[str]]:
    rules: Dict[str, List[str]] = {}
    for line in (text or "").splitlines():
        if ":" not in line:
            continue
        cat, rest = line.split(":", 1)
        cat = cat.strip()
        kws = [k.strip().lower() for k in rest.split(",") if k.strip()]
        if cat:
            rules[cat] = kws
    if not rules:
        rules = DEFAULT_CATEGORY_RULES.copy()
    return rules


def classify(notes: str, rules: Dict[str, List[str]]) -> str:
    n = (notes or "").strip().lower()
    if not n:
        return "Uncategorized"
    best_cat = "Uncategorized"
    best_score = 0
    for cat, kws in (rules or {}).items():
        score = sum(1 for k in kws if k and k in n)
        if score > best_score:
            best_score = score
            best_cat = cat
    return best_cat or "Uncategorized"


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


def monthly_summary(tx: pd.DataFrame, month: str) -> Dict[str, float]:
    df = tx[tx["Month"] == month].copy()
    if df.empty:
        return {"Expense": 0.0, "Income": 0.0, "Investment": 0.0, "CC Repay": 0.0, "International": 0.0}
    totals = df.groupby("Type")["Amount"].sum().to_dict()
    return {
        "Expense": float(totals.get("Debit", 0)),
        "Income": float(totals.get("Credit", 0)),
        "Investment": float(totals.get("Investment", 0)),
        "CC Repay": float(totals.get("CC Repay", 0)),
        "International": float(totals.get("International", 0)),
    }


def upcoming_recurring_total(prefs_list: List[Dict[str, Any]], month: str) -> float:
    try:
        return float(sum(float(p.get("Amount", 0) or 0) for p in (prefs_list or []) if bool(p.get("IsRecurring", False))))
    except Exception:
        return 0.0


def utilization_table(tx: pd.DataFrame, month: str, acct_df: pd.DataFrame) -> pd.DataFrame:
    """Simple utilization: sum of Debit by card account in month / limit."""
    if acct_df is None or acct_df.empty:
        return pd.DataFrame()
    dfm = tx[tx["Month"] == month].copy()
    if dfm.empty:
        return pd.DataFrame()
    spend = dfm[dfm["Type"] == "Debit"].groupby("Account")["Amount"].sum().reset_index()
    base = acct_df.copy()
    base["Account"] = base["Account"].astype(str).apply(normalize_account_name)
    out = base.merge(spend, on="Account", how="left").rename(columns={"Amount": "Spend"})
    out["Spend"] = pd.to_numeric(out["Spend"], errors="coerce").fillna(0.0)
    out["Limit"] = pd.to_numeric(out["Limit"], errors="coerce").fillna(0.0)
    out["Util%"] = out.apply(lambda r: (r["Spend"] / r["Limit"] * 100.0) if r["Limit"] > 0 else 0.0, axis=1)
    return out[["Account", "Emoji", "Limit", "Spend", "Util%"]]


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


# -------------------- DATA LAYER --------------------
def refresh_all() -> None:
    global STATE
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
        acct_df = pd.DataFrame(rows, columns=hdr if hdr else ACCT_HEADERS)
        for c in ACCT_HEADERS:
            if c not in acct_df.columns:
                acct_df[c] = ""
        acct_df = acct_df[ACCT_HEADERS]
        acct_df["Limit"] = pd.to_numeric(acct_df["Limit"], errors="coerce").fillna(0.0)

    tx_vals = gs_call(ws_tx.get_all_values)
    if not tx_vals or len(tx_vals) < 2:
        tx_df = pd.DataFrame(columns=TX_HEADERS + ["_row", "DateParsed", "Month"])
    else:
        hdr = tx_vals[0]
        rows = tx_vals[1:]
        tx_df = pd.DataFrame(rows, columns=hdr if hdr else TX_HEADERS)
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
    col_index = {h: i + 1 for i, h in enumerate(TX_HEADERS)}
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
        rows.append([str(r.get("Account", "")), str(r.get("Emoji", "")), str(r.get("Limit", "")), str(r.get("BillingDay", ""))])
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
    """Auto-add recurring items once per month using AutoTag = AUTO:<month>:<MerchantKey>."""
    if STATE is None:
        refresh_all()
    tx = STATE.tx_df
    prefs_list = STATE.prefs_list

    existing_tags = set(
        tx.loc[(tx["Month"] == month_str) & (tx["AutoTag"].astype(str).str.startswith("AUTO:")), "AutoTag"]
        .astype(str)
        .tolist()
    )
    created = 0
    try:
        p = pd.Period(month_str, freq="M")
        year, month = p.year, p.month
    except Exception:
        return 0

    for pref in (prefs_list or []):
        if not bool(pref.get("IsRecurring", False)):
            continue
        mk = str(pref.get("MerchantKey", "")).strip()
        if not mk:
            continue
        dom = int(float(pref.get("DayOfMonth", 1) or 1))
        dom = max(1, min(28, dom))
        due = dt.date(year, month, dom)

        tag = f"AUTO:{month_str}:{mk}"
        if tag in existing_tags:
            continue

        entry_type = str(pref.get("Type", "Debit")).strip() or "Debit"
        amount = float(pref.get("Amount", 0) or 0)
        pay = str(pref.get("Pay", "")).strip()
        account = str(pref.get("Account", "")).strip()
        category = str(pref.get("Category", "")).strip() or "Uncategorized"
        notes = str(pref.get("Notes", mk)).strip() or mk
        owner = str(pref.get("Owner", "Family")).strip() or "Family"

        add_transaction(due, owner, entry_type, amount, pay, account, category, notes, auto_tag=tag)
        created += 1

    return created


# -------------------- UI THEME --------------------
def ui_theme() -> None:
    css = """
    <style>
      :root { --bg:#070B12; --card:#0E1625; --card2:#0B1220; --text:#E9EEF7; --muted:rgba(233,238,247,.68); --border:rgba(233,238,247,.12); }
      body{ background:var(--bg); color:var(--text); }
      .q-page{ background:var(--bg)!important; }
      .my-topbar{ background:linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,0)); border:1px solid var(--border); border-radius:18px; padding:12px 14px; box-shadow:0 10px 30px rgba(0,0,0,.35); }
      .my-card{ background: radial-gradient(1200px 600px at 0% 0%, rgba(121,132,255,.16), transparent 60%), radial-gradient(900px 400px at 100% 0%, rgba(62,255,202,.10), transparent 55%), var(--card);
        border:1px solid var(--border); border-radius:18px; box-shadow:0 10px 28px rgba(0,0,0,.38); }
      .my-card.flat{ background:var(--card2); }
      .my-pill{ border-radius:999px; }
      .my-muted{ color:var(--muted); }
      .my-title{ font-weight:900; font-size:18px; letter-spacing:.2px; }
      .my-sub{ font-size:13px; }
      .kpi{ font-size:22px; font-weight:900; }
      .chip{ display:inline-flex; align-items:center; gap:8px; padding:6px 10px; border-radius:999px; border:1px solid rgba(233,238,247,.14); background:rgba(255,255,255,.04); }
      .my-grid{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:12px; }
      @media (min-width: 980px){ .my-grid{ grid-template-columns: repeat(3, minmax(0,1fr)); } }
      .tile{ cursor:pointer; transition:transform .08s ease, box-shadow .08s ease, border-color .08s ease; }
      .tile:hover{ transform:translateY(-1px); box-shadow:0 14px 34px rgba(0,0,0,.44); border-color:rgba(233,238,247,.22); }
      .my-table .q-table__container{ background:var(--card2)!important; border:1px solid var(--border); border-radius:16px; }
      .q-table__top,.q-table__bottom{ background:var(--card2)!important; }
      .q-field__control{ border-radius:14px!important; }
      .q-btn{ text-transform:none!important; }
      .q-tabs{ border-radius:16px; overflow:hidden; border:1px solid var(--border); }
      .q-tab__label{ font-weight:700; }
    </style>
    """
    ui.add_head_html(css)


def auth_ok(u: str, p: str) -> bool:
    return hmac.compare_digest(u or "", AUTH_USERNAME) and hmac.compare_digest(p or "", AUTH_PASSWORD)


def notify_error(e: Exception, prefix: str = "") -> None:
    ui.notify(f"{prefix}{e}", type="negative")


def rules_text_from_state() -> str:
    if STATE is None:
        return ""
    lines = []
    for k, v in (STATE.rules or {}).items():
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

                g = txm.groupby("Type")["Amount"].sum().reset_index()
                g["TypeLabel"] = g["Type"].apply(type_to_display)
                fig = px.bar(g, x="TypeLabel", y="Amount", title="Totals by Type")
                fig.update_layout(paper_bgcolor="#070B12", plot_bgcolor="#070B12", font_color="#E9EEF7")
                ui.plotly(fig).classes("w-full")

                # Debit categories chart
                deb = txm[txm["Type"] == "Debit"].copy()
                if not deb.empty:
                    cg = deb.groupby("Category")["Amount"].sum().reset_index().sort_values("Amount", ascending=False).head(12)
                    fig2 = px.bar(cg, x="Category", y="Amount", title="Top debit categories")
                    fig2.update_layout(paper_bgcolor="#070B12", plot_bgcolor="#070B12", font_color="#E9EEF7")
                    ui.plotly(fig2).classes("w-full")

            render_dashboard()

        # Add
        with ui.tab_panel("Add"):
            owners = sorted([o for o in STATE.tx_df["Owner"].astype(str).unique().tolist() if o and o != "nan"]) or ["Abhi", "Indhu"]
            accounts = sorted([a for a in STATE.acct_df["Account"].astype(str).tolist() if a and a != "nan"]) or sorted(STATE.tx_df["Account"].astype(str).unique().tolist())
            accounts = [normalize_account_name(a) for a in accounts if a and a != "nan"] or [""]
            categories = sorted([c for c in STATE.tx_df["Category"].astype(str).unique().tolist() if c and c != "nan"]) or ["Uncategorized"]
            pay_opts = ["Card", "Cash", "Interac", "Bank", "Online", "Other"]

            ui.label("Add Transaction").classes("my-title")
            ui.label("Tap a tile. Notes will auto-fill Category using Admin → Rules.").classes("my-muted my-sub")

            def open_add_dialog(entry_type: str):
                dlg = ui.dialog()
                with dlg, ui.card().classes("my-card p-5 w-[580px] max-w-[95vw]"):
                    ui.label(type_to_display(entry_type)).classes("text-lg font-bold")

                    d_owner = ui.select(owners, value=owners[0], label="Owner").classes("w-full")
                    d_date = ui.input('Date', value=dt.date.today().isoformat()).props('type=date').classes("w-full")
                    d_amount = ui.number(label="Amount", value=0.0, format="%.2f").classes("w-full")
                    d_pay = ui.select(pay_opts, value=("Card" if entry_type == "Debit" else "Other"), label="Pay / Method").classes("w-full")
                    d_acct = ui.select(accounts, value=(accounts[0] if accounts else ""), label="Account").classes("w-full")
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
                    ("LOC Draw", "move_up", "LOC Draw"),
                    ("LOC Repay", "move_down", "LOC Repay"),
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
                ut = utilization_table(tx, msel.value, STATE.acct_df)
                if ut.empty:
                    ui.label("No utilization data (check cards tab limits).").classes("my-muted")
                else:
                    ut2 = ut.copy()
                    ut2["Limit"] = ut2["Limit"].apply(money)
                    ut2["Spend"] = ut2["Spend"].apply(money)
                    ut2["Util%"] = ut2["Util%"].apply(lambda x: f"{x:.1f}%")
                    ui.table(
                        columns=[{"name": c, "label": c, "field": c} for c in ut2.columns],
                        rows=ut2.to_dict("records"),
                        row_key="Account",
                    ).classes("w-full my-table")

                upcoming = upcoming_recurring_total(STATE.prefs_list, msel.value)
                with ui.card().classes("my-card flat p-4 mt-3"):
                    ui.label("Upcoming recurring (month)").classes("my-muted my-sub")
                    ui.label(money(float(upcoming))).classes("kpi")

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

                show = df[["Date", "Owner", "Type", "Amount", "Pay", "Account", "Category", "Notes", "AutoTag", "_row"]].copy()
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

                            d_date = ui.input('Date', value=str(r.get("Date", ""))[:10] or dt.date.today().isoformat()).props('type=date').classes("w-full")
                            d_owner = ui.input("Owner", value=str(r.get("Owner", ""))).classes("w-full")
                            d_type = ui.input("Type", value=str(r.get("Type", ""))).classes("w-full")
                            d_amount = ui.number("Amount", value=float(r.get("Amount", 0) or 0), format="%.2f").classes("w-full")
                            d_pay = ui.input("Pay", value=str(r.get("Pay", ""))).classes("w-full")
                            d_acct = ui.input("Account", value=str(r.get("Account", ""))).classes("w-full")
                            d_cat = ui.input("Category", value=str(r.get("Category", ""))).classes("w-full")
                            d_notes = ui.textarea("Notes", value=str(r.get("Notes", ""))).classes("w-full")
                            d_tag = ui.input("AutoTag", value=str(r.get("AutoTag", ""))).classes("w-full")

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
                            save_admin(locks, rules_text_from_state(), STATE.rules_locked, STATE.prefs_list)
                            ui.notify("Saved locks", type="positive")
                            ui.navigate.to("/app")
                        except Exception as e:
                            notify_error(e, "Save locks failed: ")

                    ui.button("Save locks", on_click=save_locks).classes("my-pill")

                with ui.tab_panel("Rules"):
                    ta = ui.textarea("Rules (Category: keyword1, keyword2)", value=rules_text_from_state()).classes("w-full")
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
                        ui.table(
                            columns=[{"name": c, "label": c, "field": c} for c in dfp.columns],
                            rows=dfp.to_dict("records"),
                            row_key=(dfp.columns[0] if len(dfp.columns) else "MerchantKey"),
                        ).classes("w-full my-table")
                    else:
                        ui.label("No recurring templates yet.").classes("my-muted")

                    with ui.card().classes("my-card p-4 mt-3"):
                        ui.label("Add / Update template").classes("font-semibold")
                        isrec = ui.switch("IsRecurring", value=True)
                        mk = ui.input("MerchantKey (unique)", value="").classes("w-full")
                        dom = ui.number("DayOfMonth", value=1, format="%.0f").classes("w-full")
                        owner = ui.input("Owner", value="Family").classes("w-full")
                        rtype = ui.select(["Debit", "Credit", "Investment", "CC Repay", "International", "LOC Draw", "LOC Repay"], value="Debit", label="Type").classes("w-full")
                        amt = ui.number("Amount", value=0.0, format="%.2f").classes("w-full")
                        pay = ui.input("Pay", value="Card").classes("w-full")
                        acct_choices = accounts if accounts else [""]
                        acct = ui.select(acct_choices, value=(acct_choices[0] if acct_choices else ""), label="Account").classes("w-full")
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
                                save_admin(STATE.locked_months, rules_text_from_state(), STATE.rules_locked, plist)
                                ui.notify("Saved template", type="positive")
                                ui.navigate.to("/app")
                            except Exception as e:
                                notify_error(e, "Save template failed: ")

                        def delete_template():
                            try:
                                key = (mk.value or "").strip()
                                if not key:
                                    raise ValueError("MerchantKey required")
                                plist = [p for p in (STATE.prefs_list or []) if str(p.get("MerchantKey", "")).strip() != key]
                                save_admin(STATE.locked_months, rules_text_from_state(), STATE.rules_locked, plist)
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
                        ui.table(
                            columns=[{"name": c, "label": c, "field": c} for c in ACCT_HEADERS],
                            rows=df.to_dict("records"),
                            row_key="Account",
                        ).classes("w-full my-table")
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
                                    d.loc[d["Account"].astype(str) == acc, ["Emoji", "Limit", "BillingDay"]] = [row["Emoji"], row["Limit"], row["BillingDay"]]
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
                                d_date = ui.input('Date', value=str(r.get("Date", ""))[:10] or dt.date.today().isoformat()).props('type=date').classes("w-full")
                                d_owner = ui.input("Owner", value=str(r.get("Owner", ""))).classes("w-full")
                                d_type = ui.input("Type", value=str(r.get("Type", ""))).classes("w-full")
                                d_amount = ui.number("Amount", value=float(r.get("Amount", 0) or 0), format="%.2f").classes("w-full")
                                d_pay = ui.input("Pay", value=str(r.get("Pay", ""))).classes("w-full")
                                d_acct = ui.input("Account", value=str(r.get("Account", ""))).classes("w-full")
                                d_cat = ui.input("Category", value=str(r.get("Category", ""))).classes("w-full")
                                d_notes = ui.textarea("Notes", value=str(r.get("Notes", ""))).classes("w-full")
                                d_tag = ui.input("AutoTag", value=str(r.get("AutoTag", ""))).classes("w-full")

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
    ui.run(
    host="0.0.0.0",
    port=int(os.environ.get("PORT", 8080)),
    reload=False,
    title=APP_NAME,
    storage_secret=os.environ.get("NICEGUI_STORAGE_SECRET", "change-me"),
)
