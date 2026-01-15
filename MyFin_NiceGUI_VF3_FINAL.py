
"""
MyFin — NiceGUI Stable
File: Myfin_NICGUI_V1HF7_STABLE.py

Purpose
- A stable NiceGUI implementation that you can deploy on Render and use instead of Streamlit.
- Focus on correctness + usability + a consistent dark “banking style” UI.

Key behavior changes (requested)
1) Recurring:
   - Marking an entry as recurring creates/updates a TEMPLATE in the "recurring" sheet.
   - The app auto-creates the actual transaction ONLY when the due date arrives (and only once per month).
   - No backfilling past months. No creating future months in advance.

2) Pay cycles (for dashboard clarity)
   - Abhi: semimonthly on 15th & 30th, moved to the previous Friday if it falls on weekend.
   - Indhu: biweekly Friday from anchor date 2026-01-16.

Required Render environment variables
- SERVICE_ACCOUNT_JSON: Paste your service_account.json contents (full JSON).
- NICEGUI_STORAGE_SECRET: Any long random string (32+ chars recommended).

Optional environment variables
- SPREADSHEET_NAME (default: nishanthfintrack_2026)
- APP_USER (default: admin)
- APP_PASS (default: admin)
- TIMEZONE (default: America/Winnipeg)

Expected Google Sheet tabs (auto-created if missing)
- transactions
- cards
- recurring
- rules

Render start command
- python Myfin_NICGUI_V1HF5_STABLE.py
"""

from __future__ import annotations

import os
import json
import math
import time
import calendar
import hashlib
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px

import gspread
from google.oauth2.service_account import Credentials
from nicegui import ui, app


# -----------------------------

# -----------------------------
# Navigation helper (NiceGUI API compatibility)
# -----------------------------
def nav_to(path: str) -> None:
    """Navigate within the app across different NiceGUI versions."""
    try:
        # NiceGUI v2+ style
        if hasattr(ui, 'navigate') and hasattr(ui.navigate, 'to'):
            ui.navigate.to(path)
            return
    except Exception:
        pass
    try:
        # Older style (if present)
        if hasattr(ui, 'open'):
            nav_to(path)  # type: ignore[attr-defined]
            return
    except Exception:
        pass
    # Last resort: browser redirect
    ui.run_javascript(f"window.location.href='{path}'")


# Config
# -----------------------------
TZ = os.environ.get("TIMEZONE", "America/Winnipeg")
SPREADSHEET_NAME = os.environ.get("SPREADSHEET_NAME", "nishanthfintrack_2026")
SERVICE_ACCOUNT_JSON = (os.environ.get("SERVICE_ACCOUNT_JSON") or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or os.environ.get("GOOGLE_SERVICE_ACCOUNT") or "")
STORAGE_SECRET = os.environ.get("NICEGUI_STORAGE_SECRET")  # set on Render; will be auto-derived if empty
PORT = int(os.environ.get("PORT", "10000"))

# If no storage secret provided (e.g., local dev), derive a stable secret so sessions/login work.
if not STORAGE_SECRET:
    seed = SERVICE_ACCOUNT_JSON or os.environ.get("SPREADSHEET_NAME", "") or "local-dev"
    STORAGE_SECRET = hashlib.sha256(seed.encode("utf-8")).hexdigest()

APP_TITLE = "MyFin"
APP_SUBTITLE = "Finance Tracker"

# Pay cycle config
ABHI_PAY_DAYS = (15, 30)              # semimonthly
WIFE_PAY_ANCHOR = dt.date(2026, 1, 16)  # biweekly Friday anchor


# -----------------------------
# Utilities
# -----------------------------
def today() -> dt.date:
    return dt.date.today()

def month_key(d: dt.date) -> str:
    return f"{d.year:04d}-{d.month:02d}"

def parse_date(x: Any) -> Optional[dt.date]:
    if x is None:
        return None
    if isinstance(x, dt.datetime):
        return x.date()
    if isinstance(x, dt.date):
        return x
    s = str(x).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return None

def to_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, str):
            x = x.replace(",", "").replace("$", "").strip()
        return float(x)
    except Exception:
        return 0.0

def currency(x: float) -> str:
    return f"${x:,.2f}"

def is_weekend(d: dt.date) -> bool:
    return d.weekday() >= 5

def adjust_prev_workday(d: dt.date) -> dt.date:
    # weekends only, move backward to Friday
    while is_weekend(d):
        d = d - dt.timedelta(days=1)
    return d

def abhi_pay_dates_for_month(year: int, month: int) -> List[dt.date]:
    last_day = calendar.monthrange(year, month)[1]
    out = []
    for day in ABHI_PAY_DAYS:
        dd = min(day, last_day)
        out.append(adjust_prev_workday(dt.date(year, month, dd)))
    return sorted(set(out))

def wife_pay_dates_between(start: dt.date, end: dt.date) -> List[dt.date]:
    # biweekly from anchor
    if end < start:
        return []
    anchor = WIFE_PAY_ANCHOR
    delta = (start - anchor).days
    if delta <= 0:
        cur = anchor
    else:
        k = math.ceil(delta / 14)
        cur = anchor + dt.timedelta(days=14 * k)
    out: List[dt.date] = []
    while cur <= end:
        out.append(cur)
        cur += dt.timedelta(days=14)
    return out

def sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


# -----------------------------
# Google Sheets layer
# -----------------------------
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

TABS = {
    "transactions": ["id", "date", "owner", "type", "amount", "method", "account", "category", "notes",
                     "is_recurring", "recurring_id", "created_at"],
    "cards": ["card_name", "owner", "billing_day", "max_limit", "method_name"],
    "recurring": ["recurring_id", "owner", "type", "amount", "method", "account", "category", "notes",
                  "day_of_month", "start_date", "active", "last_generated_month"],
    "rules": ["keyword", "category"],
}

_gc: Optional[gspread.Client] = None
_ss = None
_ws: Dict[str, gspread.Worksheet] = {}

def get_client() -> gspread.Client:
    global _gc
    if _gc:
        return _gc
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Missing SERVICE_ACCOUNT_JSON env var")
    info = json.loads(SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(info, scopes=SCOPE)
    _gc = gspread.authorize(creds)
    return _gc

def get_spreadsheet():
    global _ss
    if _ss is not None:
        return _ss
    _ss = get_client().open(SPREADSHEET_NAME)
    return _ss

def ensure_tabs() -> None:
    ss = get_spreadsheet()
    existing = {w.title for w in ss.worksheets()}
    for tab, headers in TABS.items():
        if tab not in existing:
            ws = ss.add_worksheet(title=tab, rows=2000, cols=max(16, len(headers) + 4))
            ws.append_row(headers, value_input_option="USER_ENTERED")
        _ws[tab] = ss.worksheet(tab)

def ws(tab: str) -> gspread.Worksheet:
    if tab not in _ws:
        ensure_tabs()
    return _ws[tab]

def read_df(tab: str) -> pd.DataFrame:
    records = ws(tab).get_all_records()
    df = pd.DataFrame(records)
    for col in TABS[tab]:
        if col not in df.columns:
            df[col] = ""
    return df

def append_row(tab: str, row: Dict[str, Any]) -> None:
    headers = TABS[tab]
    ws(tab).append_row([row.get(h, "") for h in headers], value_input_option="USER_ENTERED")

def update_row_by_id(tab: str, id_col: str, id_value: str, updates: Dict[str, Any]) -> bool:
    w = ws(tab)
    values = w.get_all_values()
    if not values:
        return False
    headers = values[0]
    if id_col not in headers:
        return False
    ridx = headers.index(id_col) + 1
    rownum = None
    for r in range(2, len(values) + 1):
        if w.cell(r, ridx).value == id_value:
            rownum = r
            break
    if rownum is None:
        return False
    for k, v in updates.items():
        if k in headers:
            c = headers.index(k) + 1
            w.update_cell(rownum, c, v)
    return True

def delete_row_by_id(tab: str, id_col: str, id_value: str) -> bool:
    w = ws(tab)
    values = w.get_all_values()
    if not values:
        return False
    headers = values[0]
    if id_col not in headers:
        return False
    ridx = headers.index(id_col) + 1
    rownum = None
    for r in range(2, len(values) + 1):
        if w.cell(r, ridx).value == id_value:
            rownum = r
            break
    if rownum is None:
        return False
    w.delete_rows(rownum)
    return True


# -----------------------------
# Caching (performance)
# -----------------------------
CACHE_TTL = 15  # seconds
_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}

def cached_df(tab: str, force: bool = False) -> pd.DataFrame:
    now = time.time()
    if (not force) and tab in _cache and (now - _cache[tab][0] < CACHE_TTL):
        return _cache[tab][1].copy()
    df = read_df(tab)
    _cache[tab] = (now, df.copy())
    return df

def invalidate(*tabs: str) -> None:
    for t in tabs:
        _cache.pop(t, None)


# -----------------------------
# Rules + Category inference
# -----------------------------
def load_rules() -> List[Tuple[str, str]]:
    df = cached_df("rules")
    out: List[Tuple[str, str]] = []
    for _, r in df.iterrows():
        kw = str(r.get("keyword", "")).strip().lower()
        cat = str(r.get("category", "")).strip()
        if kw and cat:
            out.append((kw, cat))
    return out

def infer_category(notes: str, rules: List[Tuple[str, str]]) -> str:
    n = (notes or "").lower()
    for kw, cat in rules:
        if kw in n:
            return cat
    return "Uncategorized"


# -----------------------------
# Recurring logic (template -> real entries on/after due date)
# -----------------------------
def create_or_update_recurring_template(
    *,
    owner: str,
    type_: str,
    amount: float,
    method: str,
    account: str,
    category: str,
    notes: str,
    day_of_month: int,
    start_date: dt.date,
    active: bool = True,
) -> str:
    rdf = cached_df("recurring", force=True)
    # Key by owner+type+method+account+category+notes+day
    key = f"{owner}|{type_}|{method}|{account}|{category}|{notes}|{day_of_month}"
    rid = sha16(key)

    # if exists: update core fields
    if not rdf.empty and (rdf["recurring_id"].astype(str) == rid).any():
        update_row_by_id("recurring", "recurring_id", rid, {
            "owner": owner,
            "type": type_,
            "amount": amount,
            "method": method,
            "account": account,
            "category": category,
            "notes": notes,
            "day_of_month": str(day_of_month),
            "start_date": start_date.isoformat(),
            "active": "TRUE" if active else "FALSE",
        })
        invalidate("recurring")
        return rid

    # new template
    append_row("recurring", {
        "recurring_id": rid,
        "owner": owner,
        "type": type_,
        "amount": amount,
        "method": method,
        "account": account,
        "category": category,
        "notes": notes,
        "day_of_month": str(day_of_month),
        "start_date": start_date.isoformat(),
        "active": "TRUE" if active else "FALSE",
        "last_generated_month": "",
    })
    invalidate("recurring")
    return rid

def generate_recurring_for_date(d: dt.date) -> int:
    rdf = cached_df("recurring", force=True)
    if rdf.empty:
        return 0

    tx = cached_df("transactions", force=True)
    existing = set(tx["id"].astype(str).tolist()) if not tx.empty else set()

    created = 0
    this_month = month_key(d)

    for _, r in rdf.iterrows():
        active = str(r.get("active", "TRUE")).strip().upper() in ("TRUE", "1", "YES", "Y")
        if not active:
            continue

        rid = str(r.get("recurring_id", "")).strip()
        if not rid:
            continue

        # already generated this month?
        last_gen = str(r.get("last_generated_month", "")).strip()
        if last_gen == this_month:
            continue

        dom = int(to_float(r.get("day_of_month", 0)))
        if dom <= 0:
            continue

        last_day = calendar.monthrange(d.year, d.month)[1]
        dd = min(dom, last_day)
        target = dt.date(d.year, d.month, dd)

        start_date = parse_date(r.get("start_date")) or target
        if d < start_date:
            continue

        if d < target:
            continue  # only when date arrives

        tx_id = f"R-{rid}-{this_month}"
        if tx_id in existing:
            # mark generated
            update_row_by_id("recurring", "recurring_id", rid, {"last_generated_month": this_month})
            invalidate("recurring")
            continue

        append_row("transactions", {
            "id": tx_id,
            "date": target.isoformat(),
            "owner": str(r.get("owner", "")).strip(),
            "type": str(r.get("type", "Debit")).strip(),
            "amount": float(to_float(r.get("amount", 0))),
            "method": str(r.get("method", "Other")).strip(),
            "account": str(r.get("account", "")).strip(),
            "category": str(r.get("category", "Uncategorized")).strip(),
            "notes": str(r.get("notes", "")).strip(),
            "is_recurring": "TRUE",
            "recurring_id": rid,
            "created_at": dt.datetime.now().isoformat(timespec="seconds"),
        })
        created += 1
        update_row_by_id("recurring", "recurring_id", rid, {"last_generated_month": this_month})
        invalidate("transactions", "recurring")

    return created


# -----------------------------
# Auth
# -----------------------------
def check_login(username: str, password: str) -> bool:
    u = os.environ.get("APP_USER", "admin")
    p = os.environ.get("APP_PASS", "admin")
    return username == u and password == p

def require_login() -> bool:
    return bool(app.storage.user.get("logged_in"))

def logout() -> None:
    app.storage.user["logged_in"] = False
    nav_to("/login")


# -----------------------------
# UI Theme
# -----------------------------
BANK_CSS = r"""
:root {
  --mf-bg: #0b1220;
  --mf-surface: rgba(255,255,255,0.05);
  --mf-surface-2: rgba(255,255,255,0.08);
  --mf-border: rgba(255,255,255,0.12);
  --mf-text: rgba(255,255,255,0.92);
  --mf-muted: rgba(255,255,255,0.62);
  --mf-accent: #2e7dff;
  --mf-good: #22c55e;
  --mf-bad: #ef4444;
  --mf-warn: #fbbf24;
}

body, .q-layout, .q-page {
  background: radial-gradient(1200px 700px at 20% 10%, rgba(46,125,255,0.18), transparent 60%),
              radial-gradient(900px 600px at 80% 20%, rgba(34,197,94,0.12), transparent 55%),
              radial-gradient(900px 600px at 40% 90%, rgba(251,191,36,0.08), transparent 55%),
              var(--mf-bg) !important;
  color: var(--mf-text) !important;
}

.my-card {
  background: linear-gradient(180deg, rgba(255,255,255,0.08), rgba(255,255,255,0.04)) !important;
  border: 1px solid var(--mf-border) !important;
  border-radius: 18px !important;
  box-shadow: 0 18px 50px rgba(0,0,0,0.35);
  backdrop-filter: blur(10px);
}

.kpi {
  border-radius: 16px;
  border: 1px solid var(--mf-border);
  background: rgba(255,255,255,0.05);
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

.tile {
  cursor: pointer;
  transition: transform .12s ease, background .12s ease;
}
.tile:hover { transform: translateY(-2px); background: rgba(255,255,255,0.07) !important; }
"""
ui.add_head_html(f"<style>{BANK_CSS}</style>", shared=True)


# -----------------------------
# Layout
# -----------------------------
def topbar():
    with ui.row().classes("w-full items-center justify-between px-3 py-2"):
        with ui.row().classes("items-center gap-3"):
            ui.label("💳").classes("text-2xl")
            with ui.column().classes("gap-0"):
                ui.label(APP_TITLE).classes("text-lg font-bold")
                ui.label(APP_SUBTITLE).classes("text-xs").style("color: var(--mf-muted)")
        with ui.row().classes("items-center gap-2"):
            ui.button("Refresh", on_click=lambda: refresh_all()).props("flat").classes("text-sm")
            ui.button("Logout", on_click=logout).props("flat").classes("text-sm")

def nav_button(label: str, icon: str, path: str):
    ui.button(label, on_click=lambda: nav_to(path)).props(f"flat icon={icon}").classes("w-full")

def shell(content_fn):
    with ui.header().classes("bg-transparent"):
        topbar()

    with ui.left_drawer(value=False).classes("bg-transparent"):
        with ui.column().classes("p-3 gap-2"):
            with ui.card().classes("my-card p-3"):
                ui.label("Navigation").classes("font-bold")
                ui.separator()
                nav_button("Dashboard", "dashboard", "/")
                nav_button("Add", "add_circle", "/add")
                nav_button("Transactions", "receipt_long", "/tx")
                nav_button("Cards", "credit_card", "/cards")
                nav_button("Recurring", "autorenew", "/recurring")
                nav_button("Rules", "rule", "/rules")

    with ui.page_sticky(position="bottom-left", x_offset=18, y_offset=18):
        ui.button(icon="menu").props("round").on("click", lambda: ui.open_drawer())

    with ui.column().classes("w-full max-w-[1100px] mx-auto p-3 gap-3"):
        content_fn()


# -----------------------------
# Shared actions
# -----------------------------
def refresh_all():
    invalidate("transactions", "cards", "recurring", "rules")
    ui.notify("Refreshed", type="positive")


def owners_list() -> List[str]:
    # Prefer owners from cards, else from transactions, else defaults
    cards = cached_df("cards")
    owners = set()
    if not cards.empty:
        owners |= set(cards["owner"].astype(str).tolist())
    tx = cached_df("transactions")
    if not tx.empty:
        owners |= set(tx["owner"].astype(str).tolist())
    owners = {o.strip() for o in owners if o and o.strip()}
    if not owners:
        owners = {"Abhi", "Indhu"}
    return sorted(owners)


def accounts_list() -> List[str]:
    tx = cached_df("transactions")
    accts = set()
    if not tx.empty:
        accts |= set(tx["account"].astype(str).tolist())
    accts = {a.strip() for a in accts if a and a.strip()}
    return sorted(accts)


def categories_list() -> List[str]:
    tx = cached_df("transactions")
    cats = set()
    if not tx.empty:
        cats |= set(tx["category"].astype(str).tolist())
    cats = {c.strip() for c in cats if c and c.strip()}
    base = ["Uncategorized", "Groceries", "Rent", "Utilities", "Subscriptions", "Dining", "Fuel", "Shopping", "Travel", "Health", "Salary", "Transfer"]
    return sorted(set(base) | cats)


def methods_list() -> List[str]:
    cards = cached_df("cards")
    methods = set(["Debit", "Card", "Other"])
    if not cards.empty and "method_name" in cards.columns:
        methods |= set(cards["method_name"].astype(str).tolist())
    return sorted({m.strip() for m in methods if m and m.strip()})


# -----------------------------
# Pages
# -----------------------------
@ui.page("/login")
def login_page():
    with ui.column().classes("w-full max-w-[520px] mx-auto mt-10 p-4 gap-4"):
        with ui.card().classes("my-card p-6"):
            ui.label("Sign in").classes("text-xl font-bold")
            ui.label("Use your admin credentials.").classes("text-sm").style("color: var(--mf-muted)")
            u_in = ui.input("Username").classes("w-full")
            p_in = ui.input("Password", password=True, password_toggle_button=True).classes("w-full")

            def attempt():
                if check_login(u_in.value or "", p_in.value or ""):
                    app.storage.user["logged_in"] = True
                    ui.notify("Welcome 👋", type="positive")
                    nav_to("/")
                else:
                    ui.notify("Invalid login", type="negative")

            ui.button("Login", on_click=attempt).classes("w-full").props("unelevated")


@ui.page("/")
def dashboard_page():
    if not require_login():
        nav_to("/login")
        return

    def content():
        # Safe: run recurring generation for today once per page load
        try:
            created = generate_recurring_for_date(today())
            if created:
                ui.notify(f"Auto-added {created} recurring entries for {today().isoformat()}", type="positive")
        except Exception:
            pass

        tx = cached_df("transactions")
        if tx.empty:
            with ui.card().classes("my-card p-5"):
                ui.label("No transactions yet").classes("text-lg font-bold")
                ui.label("Go to Add to create your first entry.").style("color: var(--mf-muted)")
            return

        # --- normalize expected columns (robust to sheet header variations) ---
        def _first_col(df, candidates):
            for c in candidates:
                if c in df.columns:
                    return c
            # try case-insensitive match
            lower_map = {str(col).strip().lower(): col for col in df.columns}
            for c in candidates:
                key = str(c).strip().lower()
                if key in lower_map:
                    return lower_map[key]
            return None

        c_date = _first_col(tx, ["date", "Date", "DATE", "transaction_date", "Transaction Date"])
        c_amount = _first_col(tx, ["amount", "Amount", "AMOUNT", "amt", "Amt", "value", "Value"])
        c_type = _first_col(tx, ["type", "Type", "TYPE", "transaction_type", "Transaction Type", "Type (+/-)", "type (+/-)"])

        if c_date and c_date != "date":
            tx["date"] = tx[c_date]
        if c_amount and c_amount != "amount":
            tx["amount"] = tx[c_amount]
        if c_type and c_type != "type":
            tx["type"] = tx[c_type]

        # ensure columns exist even if the sheet is missing them
        if "date" not in tx.columns:
            tx["date"] = ""
        if "amount" not in tx.columns:
            tx["amount"] = 0
        if "type" not in tx.columns:
            tx["type"] = ""

        tx["date_parsed"] = tx["date"].apply(parse_date)
        tx = tx[tx["date_parsed"].notna()].copy()
        tx["amount_num"] = tx["amount"].apply(to_float)
        # Normalize "type" column (sheet headers may vary in casing/spaces)
        if "type" not in tx.columns:
            _colmap = {str(c).strip().lower(): c for c in tx.columns}
            _src = None
            for _k in ("type", "txn type", "transaction type", "tx type"):
                if _k in _colmap:
                    _src = _colmap[_k]
                    break
            if _src is None:
                for _k, _orig in _colmap.items():
                    if "type" in _k:
                        _src = _orig
                        break
            if _src is not None:
                tx["type"] = tx[_src]
            else:
                tx["type"] = ""
        tx["type_l"] = tx["type"].astype(str).str.lower().str.strip()

        mkey = month_key(today())
        mtx = tx[tx["date_parsed"].apply(lambda d: month_key(d) == mkey)].copy()

        # Defensive normalization: some sheets may have different header casing/spacing
        if "type_l" not in mtx.columns:
            # try to locate a type-like column (case/space-insensitive)
            colmap = {str(c).strip().lower(): c for c in mtx.columns}
            src = None
            for key in ("type", "txn type", "transaction type", "tx type", "category type"):
                if key in colmap:
                    src = colmap[key]
                    break
            if src is None:
                # fallback: any column containing "type"
                for k, orig in colmap.items():
                    if "type" in k:
                        src = orig
                        break
            if src is not None:
                mtx["type_l"] = mtx[src].astype(str).str.strip().str.lower()
            else:
                mtx["type_l"] = ""
        if "amount_num" not in mtx.columns:
            # try amount-like columns
            colmap2 = {str(c).strip().lower(): c for c in mtx.columns}
            src_amt = None
            for key in ("amount", "amt", "value", "cad", "amount_cad"):
                if key in colmap2:
                    src_amt = colmap2[key]
                    break
            if src_amt is not None:
                mtx["amount_num"] = pd.to_numeric(mtx[src_amt], errors="coerce").fillna(0.0)
            else:
                mtx["amount_num"] = 0.0
        typ = None
        if "type_l" in mtx.columns:
            typ = mtx["type_l"].astype(str).str.strip().str.lower()
        else:
            # fallback: use any type-like column
            _colmap = {str(c).strip().lower(): c for c in mtx.columns}
            _src = None
            for _k in ("type", "txn type", "transaction type", "tx type"):
                if _k in _colmap:
                    _src = _colmap[_k]; break
            if _src is None:
                for _k, _orig in _colmap.items():
                    if "type" in _k: _src = _orig; break
            if _src is not None:
                typ = mtx[_src].astype(str).str.strip().str.lower()
            else:
                typ = pd.Series([""] * len(mtx))
        amt = mtx["amount_num"] if "amount_num" in mtx.columns else pd.Series([0.0] * len(mtx))
        income = amt[typ.isin(["credit", "income"])].sum()
        expense = amt[typ.isin(["debit", "expense"])].sum()
        invest = amt[typ.isin(["investment"])].sum()
        net = income - expense - invest

        with ui.row().classes("w-full gap-3"):
            for label, val in [
                ("Income (this month)", income),
                ("Expenses (this month)", expense),
                ("Investments (this month)", invest),
                ("Net (this month)", net),
            ]:
                with ui.card().classes("my-card p-4 kpi w-full"):
                    ui.label(label).classes("text-sm").style("color: var(--mf-muted)")
                    ui.label(currency(val)).classes("text-2xl font-bold")
                    ui.label(mkey).classes("text-xs").style("color: var(--mf-muted)")

        # Upcoming paydays
        start = today()
        end = start + dt.timedelta(days=45)
        pays: List[Tuple[str, dt.date]] = []
        y, m = start.year, start.month
        for _ in range(3):
            for p in abhi_pay_dates_for_month(y, m):
                if start <= p <= end:
                    pays.append(("Abhi", p))
            m += 1
            if m == 13:
                y += 1
                m = 1
        for p in wife_pay_dates_between(start, end):
            if start <= p <= end:
                pays.append(("Indhu", p))
        pays = sorted(set(pays), key=lambda x: x[1])

        with ui.card().classes("my-card p-5"):
            ui.label("Upcoming paydays").classes("text-lg font-bold")
            if not pays:
                ui.label("No paydays in the next 45 days.").style("color: var(--mf-muted)")
            else:
                for who, d in pays[:12]:
                    ui.label(f"{who}: {d.strftime('%a, %b %d, %Y')}").classes("text-sm")

        # Spending breakdown
        with ui.card().classes("my-card p-5"):
            ui.label("Spending breakdown (this month)").classes("text-lg font-bold")
            spend = mtx[mtx["type_l"].isin(["debit", "expense"])].copy()
            if spend.empty:
                ui.label("No expenses this month.").style("color: var(--mf-muted)")
            else:
                spend["category"] = spend["category"].astype(str).replace("", "Uncategorized")
                agg = spend.groupby("category", as_index=False)["amount_num"].sum()
                fig = px.pie(agg, names="category", values="amount_num", hole=0.55)
                fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color="rgba(255,255,255,0.88)",
                )
                ui.plotly(fig).classes("w-full")

        # Trend
        with ui.card().classes("my-card p-5"):
            ui.label("Cashflow trend (last 90 days)").classes("text-lg font-bold")
            recent = tx[tx["date_parsed"] >= (today() - dt.timedelta(days=90))].copy()
            recent["day"] = recent["date_parsed"].astype(str)
            recent["sign"] = recent["type_l"].map(lambda t: 1 if t in ("credit", "income") else (-1 if t in ("debit", "expense", "investment") else 0))
            recent["signed_amount"] = recent["amount_num"] * recent["sign"]
            daily = recent.groupby("day", as_index=False)["signed_amount"].sum()
            fig2 = px.area(daily, x="day", y="signed_amount")
            fig2.update_layout(
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="rgba(255,255,255,0.88)",
            )
            ui.plotly(fig2).classes("w-full")

    shell(content)


@ui.page("/add")
def add_page():
    if not require_login():
        nav_to("/login")
        return

    def open_add_dialog(entry_type: str):
        rules = load_rules()
        owners = owners_list()
        accounts = accounts_list()
        categories = categories_list()
        methods = methods_list()

        dlg = ui.dialog()
        with dlg, ui.card().classes("my-card p-5 w-[620px] max-w-[95vw]"):
            ui.label(f"Add: {entry_type}").classes("text-lg font-bold")

            d_owner = ui.select(owners, value=owners[0], label="Owner").classes("w-full")
            d_date = ui.input("Date", value=today().isoformat()).props("type=date").classes("w-full")
            d_amount = ui.number("Amount", value=0.0, format="%.2f").classes("w-full")
            d_method = ui.select(methods, value=("Card" if entry_type.lower() == "debit" else "Other"), label="Method").classes("w-full")
            d_account = ui.select(accounts or [""], value=(accounts[0] if accounts else ""), label="Account").classes("w-full")
            d_category = ui.select(categories, value="Uncategorized", label="Category").classes("w-full")
            d_notes = ui.textarea("Notes", value="").classes("w-full")
            d_rec = ui.checkbox("Mark as recurring (creates template for future cycles only)")

            def autofill():
                d_category.value = infer_category(d_notes.value or "", rules)
                ui.notify("Category suggested", type="positive")

            ui.button("Auto-category", on_click=autofill).props("flat")

            def save():
                dd = parse_date(d_date.value) or today()
                amt = float(to_float(d_amount.value))
                owner = str(d_owner.value or "").strip()
                method = str(d_method.value or "Other").strip()
                account = str(d_account.value or "").strip()
                category = str(d_category.value or "Uncategorized").strip()
                notes = str(d_notes.value or "").strip()

                # Build tx id (unique)
                tx_id = sha16(f"{owner}|{dd.isoformat()}|{entry_type}|{amt}|{method}|{account}|{category}|{notes}|{dt.datetime.now().isoformat()}")

                rec_id = ""
                if d_rec.value:
                    # IMPORTANT: template starts next cycle. Start date is the selected date, but generation is month-by-month and only on/after due date.
                    # We set template day_of_month from selected date.
                    rec_id = create_or_update_recurring_template(
                        owner=owner,
                        type_=entry_type,
                        amount=amt,
                        method=method,
                        account=account,
                        category=category,
                        notes=notes,
                        day_of_month=dd.day,
                        start_date=dd,
                        active=True,
                    )

                append_row("transactions", {
                    "id": tx_id,
                    "date": dd.isoformat(),
                    "owner": owner,
                    "type": entry_type,
                    "amount": amt,
                    "method": method,
                    "account": account,
                    "category": category,
                    "notes": notes,
                    "is_recurring": "FALSE",
                    "recurring_id": rec_id,
                    "created_at": dt.datetime.now().isoformat(timespec="seconds"),
                })
                invalidate("transactions")
                ui.notify("Saved", type="positive")
                dlg.close()

            with ui.row().classes("w-full justify-end gap-2 mt-2"):
                ui.button("Cancel", on_click=dlg.close).props("flat")
                ui.button("Save", on_click=save).props("unelevated")

        dlg.open()

    def content():
        with ui.card().classes("my-card p-5"):
            ui.label("Quick Add").classes("text-lg font-bold")
            ui.label("Tap a tile to add an entry.").classes("text-sm").style("color: var(--mf-muted)")

            tiles = [
                ("Debit (Expense)", "shopping_cart", "Debit"),
                ("Credit (Income)", "payments", "Credit"),
                ("Investment", "savings", "Investment"),
                ("Card Repay", "credit_score", "Card Repay"),
                ("International", "public", "International"),
            ]

            with ui.row().classes("w-full gap-3"):
                for label, icon, etype in tiles:
                    with ui.card().classes("my-card p-4 tile w-full"):
                        ui.label(label).classes("font-bold")
                        ui.icon(icon).classes("text-2xl")
                        ui.button("Add", on_click=lambda e=etype: open_add_dialog(e)).props("flat").classes("mt-2")

        with ui.card().classes("my-card p-5"):
            ui.label("Today’s auto status").classes("text-lg font-bold")
            ui.label("Recurring entries will be created only when the due date arrives.").style("color: var(--mf-muted)")
            ui.button("Run recurring generation now", on_click=lambda: ui.notify(f"Created {generate_recurring_for_date(today())} entries", type="positive")).props("flat")

    shell(content)


@ui.page("/tx")
def transactions_page():
    if not require_login():
        nav_to("/login")
        return

    def content():
        tx = cached_df("transactions")
        if tx.empty:
            with ui.card().classes("my-card p-5"):
                ui.label("No transactions").classes("text-lg font-bold")
            return

        tx["date_parsed"] = tx["date"].apply(parse_date)
        tx = tx[tx["date_parsed"].notna()].copy()
        tx = tx.sort_values("date_parsed", ascending=False)

        owners = sorted({o for o in tx["owner"].astype(str).tolist() if o.strip()})
        types = sorted({t for t in tx["type"].astype(str).tolist() if t.strip()})

        with ui.card().classes("my-card p-5"):
            ui.label("Transactions").classes("text-lg font-bold")
            f_owner = ui.select(["All"] + owners, value="All", label="Owner").classes("w-full")
            f_type = ui.select(["All"] + types, value="All", label="Type").classes("w-full")
            f_text = ui.input("Search notes/category/account").classes("w-full")

            table = ui.table(columns=[
                {"name": "date", "label": "Date", "field": "date"},
                {"name": "owner", "label": "Owner", "field": "owner"},
                {"name": "type", "label": "Type", "field": "type"},
                {"name": "amount", "label": "Amount", "field": "amount"},
                {"name": "method", "label": "Method", "field": "method"},
                {"name": "account", "label": "Account", "field": "account"},
                {"name": "category", "label": "Category", "field": "category"},
                {"name": "notes", "label": "Notes", "field": "notes"},
                {"name": "id", "label": "ID", "field": "id"},
            ], rows=[], row_key="id").classes("w-full")

            def refresh_table():
                df = tx.copy()
                if f_owner.value != "All":
                    df = df[df["owner"].astype(str) == f_owner.value]
                if f_type.value != "All":
                    df = df[df["type"].astype(str) == f_type.value]
                q = (f_text.value or "").strip().lower()
                if q:
                    hay = (df["notes"].astype(str) + " " + df["category"].astype(str) + " " + df["account"].astype(str)).str.lower()
                    df = df[hay.str.contains(q, na=False)]
                df = df.head(250)
                df["amount"] = df["amount"].apply(lambda x: currency(to_float(x)))
                table.rows = df.to_dict(orient="records")
                table.update()

            f_owner.on("update:model-value", lambda e: refresh_table())
            f_type.on("update:model-value", lambda e: refresh_table())
            f_text.on("update:model-value", lambda e: refresh_table())

            refresh_table()

            # Edit/Delete
            def open_edit(row: Dict[str, Any]):
                dlg = ui.dialog()
                with dlg, ui.card().classes("my-card p-5 w-[720px] max-w-[95vw]"):
                    ui.label("Edit transaction").classes("text-lg font-bold")
                    tid = str(row.get("id", "")).strip()

                    e_date = ui.input("Date", value=str(row.get("date", ""))).props("type=date").classes("w-full")
                    e_owner = ui.input("Owner", value=str(row.get("owner", ""))).classes("w-full")
                    e_type = ui.input("Type", value=str(row.get("type", ""))).classes("w-full")
                    e_amount = ui.number("Amount", value=to_float(row.get("amount", 0))).classes("w-full")
                    e_method = ui.input("Method", value=str(row.get("method", ""))).classes("w-full")
                    e_account = ui.input("Account", value=str(row.get("account", ""))).classes("w-full")
                    e_category = ui.input("Category", value=str(row.get("category", ""))).classes("w-full")
                    e_notes = ui.textarea("Notes", value=str(row.get("notes", ""))).classes("w-full")

                    def save_edit():
                        ok = update_row_by_id("transactions", "id", tid, {
                            "date": (parse_date(e_date.value) or today()).isoformat(),
                            "owner": e_owner.value or "",
                            "type": e_type.value or "",
                            "amount": float(to_float(e_amount.value)),
                            "method": e_method.value or "",
                            "account": e_account.value or "",
                            "category": e_category.value or "",
                            "notes": e_notes.value or "",
                        })
                        if ok:
                            invalidate("transactions")
                            ui.notify("Updated", type="positive")
                            dlg.close()
                            nav_to("/tx")
                        else:
                            ui.notify("Could not update (id not found)", type="negative")

                    with ui.row().classes("w-full justify-end gap-2"):
                        ui.button("Cancel", on_click=dlg.close).props("flat")
                        ui.button("Save", on_click=save_edit).props("unelevated")
                dlg.open()

            def open_delete(row: Dict[str, Any]):
                tid = str(row.get("id", "")).strip()
                if delete_row_by_id("transactions", "id", tid):
                    invalidate("transactions")
                    ui.notify("Deleted", type="positive")
                    nav_to("/tx")
                else:
                    ui.notify("Delete failed", type="negative")

            with ui.row().classes("gap-2 mt-3"):
                ui.button("Edit selected", on_click=lambda: open_edit(table.selected[0]) if table.selected else ui.notify("Select a row", type="warning")).props("flat")
                ui.button("Delete selected", on_click=lambda: open_delete(table.selected[0]) if table.selected else ui.notify("Select a row", type="warning")).props("flat")

    shell(content)


@ui.page("/cards")
def cards_page():
    if not require_login():
        nav_to("/login")
        return

    def content():
        cards = cached_df("cards")
        with ui.card().classes("my-card p-5"):
            ui.label("Cards").classes("text-lg font-bold")
            ui.label("Billing day is day-of-month.").style("color: var(--mf-muted)")

            t = ui.table(columns=[
                {"name": "card_name", "label": "Card", "field": "card_name"},
                {"name": "owner", "label": "Owner", "field": "owner"},
                {"name": "billing_day", "label": "Billing Day", "field": "billing_day"},
                {"name": "max_limit", "label": "Max Limit", "field": "max_limit"},
                {"name": "method_name", "label": "Method Name", "field": "method_name"},
            ], rows=cards.to_dict(orient="records") if not cards.empty else [], row_key="card_name").classes("w-full")

            with ui.row().classes("w-full gap-3 mt-3"):
                n_name = ui.input("Card name").classes("w-full")
                n_owner = ui.input("Owner").classes("w-full")
                n_bill = ui.number("Billing day", value=1).classes("w-full")
                n_lim = ui.number("Max limit", value=0).classes("w-full")
                n_meth = ui.input("Method name (optional)", value="Card").classes("w-full")

            def add_card():
                append_row("cards", {
                    "card_name": n_name.value or "",
                    "owner": n_owner.value or "",
                    "billing_day": int(to_float(n_bill.value)),
                    "max_limit": float(to_float(n_lim.value)),
                    "method_name": n_meth.value or "Card",
                })
                invalidate("cards")
                ui.notify("Card added", type="positive")
                nav_to("/cards")

            ui.button("Add card", on_click=add_card).props("unelevated")

    shell(content)


@ui.page("/recurring")
def recurring_page():
    if not require_login():
        nav_to("/login")
        return

    def content():
        rdf = cached_df("recurring", force=True)
        with ui.card().classes("my-card p-5"):
            ui.label("Recurring templates").classes("text-lg font-bold")
            ui.label("Templates only. Transactions get created when the due date arrives.").style("color: var(--mf-muted)")

            if rdf.empty:
                ui.label("No templates yet. Mark an Add entry as recurring to create one.").style("color: var(--mf-muted)")
                return

            rdf2 = rdf.copy()
            rdf2["active"] = rdf2["active"].astype(str)
            table = ui.table(columns=[
                {"name": "recurring_id", "label": "ID", "field": "recurring_id"},
                {"name": "owner", "label": "Owner", "field": "owner"},
                {"name": "type", "label": "Type", "field": "type"},
                {"name": "amount", "label": "Amount", "field": "amount"},
                {"name": "day_of_month", "label": "Day", "field": "day_of_month"},
                {"name": "category", "label": "Category", "field": "category"},
                {"name": "active", "label": "Active", "field": "active"},
                {"name": "last_generated_month", "label": "Last Gen", "field": "last_generated_month"},
            ], rows=rdf2.to_dict(orient="records"), row_key="recurring_id").classes("w-full")

            def toggle_active():
                if not table.selected:
                    ui.notify("Select a row", type="warning")
                    return
                row = table.selected[0]
                rid = str(row.get("recurring_id", ""))
                cur = str(row.get("active", "TRUE")).strip().upper() in ("TRUE", "1", "YES", "Y")
                update_row_by_id("recurring", "recurring_id", rid, {"active": "FALSE" if cur else "TRUE"})
                invalidate("recurring")
                nav_to("/recurring")

            def delete_template():
                if not table.selected:
                    ui.notify("Select a row", type="warning")
                    return
                rid = str(table.selected[0].get("recurring_id", ""))
                if delete_row_by_id("recurring", "recurring_id", rid):
                    invalidate("recurring")
                    ui.notify("Deleted template", type="positive")
                    nav_to("/recurring")
                else:
                    ui.notify("Delete failed", type="negative")

            with ui.row().classes("gap-2 mt-3"):
                ui.button("Toggle active", on_click=toggle_active).props("flat")
                ui.button("Delete template", on_click=delete_template).props("flat")
                ui.button("Run generation (today)", on_click=lambda: ui.notify(f"Created {generate_recurring_for_date(today())} entries", type="positive")).props("flat")

    shell(content)


@ui.page("/rules")
def rules_page():
    if not require_login():
        nav_to("/login")
        return

    def content():
        rdf = cached_df("rules", force=True)
        with ui.card().classes("my-card p-5"):
            ui.label("Rules").classes("text-lg font-bold")
            ui.label("Keyword → category mapping used for Auto-category in Add.").style("color: var(--mf-muted)")

            table = ui.table(columns=[
                {"name": "keyword", "label": "Keyword", "field": "keyword"},
                {"name": "category", "label": "Category", "field": "category"},
            ], rows=rdf.to_dict(orient="records") if not rdf.empty else [], row_key="keyword").classes("w-full")

            with ui.row().classes("w-full gap-3 mt-3"):
                k = ui.input("Keyword (lowercase recommended)").classes("w-full")
                c = ui.input("Category").classes("w-full")

            def add_rule():
                append_row("rules", {"keyword": k.value or "", "category": c.value or ""})
                invalidate("rules")
                ui.notify("Rule added", type="positive")
                nav_to("/rules")

            def del_rule():
                if not table.selected:
                    ui.notify("Select a row", type="warning")
                    return
                kw = str(table.selected[0].get("keyword", ""))
                if delete_row_by_id("rules", "keyword", kw):
                    invalidate("rules")
                    ui.notify("Deleted", type="positive")
                    nav_to("/rules")
                else:
                    ui.notify("Delete failed", type="negative")

            with ui.row().classes("gap-2 mt-3"):
                ui.button("Add rule", on_click=add_rule).props("unelevated")
                ui.button("Delete selected", on_click=del_rule).props("flat")

    shell(content)


# -----------------------------
# Boot
# -----------------------------
def bootstrap():
    ensure_tabs()

bootstrap()

ui.run(
    host="0.0.0.0",
    port=PORT,
    storage_secret=STORAGE_SECRET or "PLEASE_SET_NICEGUI_STORAGE_SECRET",
    title=APP_TITLE,
)
