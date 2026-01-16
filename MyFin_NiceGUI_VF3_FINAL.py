#!/usr/bin/env python3
"""MyFin - NiceGUI VF2 Phase 2

This phase focuses on making the app *actually live* with your Google Sheet while keeping the
Phase-1 skeleton you liked.

Key changes vs Phase 1:
- Robust worksheet auto-detection: supports both lowercase ("transactions") and Title Case ("Transactions").
- Transactions page now reads live data + has refresh.
- Add flows write to sheet (including LOC Withdrawal/Repayment).
- Cards page shows card-style tiles and reads live data.
- Owner is no longer a required input; we store Owner='Family' by default for compatibility.

ENV VARS (required):
- SERVICE_ACCOUNT_JSON : full JSON string for your Google service account
- GOOGLE_SHEET_ID      : Spreadsheet id (from the URL between /d/ and /edit)
- MYFIN_USERNAME       : login username
- MYFIN_PASSWORD       : login password
- STORAGE_SECRET       : any random long string (for session storage)

Optional:
- SHEET_TX, SHEET_CARDS, SHEET_RULES, SHEET_RECURRING, SHEET_ADMIN
  (defaults auto-detect to your common names)
"""

import os
import json
import uuid
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import pandas as pd

from nicegui import ui, app

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

APP_TITLE = 'MyFin'
CACHE_TTL_SEC = 30

# Candidate worksheet names (auto-detect)
DEFAULT_WS_NAMES = {
    'transactions': ['transactions', 'Transactions', 'TX', 'Tx', 'Transaction', 'Transactions 2026'],
    'cards': ['cards', 'Cards', 'accounts', 'Accounts', 'CreditCards', 'Credit Cards'],
    'rules': ['rules', 'Rules'],
    'recurring': ['recurring', 'Recurring'],
    'admin': ['admin', 'Admin'],
}

# Canonical headers (we create missing sheets with these)
TX_HEADERS = ['TxId', 'Date', 'Owner', 'Type', 'Amount', 'Pay', 'Account', 'Category', 'Notes', 'CreatedAt', 'AutoTag']
CARDS_HEADERS = ['Card', 'BillingDay', 'MaxLimit', 'Notes']
RULES_HEADERS = ['Keyword', 'Category']
RECUR_HEADERS = ['Key', 'Value']
ADMIN_HEADERS = ['Key', 'Value']

# In-app types (kept aligned with your Streamlit labels)
TX_TYPES = [
    'Expense',
    'Income',
    'Investment',
    'Credit Card Repayment',
    'LOC Withdrawal',
    'LOC Repayment',
]

# ============================
# Env helpers
# ============================

def env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f'Missing required env var: {name}')
    return v


def env_optional(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v else default


# ============================
# Google Sheets helpers
# ============================

def _auth_client() -> gspread.Client:
    sa_json = env_required('SERVICE_ACCOUNT_JSON')
    try:
        info = json.loads(sa_json)
    except Exception as e:
        raise RuntimeError('SERVICE_ACCOUNT_JSON is not valid JSON') from e

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def _open_spreadsheet(client: gspread.Client) -> gspread.Spreadsheet:
    sheet_id = env_required('GOOGLE_SHEET_ID')
    return client.open_by_key(sheet_id)


def _list_titles(ss: gspread.Spreadsheet) -> List[str]:
    try:
        return [w.title for w in ss.worksheets()]
    except Exception:
        return []


def _resolve_ws(ss: gspread.Spreadsheet, kind: str, headers: List[str]) -> gspread.Worksheet:
    # allow overrides
    override = os.getenv(f'SHEET_{kind.upper()}')
    candidates = [override] if override else []
    candidates += DEFAULT_WS_NAMES[kind]

    existing = {w.title: w for w in ss.worksheets()}
    for t in candidates:
        if t and t in existing:
            return existing[t]

    # If none exists, create using the first non-empty candidate or a sensible default
    create_title = next((c for c in candidates if c), kind.title())
    ws = ss.add_worksheet(title=create_title, rows=2000, cols=max(10, len(headers)))
    ws.append_row(headers)
    return ws


def _get_all_values(ws: gspread.Worksheet) -> List[List[str]]:
    try:
        return ws.get_all_values()
    except Exception:
        return []


def _ensure_headers(ws: gspread.Worksheet, headers: List[str]) -> None:
    vals = _get_all_values(ws)
    if not vals:
        ws.append_row(headers)
        return
    current = vals[0]
    if [c.strip() for c in current] == headers:
        return
    # If headers differ, we won't rewrite; we will map dynamically instead.


# ============================
# Data normalization
# ============================

def _parse_float(x: str) -> float:
    try:
        s = str(x).strip().replace(',', '')
        if s == '':
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def _parse_date(x: str) -> Optional[date]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    for fmt in ('%Y-%m-%d', '%d-%b-%Y', '%d/%m/%Y', '%m/%d/%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    # last resort: pandas
    try:
        return pd.to_datetime(s, errors='coerce').date()
    except Exception:
        return None


def _df_from_sheet(values: List[List[str]]) -> pd.DataFrame:
    if not values or len(values) < 2:
        return pd.DataFrame()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    # pad rows
    padded = [r + [''] * max(0, len(headers) - len(r)) for r in rows]
    return pd.DataFrame(padded, columns=headers)


def _normalize_transactions(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=['txid', 'date', 'owner', 'type', 'amount', 'pay', 'account', 'category', 'notes', 'createdat', 'autotag'])

    # map possible columns
    colmap = {}
    lower = {c.lower(): c for c in df_raw.columns}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n.lower() in lower:
                return lower[n.lower()]
        return None

    col_txid = pick('txid', 'id', 'TxId', 'TXID')
    col_date = pick('date', 'Date', 'txn_date', 'Transaction Date')
    col_owner = pick('owner', 'Owner')
    col_type = pick('type', 'Type', 'txn_type', 'Transaction Type')
    col_amount = pick('amount', 'Amount', 'amt')
    col_pay = pick('pay', 'Pay', 'method', 'Payment')
    col_account = pick('account', 'Account', 'card', 'Card')
    col_cat = pick('category', 'Category', 'cat')
    col_notes = pick('notes', 'Notes', 'reason', 'Reason')
    col_created = pick('createdat', 'CreatedAt', 'created_at')
    col_autotag = pick('autotag', 'AutoTag', 'auto_tag')

    out = pd.DataFrame()
    out['txid'] = df_raw[col_txid] if col_txid else ''
    out['date'] = df_raw[col_date] if col_date else ''
    out['owner'] = df_raw[col_owner] if col_owner else 'Family'
    out['type'] = df_raw[col_type] if col_type else ''
    out['amount'] = df_raw[col_amount] if col_amount else ''
    out['pay'] = df_raw[col_pay] if col_pay else ''
    out['account'] = df_raw[col_account] if col_account else ''
    out['category'] = df_raw[col_cat] if col_cat else ''
    out['notes'] = df_raw[col_notes] if col_notes else ''
    out['createdat'] = df_raw[col_created] if col_created else ''
    out['autotag'] = df_raw[col_autotag] if col_autotag else ''

    out['amount_num'] = out['amount'].apply(_parse_float)
    out['type_l'] = out['type'].astype(str).str.strip().str.lower()
    out['date_obj'] = out['date'].apply(_parse_date)
    return out


def _normalize_cards(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=['card', 'billingday', 'maxlimit', 'notes'])

    lower = {c.lower(): c for c in df_raw.columns}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n.lower() in lower:
                return lower[n.lower()]
        return None

    c_card = pick('card', 'Card', 'name', 'Name')
    c_bill = pick('billingday', 'BillingDay', 'billing', 'Billing Date', 'Billing')
    c_lim = pick('maxlimit', 'MaxLimit', 'limit', 'Max Limit')
    c_notes = pick('notes', 'Notes')

    out = pd.DataFrame()
    out['card'] = df_raw[c_card] if c_card else ''
    out['billingday'] = df_raw[c_bill] if c_bill else ''
    out['maxlimit'] = df_raw[c_lim] if c_lim else ''
    out['notes'] = df_raw[c_notes] if c_notes else ''
    out['billingday_num'] = out['billingday'].apply(_parse_float).astype(int, errors='ignore')
    out['maxlimit_num'] = out['maxlimit'].apply(_parse_float)
    return out


def _normalize_rules(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=['keyword', 'category'])
    lower = {c.lower(): c for c in df_raw.columns}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n.lower() in lower:
                return lower[n.lower()]
        return None

    c_k = pick('keyword', 'Keyword', 'key')
    c_c = pick('category', 'Category', 'value')

    out = pd.DataFrame()
    out['keyword'] = df_raw[c_k] if c_k else ''
    out['category'] = df_raw[c_c] if c_c else ''
    out['keyword_l'] = out['keyword'].astype(str).str.strip().str.lower()
    return out


# ============================
# Cache + data access
# ============================

_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}


def _now_ts() -> float:
    return datetime.utcnow().timestamp()


def cached(kind: str, loader, force: bool = False) -> pd.DataFrame:
    now = _now_ts()
    if (not force) and kind in _cache:
        ts, df = _cache[kind]
        if (now - ts) < CACHE_TTL_SEC:
            return df
    df = loader()
    _cache[kind] = (now, df)
    return df


def clear_cache() -> None:
    _cache.clear()


def get_ss() -> gspread.Spreadsheet:
    client = _auth_client()
    return _open_spreadsheet(client)


def load_transactions() -> pd.DataFrame:
    ss = get_ss()
    ws = _resolve_ws(ss, 'transactions', TX_HEADERS)
    vals = _get_all_values(ws)
    raw = _df_from_sheet(vals)
    return _normalize_transactions(raw)


def load_cards() -> pd.DataFrame:
    ss = get_ss()
    ws = _resolve_ws(ss, 'cards', CARDS_HEADERS)
    vals = _get_all_values(ws)
    raw = _df_from_sheet(vals)
    return _normalize_cards(raw)


def load_rules() -> pd.DataFrame:
    ss = get_ss()
    ws = _resolve_ws(ss, 'rules', RULES_HEADERS)
    vals = _get_all_values(ws)
    raw = _df_from_sheet(vals)
    return _normalize_rules(raw)


def _infer_category(notes: str, rules_df: pd.DataFrame) -> str:
    t = (notes or '').lower()
    if rules_df is None or rules_df.empty or not t:
        return ''
    # simple contains-match; first hit wins
    for _, r in rules_df.iterrows():
        k = str(r.get('keyword_l', '')).strip()
        if k and k in t:
            return str(r.get('category', '')).strip()
    return ''


def append_transaction(payload: Dict[str, str]) -> None:
    ss = get_ss()
    ws = _resolve_ws(ss, 'transactions', TX_HEADERS)
    vals = _get_all_values(ws)

    # detect header row
    if not vals:
        ws.append_row(TX_HEADERS)
        header = TX_HEADERS
    else:
        header = [h.strip() for h in vals[0]]

    # ensure required fields exist
    txid = payload.get('TxId') or str(uuid.uuid4())
    created = payload.get('CreatedAt') or datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    row_dict = {
        'TxId': txid,
        'Date': payload.get('Date', ''),
        'Owner': payload.get('Owner', 'Family'),
        'Type': payload.get('Type', ''),
        'Amount': payload.get('Amount', ''),
        'Pay': payload.get('Pay', ''),
        'Account': payload.get('Account', ''),
        'Category': payload.get('Category', ''),
        'Notes': payload.get('Notes', ''),
        'CreatedAt': created,
        'AutoTag': payload.get('AutoTag', ''),
    }

    row = [row_dict.get(h, '') for h in header]
    ws.append_row(row, value_input_option='USER_ENTERED')
    clear_cache()


# ============================
# Auth (simple)
# ============================

USERNAME = env_required('MYFIN_USERNAME')
PASSWORD = env_required('MYFIN_PASSWORD')
STORAGE_SECRET = env_required('STORAGE_SECRET')

app.storage_secret = STORAGE_SECRET


def is_logged_in() -> bool:
    return bool(app.storage.user.get('logged_in'))


def require_login() -> None:
    if not is_logged_in():
        ui.navigate.to('/login')


# ============================
# UI helpers (premium-ish dark by default)
# ============================

ui.colors(primary='#0f172a', secondary='#1f2937', accent='#22c55e', positive='#22c55e', negative='#ef4444', warning='#f59e0b')
ui.dark_mode().enable()

ui.add_head_html(
    """
    <style>
      :root { --mf-card: rgba(255,255,255,0.06); --mf-border: rgba(255,255,255,0.12); }
      .mf-card { background: var(--mf-card); border: 1px solid var(--mf-border); border-radius: 16px; }
      .mf-soft { color: rgba(255,255,255,0.72); }
      .mf-h1 { font-size: 18px; font-weight: 700; }
      .mf-h2 { font-size: 14px; font-weight: 600; }
      .mf-btn { border-radius: 999px !important; }
      .mf-chip { border: 1px solid var(--mf-border); border-radius: 999px; padding: 2px 10px; font-size: 12px; }
      .q-field--filled .q-field__control { background: rgba(255,255,255,0.06) !important; }
      .q-table__container { background: transparent !important; }
    </style>
    """
)


def money(x: float) -> str:
    return f"${x:,.2f}"


def top_shell(page_title: str, content_fn) -> None:
    require_login()

    drawer = ui.left_drawer(value=False).props('overlay bordered')
    with drawer:
        ui.label('MyFin').classes('mf-h1 q-mb-sm')
        ui.separator()
        ui.button('Dashboard', on_click=lambda: ui.navigate.to('/dashboard')).props('flat').classes('full-width')
        ui.button('Transactions', on_click=lambda: ui.navigate.to('/transactions')).props('flat').classes('full-width')
        ui.button('Add', on_click=lambda: ui.navigate.to('/add')).props('flat').classes('full-width')
        ui.button('Cards', on_click=lambda: ui.navigate.to('/cards')).props('flat').classes('full-width')
        ui.button('Rules', on_click=lambda: ui.navigate.to('/rules')).props('flat').classes('full-width')
        ui.button('Admin', on_click=lambda: ui.navigate.to('/admin')).props('flat').classes('full-width')
        ui.separator()
        ui.button('Logout', on_click=lambda: (app.storage.user.clear(), ui.navigate.to('/login'))).props('flat color=negative').classes('full-width')

    with ui.header().classes('items-center justify-between'):
        ui.button(icon='menu', on_click=lambda: drawer.toggle()).props('flat round').classes('mf-btn')
        ui.label(page_title).classes('mf-h2')
        ui.button(icon='refresh', on_click=lambda: (clear_cache(), ui.notify('Refreshed'))).props('flat round').classes('mf-btn')

    with ui.column().classes('w-full q-pa-md'):
        content_fn()

    # Mobile bottom bar for easy nav
    with ui.footer().classes('q-pa-xs'):
        with ui.row().classes('w-full justify-around'):
            ui.button(icon='dashboard', on_click=lambda: ui.navigate.to('/dashboard')).props('flat round').classes('mf-btn')
            ui.button(icon='receipt_long', on_click=lambda: ui.navigate.to('/transactions')).props('flat round').classes('mf-btn')
            ui.button(icon='add_circle', on_click=lambda: ui.navigate.to('/add')).props('flat round').classes('mf-btn')
            ui.button(icon='credit_card', on_click=lambda: ui.navigate.to('/cards')).props('flat round').classes('mf-btn')
            ui.button(icon='rule', on_click=lambda: ui.navigate.to('/rules')).props('flat round').classes('mf-btn')


# ============================
# Pages
# ============================

@ui.page('/')
def root_page():
    ui.navigate.to('/dashboard' if is_logged_in() else '/login')


@ui.page('/login')
def login_page():
    if is_logged_in():
        ui.navigate.to('/dashboard')
        return

    with ui.column().classes('w-full items-center justify-center q-pa-lg'):
        with ui.card().classes('mf-card w-full').style('max-width: 420px;'):
            ui.label('Sign in').classes('mf-h1')
            ui.label('MyFin secure access').classes('mf-soft q-mb-md')
            u = ui.input('Username').props('filled').classes('w-full')
            p = ui.input('Password', password=True, password_toggle_button=True).props('filled').classes('w-full')

            def do_login():
                if u.value == USERNAME and p.value == PASSWORD:
                    app.storage.user['logged_in'] = True
                    ui.navigate.to('/dashboard')
                else:
                    ui.notify('Invalid login', color='negative')

            ui.button('Login', on_click=do_login).props('unelevated').classes('w-full')


@ui.page('/dashboard')
def dashboard_page():
    def content():
        tx = cached('transactions', load_transactions, force=False)

        if tx.empty:
            ui.label('No transactions found yet.').classes('mf-soft')
            ui.label('Tip: confirm GOOGLE_SHEET_ID and SERVICE_ACCOUNT_JSON, then refresh.').classes('mf-soft')
            return

        income = tx.loc[tx['type_l'].isin(['income', 'credit']), 'amount_num'].sum()
        expense = tx.loc[tx['type_l'].isin(['expense', 'debit']), 'amount_num'].sum()
        invest = tx.loc[tx['type_l'].str.contains('invest', na=False), 'amount_num'].sum()
        net = income - expense - invest

        with ui.row().classes('w-full q-col-gutter-md'):
            for title, val in [('Income', income), ('Expense', expense), ('Investment', invest), ('Net', net)]:
                with ui.card().classes('mf-card col-12 col-sm-6 col-md-3'):
                    ui.label(title).classes('mf-soft')
                    ui.label(money(val)).classes('mf-h1')

        ui.separator().classes('q-my-md')

        # Recent transactions
        tx2 = tx.copy()
        tx2 = tx2.sort_values(by='date_obj', ascending=False, na_position='last').head(12)
        with ui.card().classes('mf-card'):
            ui.label('Recent').classes('mf-h2')
            cols = [
                {'name': 'date', 'label': 'Date', 'field': 'date'},
                {'name': 'type', 'label': 'Type', 'field': 'type'},
                {'name': 'amount', 'label': 'Amount', 'field': 'amount_num'},
                {'name': 'account', 'label': 'Account', 'field': 'account'},
                {'name': 'notes', 'label': 'Notes', 'field': 'notes'},
            ]
            rows = tx2[['date', 'type', 'amount_num', 'account', 'notes']].to_dict('records')
            ui.table(columns=cols, rows=rows, row_key='notes').props('dense flat')

    top_shell('Dashboard', content)


@ui.page('/transactions')
def transactions_page():
    def content():
        tx = cached('transactions', load_transactions)

        with ui.row().classes('w-full items-center q-col-gutter-sm'):
            month = ui.select(
                options=['All', 'This month', 'Last month'],
                value='All',
                label='Range'
            ).props('filled').classes('col-12 col-sm-4')
            txt = ui.input('Search notes/account').props('filled').classes('col-12 col-sm-8')

        def filtered_df() -> pd.DataFrame:
            df = tx
            if df.empty:
                return df
            df = df.copy()
            today = datetime.now().date()
            if month.value == 'This month':
                df = df[df['date_obj'].apply(lambda d: d and d.month == today.month and d.year == today.year)]
            elif month.value == 'Last month':
                y = today.year
                m = today.month - 1
                if m == 0:
                    m = 12
                    y -= 1
                df = df[df['date_obj'].apply(lambda d: d and d.month == m and d.year == y)]
            q = (txt.value or '').strip().lower()
            if q:
                df = df[df['notes'].astype(str).str.lower().str.contains(q) | df['account'].astype(str).str.lower().str.contains(q)]
            return df.sort_values(by='date_obj', ascending=False, na_position='last')

        with ui.card().classes('mf-card q-mt-md'):
            ui.label('Transactions').classes('mf-h2')
            table = ui.table(
                columns=[
                    {'name': 'date', 'label': 'Date', 'field': 'date'},
                    {'name': 'type', 'label': 'Type', 'field': 'type'},
                    {'name': 'amount', 'label': 'Amount', 'field': 'amount_num'},
                    {'name': 'account', 'label': 'Account', 'field': 'account'},
                    {'name': 'category', 'label': 'Category', 'field': 'category'},
                    {'name': 'notes', 'label': 'Notes', 'field': 'notes'},
                ],
                rows=filtered_df().head(400).to_dict('records'),
                row_key='txid',
            ).props('dense flat')

        def refresh_table():
            table.rows = filtered_df().head(400).to_dict('records')
            table.update()

        month.on('update:model-value', refresh_table)
        txt.on('update:model-value', refresh_table)

        ui.button('Force refresh from sheet', on_click=lambda: (cached('transactions', load_transactions, force=True), ui.notify('Reloaded'), refresh_table())).props('outline').classes('q-mt-md')

    top_shell('Transactions', content)


@ui.page('/add')
def add_page():
    def content():
        rules = cached('rules', load_rules)
        cards = cached('cards', load_cards)

        ui.label('Add transaction').classes('mf-h2 q-mb-sm')

        # Tile grid
        with ui.row().classes('w-full q-col-gutter-md'):
            for label, icon in [
                ('Expense', 'remove_circle'),
                ('Income', 'add_circle'),
                ('Investment', 'savings'),
                ('Credit Card Repayment', 'credit_score'),
                ('LOC Withdrawal', 'account_balance_wallet'),
                ('LOC Repayment', 'payments'),
            ]:
                with ui.card().classes('mf-card col-6 col-sm-4 col-md-2'):
                    ui.icon(icon).classes('text-2xl')
                    ui.label(label).classes('mf-h2')
                    ui.button('Open', on_click=lambda t=label: ui.navigate.to(f'/add/{t}')).props('flat').classes('mf-btn')

        ui.separator().classes('q-my-md')
        ui.label('Tip: choose an action above.').classes('mf-soft')

    top_shell('Add', content)


@ui.page('/add/{tx_type}')
def add_form_page(tx_type: str):
    def content():
        tx_type_clean = tx_type
        if tx_type_clean not in TX_TYPES:
            ui.notify('Unknown action', color='negative')
            ui.navigate.to('/add')
            return

        rules = cached('rules', load_rules)
        cards = cached('cards', load_cards)

        # shared fields
        with ui.card().classes('mf-card'):
            ui.label(tx_type_clean).classes('mf-h1')
            d = ui.input('Date (YYYY-MM-DD)', value=datetime.now().strftime('%Y-%m-%d')).props('filled')
            amt = ui.input('Amount').props('filled')
            acc_options = sorted([c for c in cards['card'].dropna().astype(str).tolist() if c.strip()]) if not cards.empty else []
            account = ui.select(acc_options, label='Account / Card (optional)').props('filled')
            pay = ui.select(['Debit', 'Credit', 'Interac', 'Transfer', 'Cash', 'LOC'], label='Pay (optional)').props('filled')
            notes = ui.textarea('Notes').props('filled')
            category = ui.input('Category (optional)').props('filled')

            # Defaults for LOC types
            if tx_type_clean in ['LOC Withdrawal', 'LOC Repayment']:
                pay.value = 'LOC'
                if not account.value:
                    account.value = 'Line of Credit'

            def submit():
                # basic validation
                if not d.value or not amt.value:
                    ui.notify('Date and Amount are required', color='negative')
                    return

                cat = category.value.strip() if category.value else ''
                auto = ''
                if not cat:
                    inferred = _infer_category(notes.value or '', rules)
                    if inferred:
                        cat = inferred
                        auto = 'Y'

                payload = {
                    'TxId': str(uuid.uuid4()),
                    'Date': str(d.value).strip(),
                    'Owner': 'Family',
                    'Type': tx_type_clean,
                    'Amount': str(amt.value).strip(),
                    'Pay': str(pay.value or '').strip(),
                    'Account': str(account.value or '').strip(),
                    'Category': cat,
                    'Notes': str(notes.value or '').strip(),
                    'CreatedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'AutoTag': auto,
                }

                try:
                    append_transaction(payload)
                    ui.notify('Saved', color='positive')
                    ui.navigate.to('/transactions')
                except Exception as e:
                    ui.notify(f'Failed to save: {e}', color='negative')

            with ui.row().classes('q-col-gutter-sm q-mt-md'):
                ui.button('Save', on_click=submit).props('unelevated').classes('mf-btn')
                ui.button('Back', on_click=lambda: ui.navigate.to('/add')).props('flat').classes('mf-btn')

    top_shell('Add', content)


@ui.page('/cards')
def cards_page():
    def content():
        cards = cached('cards', load_cards)

        ui.label('Cards / Accounts').classes('mf-h2 q-mb-sm')
        if cards.empty:
            ui.label('No cards found in sheet.').classes('mf-soft')
            ui.label('If your tab is named differently, set SHEET_CARDS env var.').classes('mf-soft')
            return

        with ui.row().classes('w-full q-col-gutter-md'):
            for _, r in cards.iterrows():
                name = str(r.get('card', '')).strip() or 'Card'
                bill = str(r.get('billingday', '')).strip()
                lim = r.get('maxlimit_num', 0.0)
                note = str(r.get('notes', '')).strip()

                with ui.card().classes('mf-card col-12 col-sm-6 col-md-4'):
                    ui.label(name).classes('mf-h1')
                    with ui.row().classes('q-col-gutter-sm q-mt-xs'):
                        ui.label(f'Billing: {bill}').classes('mf-chip')
                        ui.label(f'Limit: {money(lim)}').classes('mf-chip')
                    if note:
                        ui.label(note).classes('mf-soft q-mt-sm')

        ui.button('Force refresh from sheet', on_click=lambda: (cached('cards', load_cards, force=True), ui.notify('Reloaded'))).props('outline').classes('q-mt-md')

    top_shell('Cards', content)


@ui.page('/rules')
def rules_page():
    def content():
        rules = cached('rules', load_rules)

        with ui.card().classes('mf-card'):
            ui.label('Rules (keyword -> category)').classes('mf-h2')

            cols = [
                {'name': 'keyword', 'label': 'Keyword', 'field': 'keyword'},
                {'name': 'category', 'label': 'Category', 'field': 'category'},
            ]
            rows = rules[['keyword', 'category']].to_dict('records') if not rules.empty else []
            ui.table(columns=cols, rows=rows, row_key='keyword').props('dense flat')

            ui.separator().classes('q-my-md')
            ui.label('Add new rule').classes('mf-soft')
            k = ui.input('Keyword').props('filled')
            c = ui.input('Category').props('filled')

            def add_rule():
                if not k.value or not c.value:
                    ui.notify('Keyword and Category required', color='negative')
                    return
                ss = get_ss()
                ws = _resolve_ws(ss, 'rules', RULES_HEADERS)
                vals = _get_all_values(ws)
                if not vals:
                    ws.append_row(RULES_HEADERS)
                ws.append_row([k.value.strip(), c.value.strip()], value_input_option='USER_ENTERED')
                clear_cache()
                ui.notify('Rule saved', color='positive')
                ui.navigate.to('/rules')

            ui.button('Save rule', on_click=add_rule).props('unelevated').classes('mf-btn q-mt-sm')

    top_shell('Rules', content)


@ui.page('/admin')
def admin_page():
    def content():
        with ui.card().classes('mf-card'):
            ui.label('Admin').classes('mf-h2')
            ui.label('Phase 2: basic controls (more in Phase 3)').classes('mf-soft')
            ui.separator().classes('q-my-md')
            ui.button('Clear cache', on_click=lambda: (clear_cache(), ui.notify('Cache cleared'))).props('outline')
            ui.button('Show detected worksheet titles', on_click=lambda: ui.notify(', '.join(_list_titles(get_ss())) or 'No access', timeout=8)).props('outline').classes('q-ml-sm')

    top_shell('Admin', content)


# ============================
# Run
# ============================

if __name__ in { '__main__', '__mp_main__' }:
    ui.run(title=APP_TITLE, reload=False, host='0.0.0.0', port=int(os.getenv('PORT', '8080')))
