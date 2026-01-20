# ======================================
# MyFin App – Phase 4.6A
# Scope: Premium UI foundation & UX fixes
# Base: VF2_P4_5 (Last Stable)
# ======================================

# ==============================
# MyFin App – Phase 4.5 (P4.4 + P4.5 combined)
# Base: Myfin_NICEGUI_VF2_P4_2 (last stable)
# Changes: Budgets setup UX, Transactions table mobile UX, Rules edit, Cards utilization bars,
#          Dashboard pay-period view, Premium login styling
# ==============================

"""
MyFin — NiceGUI Stable
File: Myfin_NICEGUI_VF2_P4_2.py

Purpose
- A stable NiceGUI implementation that you can deploy on Render and use instead of Streamlit.
- Focus on correctness + usability + a consistent dark “banking style” UI.

Key behavior changes (requested)
1) Recurring:
   - Marking an entry as recurring creates/updates a TEMPLATE in the "recurring" sheet.
   - The app auto-creates the actual transaction ONLY when the due date arrives (and only once per month).
   - No backfilling past months. No creating future months in advance.

2) Pay cycles
   - Kept as "family" (no owner split). Any pay-cycle specific dashboards are deferred to later phases.

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
- python Myfin_NICEGUI_VF2_P3_12_3.py
"""

from __future__ import annotations

import uuid
import datetime
import logging

# Lightweight logger used across the app
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger("myfin")

def log(message: str) -> None:
    """Log a message to stdout and the configured logger."""
    try:
        _logger.info(message)
    except Exception:
        print(message)

# Simple in-memory cache for worksheet->DataFrame
_df_cache: dict[tuple[str, str], object] = {}


import os
import json
import re
import math
import time
import calendar
import hashlib
import base64
import asyncio
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, cast

import pandas as pd
import plotly.express as px

import gspread

# NOTE: Receipt scanning uses free, client-side OCR (tesseract.js) loaded from a CDN.
# No paid APIs are used.


# -------------------- Google Sheets retry helpers --------------------
from gspread.exceptions import APIError as GSpreadAPIError

def gs_retry(fn, *, retries: int = 6, base_sleep: float = 0.8):
    """Retry wrapper for Google Sheets API calls.

    Handles transient 429/5xx errors by backing off. Raises the last exception on failure.
    """
    import time
    import random

    last = None
    for i in range(retries):
        try:
            return fn()
        except GSpreadAPIError as e:
            last = e
            msg = str(e)
            # common transient cases: 429 quota, 500/502/503/504
            transient = any(code in msg for code in ['429', '500', '502', '503', '504'])
            if not transient or i == retries - 1:
                raise
            sleep_s = base_sleep * (2 ** i) + random.uniform(0, 0.35)
            time.sleep(sleep_s)

    if last:
        raise last

from gspread.exceptions import APIError
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
            ui.open(path)  # type: ignore[attr-defined]
            return
    except Exception:
        pass
    # Last resort: browser redirect
    ui.run_javascript(f"window.location.href='{path}'")


# Config
# -----------------------------
TZ = os.environ.get("TIMEZONE", "America/Winnipeg")

# Spreadsheet identification
# Prefer an ID (the long id in the Google Sheets URL). If not available, fall back to a spreadsheet name.
SPREADSHEET_ID = (
    os.environ.get('SPREADSHEET_ID')
    or os.environ.get('GOOGLE_SHEET_ID')
    or os.environ.get('GOOGLE_SHEETID')
)
SPREADSHEET_NAME = (
    os.environ.get('SPREADSHEET_NAME')
    or os.environ.get('GOOGLE_SHEET_NAME')
    or 'nishanthfintrack_2026'
)

# When worksheets are missing, the app currently creates them. This can hide an ID/name mismatch by creating
# new empty tabs. Setting this to "0" will enforce that existing sheets must be present.
ALLOW_CREATE_MISSING_SHEETS = os.environ.get('ALLOW_CREATE_MISSING_SHEETS', '1').strip() not in {'0', 'false', 'False'}
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
# Data cache (prevents repeated Google Sheets reads)
# -----------------------------
# Default to 5 minutes to avoid Google Sheets "Read requests per minute" quota issues.
CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", "300"))  # seconds

# Safety switch: when a sheet/tab name mismatch happens, auto-creating blank worksheets makes the app
# look like it "has no data" while actually reading a new empty tab. Default is OFF so we fail loudly.
ALLOW_CREATE_MISSING_SHEETS = os.environ.get('ALLOW_CREATE_MISSING_SHEETS', '0').strip().lower() in {'1', 'true', 'yes', 'y'}
_cache: Dict[str, Tuple[float, pd.DataFrame]] = {}

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

    # Google Sheets can sometimes deliver dates as serial numbers (e.g., 45567)
    # depending on formatting. Convert these to real dates.
    if s.isdigit() and len(s) >= 5:
        try:
            serial = int(s)
            # Excel/Sheets serial date origin (1899-12-30) works for modern dates.
            origin = dt.date(1899, 12, 30)
            return origin + dt.timedelta(days=serial)
        except Exception:
            pass
    # Fast path for common explicit formats
    for fmt in (
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%d-%b-%Y",     # 16-Jan-2026
        "%d %b %Y",     # 16 Jan 2026
        "%b %d, %Y",    # Jan 16, 2026
        "%d %B %Y",     # 16 January 2026
        "%B %d, %Y",    # January 16, 2026
    ):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass

    # Google Sheets sometimes returns date-like serial numbers
    # (days since 1899-12-30). If the value looks numeric, try that.
    try:
        if re.fullmatch(r"\d+(?:\.\d+)?", s):
            n = float(s)
            if 20000 <= n <= 60000:  # reasonable range for modern dates
                base = dt.date(1899, 12, 30)
                return base + dt.timedelta(days=int(n))
    except Exception:
        pass

    # Robust fallback: try pandas with both day-first and month-first
    try:
        d = pd.to_datetime(s, errors='coerce', dayfirst=False)
        if pd.notna(d):
            return d.date()
    except Exception:
        pass
    try:
        d = pd.to_datetime(s, errors='coerce', dayfirst=True)
        if pd.notna(d):
            return d.date()
    except Exception:
        pass
    return None




def now_iso() -> str:
    """Return current UTC timestamp as ISO-8601 string with timezone."""
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')
def parse_money(value: object, default: float = 0.0) -> float:
    """Parse money-ish values like '$25,000', '25000', 25000 into float."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return default
    s = str(value).strip()
    if not s or s.lower() in ('nan', 'none'):
        return default
    # keep digits, minus, dot
    s = s.replace(',', '')
    if s.startswith('$'):
        s = s[1:].strip()
    # remove any remaining currency symbols/spaces
    s = ''.join(ch for ch in s if (ch.isdigit() or ch in '.-'))
    if not s or s in ('-', '.', '-.'):
        return default
    try:
        return float(s)
    except Exception:
        return default


def _guess_merchant_from_text(text: str) -> str:
    """Best-effort merchant extraction from OCR text."""
    t = text.upper()
    # prefer known merchants if present
    known = [
        "WALMART",
        "DOLLARAMA",
        "COSTCO",
        "SUPERSTORE",
        "LOBLAWS",
        "NO FRILLS",
        "FRESHCO",
        "CANADIAN TIRE",
        "TIM HORTONS",
        "MCDONALD",
        "STARBUCKS",
        "GILL",
    ]
    for k in known:
        if k in t:
            # keep original casing style
            return k.title() if k != "WALMART" else "Walmart"

    # fallback: first non-empty line that isn't obviously an address/phone/terminal
    bad = ("WINNIPEG", "MB", "MANITOBA", "CANADA", "TEL", "PHONE", "STORE", "POS", "TERMINAL")
    for line in [ln.strip() for ln in text.splitlines() if ln.strip()]:
        up = line.upper()
        if any(b in up for b in bad):
            continue
        # skip if mostly digits
        digits = sum(ch.isdigit() for ch in line)
        if len(line) > 0 and digits / max(1, len(line)) > 0.5:
            continue
        if 2 <= len(line) <= 40:
            return line.strip().title()
    return ""


def _extract_date_from_text(text: str) -> Optional[dt.date]:
    """Extract a receipt date from OCR text using candidate scoring.

    Supported formats (most common in your receipts):
      - MM/DD/YYYY (e.g., Gill’s 12/8/2025)
      - MM/DD/YY   (e.g., Walmart 1/17/26)
      - YY/MM/DD   (e.g., Dollarama 26/01/17 -> 2026-01-17)
      - YYYY/MM/DD, YYYY-MM-DD

    We avoid false positives from terminal/store IDs by:
      - strict month/day/year validation
      - scoring candidates with DATE keywords / time proximity / recency
      - rejecting ID-heavy lines unless they include date keywords

    Returns a dt.date or None (safe: we prefer empty over wrong).
    """
    if not text:
        return None

    lines = [ln.strip() for ln in str(text).splitlines() if ln.strip()]
    if not lines:
        return None

    ignore_tokens = (
        'st#', 'store', 'term', 'tran', 'ref', 'seq', 'tc#',
        'lane', 'op', 'auth', 'invoice', 'order', 'reg', 'terminal', 'cashier', 'till'
    )
    date_keywords = ('date', 'dte', 'trans', 'transaction', 'purchase', 'time', 'issued')

    def norm_year(y: int) -> int:
        return 2000 + y if 0 <= y <= 99 else y

    def valid_date(yy: int, mm: int, dd: int) -> Optional[dt.date]:
        yy = norm_year(yy)
        if not (2020 <= yy <= 2032):
            return None
        if not (1 <= mm <= 12 and 1 <= dd <= 31):
            return None
        try:
            return dt.date(yy, mm, dd)
        except Exception:
            return None

    patterns = [
        ('YMD4', re.compile(r'(?<!\d)(\d{4})[\-/](\d{1,2})[\-/](\d{1,2})(?!\d)')),
        ('MDY4', re.compile(r'(?<!\d)(\d{1,2})[\-/](\d{1,2})[\-/](\d{4})(?!\d)')),
        ('MDY2', re.compile(r'(?<!\d)(\d{1,2})[\-/](\d{1,2})[\-/](\d{2})(?!\d)')),
        ('YMD2', re.compile(r'(?<!\d)(\d{2})[\-/](\d{1,2})[\-/](\d{1,2})(?!\d)')),
    ]

    today_d = today()
    candidates: list[tuple[float, dt.date]] = []

    for i, ln in enumerate(lines):
        low = ln.lower()
        has_kw = any(k in low for k in date_keywords)
        has_time = bool(re.search(r'(?<!\d)\d{1,2}:\d{2}(?::\d{2})?(?!\d)', ln))

        # Skip ID-heavy lines unless they contain date keywords
        if any(tok in low for tok in ignore_tokens) and not has_kw:
            continue

        digit_ratio = sum(ch.isdigit() for ch in ln) / max(1, len(ln))

        for kind, rx in patterns:
            for m in rx.finditer(ln):
                try:
                    if kind == 'YMD4':
                        yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    elif kind == 'MDY4':
                        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    elif kind == 'MDY2':
                        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
                        yy = norm_year(yy)
                    else:  # YMD2 (Dollarama-style)
                        yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
                        yy = norm_year(yy)

                    d = valid_date(yy, mm, dd)
                    if not d:
                        continue

                    score = 0.0
                    if has_kw:
                        score += 6.0
                    if has_time:
                        score += 3.5

                    delta = abs((today_d - d).days)
                    if delta <= 7:
                        score += 5.0
                    elif delta <= 31:
                        score += 3.0
                    elif delta <= 120:
                        score += 1.0
                    else:
                        score -= 2.0

                    # Walmart often prints date near bottom; some merchants at top
                    if i <= 4:
                        score += 1.0
                    if i >= len(lines) - 5:
                        score += 1.0

                    # Penalize ID-heavy lines
                    if digit_ratio > 0.55 and not has_kw:
                        score -= 2.0

                    # Penalize if lots of digits remain aside from the matched date
                    if re.search(r'\d{6,}', rx.sub('', ln)):
                        score -= 2.0

                    candidates.append((score, d))
                except Exception:
                    continue

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_date = candidates[0]

    # Safety: don't guess if confidence too low
    if best_score < 2.0:
        return None

    return best_date

def _extract_total_amount(text: str) -> Tuple[Optional[float], float, str]:
    """Try to find the final total.

    Returns (amount, confidence, source).

    Confidence is a heuristic score in [0..10]. Source is the keyword bucket used.
    """
    lines = [ln.strip() for ln in (text or '').splitlines() if ln.strip()]

    # Prefer explicit totals; avoid SUBTOTAL/TAX unless nothing else exists.
    # Scores tuned for receipts; higher is more trustworthy.
    key_scores = [
        ('GRAND TOTAL', 10.0),
        ('AMOUNT DUE', 9.5),
        ('BALANCE DUE', 9.5),
        ('TOTAL', 8.5),
        ('BALANCE', 7.5),
        ('PAYMENT', 6.0),
        # SUBTOTAL is only used if no better match exists
        ('SUBTOTAL', 2.0),
    ]

    negative_markers = (
        'SUBTOTAL', 'TAX', 'GST', 'PST', 'HST', 'TIP', 'CHANGE', 'CASH', 'REFUND', 'RETURN',
        'DISCOUNT', 'SAVINGS', 'COUPON', 'POINTS', 'DEPOSIT',
    )

    # Currency-like values (prefer cents): 12.34, $12.34, 1,234.56
    money_pat = re.compile(r"(?:(?:CAD|USD)\s*)?(\$?\s*-?\d{1,7}(?:[\,\s]\d{3})*(?:\.\d{2})?)")

    candidates: list[tuple[float, float, int, str]] = []  # (amount, score, line_idx, source)

    for i, ln in enumerate(lines):
        up = ln.upper()
        vals = money_pat.findall(ln)
        if not vals:
            continue

        # Base score: if line has decimals and looks like a total line
        base = 0.5
        if re.search(r"\.\d{2}\b", ln):
            base += 0.8

        # Keyword-based scoring
        source = ''
        kscore = 0.0
        for key, sc in key_scores:
            if key in up:
                # Special case: avoid catching TOTAL in SUBTOTAL (handled by explicit SUBTOTAL entry)
                if key == 'TOTAL' and 'SUBTOTAL' in up:
                    continue
                kscore = max(kscore, sc)
                source = key

        # Penalize lines that are clearly not the final payable amount
        penalty = 0.0
        if any(m in up for m in negative_markers):
            # If we matched SUBTOTAL explicitly, don't over-penalize
            if source != 'SUBTOTAL':
                penalty += 3.0

        # Take the last amount on the line (receipts often show: TOTAL 12.34)
        v = parse_money(vals[-1], default=float('nan'))
        if math.isnan(v) or v == 0:
            continue
        v = abs(v)

        score = max(base + kscore - penalty, 0.0)
        candidates.append((v, score, i, source or 'LINE'))

    if candidates:
        # pick best score; tie-break by later line (totals tend to appear near the bottom)
        candidates.sort(key=lambda t: (t[1], t[2]), reverse=True)
        amt, score, _, source = candidates[0]
        # Clamp score to 0..10 for display
        score = max(0.0, min(10.0, score))
        return amt, score, source

    # Fallback: pick the largest plausible currency-like value anywhere
    best: Optional[float] = None
    for ln in lines:
        for s in money_pat.findall(ln):
            v = parse_money(s, default=float('nan'))
            if math.isnan(v):
                continue
            v = abs(v)
            if v <= 0:
                continue
            if best is None or v > best:
                best = v

    return best, (1.0 if best is not None else 0.0), 'MAX'


def _extract_card_last4(text: str) -> str:
    """Try to find last-4 digits of card, if printed."""
    # common formats: **** 1234, XXXX1234, x1234
    patterns = [
        r"\*{2,}\s*(\d{4})",
        r"X{2,}\s*(\d{4})",
        r"(?:VISA|MASTERCARD|MASTER CARD|MC|DEBIT)\D{0,15}(\d{4})",
    ]
    for pat in patterns:
        m = re.search(pat, text.upper())
        if m:
            return m.group(1)
    return ""


def pick_account_from_last4(cards_df: pd.DataFrame, last4: str) -> str:
    """Return the matching card_name/account for a detected card last-4.

    - Expects a column named 'card_last4' (recommended).
      Also accepts common variants like 'last4', 'last_4'.
    - Returns '' if no unique match.
    - Safe with empty/missing columns.
    """
    try:
        if cards_df is None or cards_df.empty:
            return ''
        digits = re.sub(r'[^0-9]', '', str(last4 or ''))
        if len(digits) != 4:
            return ''

        # locate last-4 column
        col_map = {str(c).strip().lower(): c for c in cards_df.columns}
        last4_col = None
        for k in ('card_last4', 'last4', 'last_4', 'cardlast4', 'ending4', 'ending_4'):
            if k in col_map:
                last4_col = col_map[k]
                break
        if not last4_col:
            return ''

        # locate card/account name column
        name_col = None
        for k in ('card_name', 'account', 'name'):
            if k in col_map:
                name_col = col_map[k]
                break
        if not name_col:
            return ''

        # match
        def norm(v: object) -> str:
            d = re.sub(r'[^0-9]', '', str(v or ''))
            return d[-4:] if len(d) >= 4 else d

        matches = cards_df[cards_df[last4_col].apply(norm) == digits]
        if matches.empty:
            return ''
        # if multiple, pick first non-empty name
        for v in matches[name_col].astype(str).tolist():
            v = v.strip()
            if v and v.lower() != 'nan':
                return v
        return ''
    except Exception:
        return ''


def parse_receipt_text(text: str) -> Dict[str, Any]:
    """Return best-effort parsed fields from OCR text.

    Fields:
      - merchant: str
      - date: datetime.date|None
      - amount: float|None
      - amount_confidence: float (0..10)
      - amount_source: str
      - card_last4: str
      - raw: str
    """
    cleaned = text or ""
    amount, conf, source = _extract_total_amount(cleaned)
    return {
        "merchant": _guess_merchant_from_text(cleaned),
        "date": _extract_date_from_text(cleaned),
        "amount": amount,
        "amount_confidence": conf,
        "amount_source": source,
        "card_last4": _extract_card_last4(cleaned),
        "raw": cleaned,
    }


def to_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, str):
            x = x.replace(",", "").replace("$", "").strip()
        return float(x)
    except Exception:
        return 0.0


def wide_transactions_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a 'wide' Transactions sheet into the app's long format.

    If the sheet already contains 'type' and 'amount' columns, this returns df unchanged.
    Otherwise it looks for common MyFin columns like:
    Date, International transaction, Credit, Investment, Credit card repay, Debit,
    LOC Withdrawal, LOC Repayment, Account, Reason/Notes.
    """
    cols_norm = {c.strip().lower(): c for c in df.columns}
    if 'type' in cols_norm and 'amount' in cols_norm:
        return df

    date_col = cols_norm.get('date')
    if not date_col:
        return df

    # helper to find first matching column
    # We support both:
    # 1) exact header matches (e.g., "credit")
    # 2) "contains" matches (e.g., "credit card repay (amount ...)" contains "credit card repay")
    def pick(*names: str) -> Optional[str]:
        # exact
        for n in names:
            if n in cols_norm:
                return cols_norm[n]
        # contains
        for n in names:
            for k_norm, orig in cols_norm.items():
                if n and (n in k_norm):
                    return orig
        return None

    notes_col = pick('reason/notes', 'reason', 'notes', 'note', 'description', 'remarks')
    account_col = pick('account', 'accounts')
    owner_col = pick('owner', 'person', 'who')

    # category amount columns
    mapping = [
        ('international', pick('international transaction', 'international', 'intl', 'remittance')),
        ('credit', pick('credit', 'income')),
        ('investment', pick('investment', 'invest')),
        ('cc_repay', pick('credit card repay', 'credit card repayment', 'creditcard repay', 'cc repay', 'cc repayment')),
        ('debit', pick('debit', 'expense', 'spend')),
        ('loc_withdrawal', pick('loc withdrawal', 'loc draw', 'line of credit withdrawal')),
        ('loc_repayment', pick('loc repayment', 'loc repay', 'line of credit repayment')),
    ]

    # build long rows
    out_rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        base = {
            'date': r.get(date_col),
            'notes': (str(r.get(notes_col)).strip() if notes_col and pd.notna(r.get(notes_col)) else ''),
            'account': (str(r.get(account_col)).strip() if account_col and pd.notna(r.get(account_col)) else ''),
            'owner': (str(r.get(owner_col)).strip() if owner_col and pd.notna(r.get(owner_col)) else 'Family'),
        }

        any_added = False
        for t, c in mapping:
            if not c:
                continue
            amt = to_float(r.get(c))
            if abs(amt) > 1e-9:
                row = dict(base)
                row.update({'type': t, 'amount': amt})
                out_rows.append(row)
                any_added = True

        # if no category columns found, keep row (helps surface schema issues)
        if not any_added:
            row = dict(base)
            row.update({'type': str(r.get(pick('type')) or '').strip(), 'amount': to_float(r.get(pick('amount')) or 0)})
            out_rows.append(row)

    out = pd.DataFrame(out_rows)
    # ensure expected columns exist
    for c in ['date', 'type', 'amount', 'account', 'notes', 'owner']:
        if c not in out.columns:
            out[c] = '' if c != 'amount' else 0.0
    return out

def normalize_title(s: str) -> str:
    # Normalize worksheet names for robust matching (ignore spaces/punctuation)
    return ''.join(ch for ch in (s or '').lower() if ch.isalnum())



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

# Quota protection / memoization for Sheets metadata
_tabs_ready: bool = False
_tabs_ready_at: float = 0.0
_header_cache: Dict[str, List[str]] = {}
_migrated_tx_ids: bool = False


def _col_to_letter(idx0: int) -> str:
    """0-based column index -> Google Sheets column letter."""
    n = idx0 + 1
    s = ''
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _migrate_transactions_id_column() -> None:
    """Backfill transactions.id from legacy TxId/txid column.

    Why: older sheets had a 'TxId' column; Phase 3 uses a canonical 'id' column.
    Edit/update looked up rows by 'id', so legacy rows (id blank) could not be edited.

    This runs once per process and only writes when it detects blanks.
    """
    global _migrated_tx_ids
    if _migrated_tx_ids:
        return
    _migrated_tx_ids = True

    try:
        w = ws('transactions')
        hdr = sheet_headers('transactions')
        if not hdr:
            return

        def _find_idx(names: List[str]) -> Optional[int]:
            lowered = [h.strip().lower() for h in hdr]
            for n in names:
                if n.lower() in lowered:
                    return lowered.index(n.lower())
            return None

        id_idx = _find_idx(['id'])
        legacy_idx = _find_idx(['txid', 'tx_id', 'TxId'])
        if id_idx is None or legacy_idx is None:
            return

        # Read whole columns (1 API call each). Values include header at row 1.
        id_col = w.col_values(id_idx + 1)
        legacy_col = w.col_values(legacy_idx + 1)
        n_rows = max(len(id_col), len(legacy_col))
        if n_rows <= 1:
            return

        # Normalize lengths
        id_col += [''] * (n_rows - len(id_col))
        legacy_col += [''] * (n_rows - len(legacy_col))

        new_vals: List[List[str]] = []
        changed = False
        for r in range(2, n_rows + 1):
            cur = (id_col[r - 1] or '').strip()
            leg = (legacy_col[r - 1] or '').strip()
            # If both are empty, generate a stable id so edit/delete works for legacy rows.
            val = cur or leg or uuid.uuid4().hex
            if val != cur:
                changed = True
            new_vals.append([val])

        if not changed:
            return

        col_letter = _col_to_letter(id_idx)
        w.update(f'{col_letter}2:{col_letter}{n_rows}', new_vals)
        _header_cache.pop('transactions', None)
        _df_cache.pop('transactions', None)
    except Exception as e:
        # Never break the app because of a migration attempt.
        print(f'[migrate_tx_ids] skipped due to: {e}')

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
    gc = get_client()
    try:
        if SPREADSHEET_ID:
            _ss = gc.open_by_key(SPREADSHEET_ID)
        else:
            _ss = gc.open(SPREADSHEET_NAME)
    except Exception as e:
        # Surface spreadsheet open issues clearly in Render logs.
        print(f"[MyFin] Failed to open spreadsheet. id={bool(SPREADSHEET_ID)} name={SPREADSHEET_NAME!r}: {e}")
        raise

    # Helpful diagnostics in logs so we can confirm the app is reading the correct file.
    try:
        titles = [w.title for w in _ss.worksheets()]
        print(f"[MyFin] Opened spreadsheet: '{_ss.title}' | worksheets={titles}")
    except Exception:
        pass
    return _ss

def ensure_tabs() -> None:
    """Ensure required worksheets exist and map them case-insensitively.

    Important: Many users already have sheets like "Transactions" with capital letters.
    Earlier versions created new empty "transactions" tabs, which made the app look blank.
    This function always reuses existing tabs (case-insensitive) when present.
    """

    # Avoid hammering the Sheets API: only (re)build the worksheet map occasionally.
    global _ws, _tabs_ready, _tabs_ready_at
    now = time.time()
    if _tabs_ready and _ws and (now - _tabs_ready_at) < 300:
        return

    ss = get_spreadsheet()
    existing = {normalize_title(w.title): w for w in ss.worksheets()}

    _ws = {}
    missing_tabs: list[str] = []

    for tab, headers in TABS.items():
        key = normalize_title(tab)
        w = existing.get(key)

        if w is None:
            missing_tabs.append(tab)
            if not ALLOW_CREATE_MISSING_SHEETS:
                continue
            # create new sheet (one-time)
            w = ss.add_worksheet(title=tab, rows=2000, cols=max(12, len(headers) + 2))
            w.append_row(headers)
            _header_cache[tab] = headers
        else:
            # Do NOT call get_all_values() here (very expensive in quota terms).
            # Only read the header row once and cache it.
            try:
                cur = [c.strip() for c in (w.row_values(1) or [])]
            except Exception:
                cur = []
            if not cur:
                try:
                    w.append_row(headers)
                except Exception:
                    pass
                _header_cache[tab] = headers
            else:
                _header_cache[tab] = cur

        if w is not None:
            _ws[tab] = w

    if missing_tabs and not ALLOW_CREATE_MISSING_SHEETS:
        existing_titles = [w.title for w in ss.worksheets()]
        raise RuntimeError(
            "Missing required Google Sheets tabs: "
            + ", ".join(missing_tabs)
            + ". Existing tabs: "
            + ", ".join(existing_titles)
            + ".\nFix: rename your sheets to match (Transactions, Rules, Cards, Recurring) "
            + "or set ALLOW_CREATE_MISSING_SHEETS=1 to let the app create them."
        )

    _tabs_ready = True
    _tabs_ready_at = now


def ws(tab: str) -> gspread.Worksheet:
    ensure_tabs()
    return _ws[tab]


def sheet_headers(tab: str) -> list[str]:
    # Prefer cached header row to avoid repeated reads.
    ensure_tabs()
    if tab in _header_cache and _header_cache[tab]:
        return [c.strip() for c in _header_cache[tab]]

    w = ws(tab)
    try:
        headers = [c.strip() for c in (w.row_values(1) or [])]
    except Exception:
        headers = []

    if not headers:
        headers = TABS[tab]
        try:
            w.append_row(headers)
        except Exception:
            pass
    _header_cache[tab] = headers
    return headers


def read_df_optional(sheet_title: str) -> pd.DataFrame:
    """Read an arbitrary worksheet by title if it exists; otherwise return empty DF.

    Used for legacy/admin sources (e.g., rules_text) without forcing new required tabs.
    Matching is case-insensitive and tolerant of spaces/underscores.
    """
    def _norm(s: str) -> str:
        return re.sub(r"\s+", "", str(s).strip().lower().replace('_', ' '))

    try:
        ss = get_spreadsheet()
        want = _norm(sheet_title)
        target = None
        for w in ss.worksheets():
            if _norm(w.title) == want:
                target = w
                break
        if target is None:
            return pd.DataFrame()

        values = target.get_all_values()
        if not values or len(values) < 2:
            return pd.DataFrame()

        headers = [str(h).strip() for h in values[0]]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=headers)
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()


def read_df(tab: str) -> pd.DataFrame:
    """Read a worksheet into a DataFrame (all values as strings initially).

    This is intentionally tolerant of extra columns or different header casing/order.
    """
    w = ws(tab)
    # Sheets quota is based on request count. NiceGUI can cause short bursts of reads
    # (initial load + websocket reconnects). Retry a few times on HTTP 429.
    values: List[List[str]]
    last_err: Optional[Exception] = None
    for delay in (0.0, 1.0, 2.0, 4.0):
        if delay:
            time.sleep(delay)
        try:
            values = w.get_all_values()
            break
        except APIError as e:
            last_err = e
            if '429' not in str(e):
                raise
    else:
        # exhausted retries
        raise cast(Exception, last_err)
    if not values or len(values) == 0:
        return pd.DataFrame(columns=sheet_headers(tab))

    # Some sheets have a few header / notes rows before the actual header row. We try to detect the
    # best header row within the first 10 rows by matching expected headers for this tab.
    expected = {normalize_title(h) for h in sheet_headers(tab) if h.strip()}
    header_row_idx = 0
    best_score = -1
    for i in range(min(10, len(values))):
        row = values[i]
        score = sum(1 for c in row if normalize_title(c) in expected)
        if score > best_score:
            best_score = score
            header_row_idx = i
    headers = [c.strip() for c in (values[header_row_idx] if values else [])]
    rows = values[header_row_idx + 1 :]

    if not headers or len(rows) == 0:
        return pd.DataFrame(columns=sheet_headers(tab))

    width = len(headers)
    norm_rows = [r + [''] * (width - len(r)) if len(r) < width else r[:width] for r in rows]
    df = pd.DataFrame(norm_rows, columns=headers)

    # Coalesce duplicate header names (common during sheet migrations).
    # Example: both "Emoji" and "emoji" can exist; then df['emoji'] returns a DataFrame,
    # and card rendering may appear empty.
    if df.columns.duplicated().any():
        new_cols: list[str] = []
        seen: set[str] = set()
        for col in df.columns:
            if col not in seen:
                new_cols.append(col)
                seen.add(col)
        fixed = pd.DataFrame(index=df.index)
        for col in new_cols:
            if (df.columns == col).sum() == 1:
                fixed[col] = df[col]
            else:
                # take the first, then fill with later duplicates if blank
                dup = df.loc[:, df.columns == col]
                s = dup.iloc[:, 0].copy()
                for i in range(1, dup.shape[1]):
                    s = s.where((s.astype(str).str.strip() != '') & s.notna(), dup.iloc[:, i])
                fixed[col] = s
        df = fixed
    return df


def append_row(tab: str, row: dict[str, Any]) -> None:
    w = ws(tab)
    headers = sheet_headers(tab)
    # allow case-insensitive matching of provided keys
    lower_map = {h.lower(): h for h in headers}
    values = [''] * len(headers)
    for k, v in row.items():
        hk = lower_map.get(str(k).lower())
        if hk is None:
            continue
        i = headers.index(hk)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            values[i] = str(v)
        elif isinstance(v, dt.date):
            values[i] = v.isoformat()
        else:
            values[i] = '' if v is None else str(v)
    w.append_row(values, value_input_option='USER_ENTERED')



def append_tx(tx: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
    """Append a transaction to the `transactions` worksheet.

    This function accepts either:
      1) a fully-formed transaction dict via `tx`, OR
      2) keyword args (used by the Add page), e.g. tx_id=..., date=..., owner=..., etc.

    Supported id keys: `tx_id` (legacy) and `id` (current).
    """

    # Build a transaction dict if called with kwargs (legacy call-sites used `tx_id=`).
    if tx is None:
        tx = dict(kwargs)
    else:
        # merge kwargs over tx (kwargs wins)
        tx = {**tx, **kwargs}

    # Normalize primary key
    if 'id' not in tx or not str(tx.get('id', '')).strip():
        if 'tx_id' in tx and str(tx.get('tx_id', '')).strip():
            tx['id'] = tx['tx_id']

    # Normalize common field names (some call-sites use `type_` / `date_`)
    if 'date' not in tx and 'date_' in tx:
        tx['date'] = tx.get('date_')
    if 'type' not in tx and 'type_' in tx:
        tx['type'] = tx.get('type_')

    # Ensure required columns exist even if blank
    tx.setdefault('owner', '')
    tx.setdefault('type', '')
    tx.setdefault('amount', '')
    tx.setdefault('method', '')
    tx.setdefault('account', '')
    tx.setdefault('category', '')
    tx.setdefault('notes', '')
    tx.setdefault('is_recurring', False)
    tx.setdefault('recurring_id', '')
    tx.setdefault('created_at', tx.get('created_at', now_iso()))

    # `append_row` expects a dict; it writes values in the sheet's header order.
    append_row('transactions', tx)

def find_row_index_by_id(tab: str, id_col: str, id_val: str) -> tuple[int, list[str]] | tuple[None, list[str]]:
    w = ws(tab)
    values = w.get_all_values()
    if not values:
        return None, []
    headers = [c.strip() for c in values[0]]
    # locate id column case-insensitively
    col_idx = None
    for i, h in enumerate(headers):
        if h.strip().lower() == id_col.strip().lower():
            col_idx = i
            break
    if col_idx is None:
        return None, headers
    for r_i, row in enumerate(values[1:], start=2):
        if len(row) > col_idx and str(row[col_idx]).strip() == str(id_val).strip():
            return r_i, headers
    return None, headers


def update_row_by_id(tab: str, id_col: str, id_val: str, updates: dict[str, Any]) -> bool:
    w = ws(tab)
    row_idx, headers = find_row_index_by_id(tab, id_col, id_val)
    if row_idx is None and tab == 'transactions' and str(id_col).lower() == 'id':
        # Backward compatibility: if the sheet still uses legacy id columns.
        for alt in ('txid', 'TxId', 'TXID'):
            row_idx, headers = find_row_index_by_id(tab, alt, id_val)
            if row_idx is not None:
                break
    if row_idx is None:
        return False
    lower_map = {h.lower(): (i, h) for i, h in enumerate(headers)}
    # build updates per cell
    for k, v in updates.items():
        key = str(k).lower()
        if key not in lower_map:
            continue
        col_i, _ = lower_map[key]
        # A1 notation: row_idx, col_i+1
        cell = gspread.utils.rowcol_to_a1(row_idx, col_i + 1)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            vv = str(v)
        elif isinstance(v, dt.date):
            vv = v.isoformat()
        else:
            vv = '' if v is None else str(v)
        w.update_acell(cell, vv)
    return True


def delete_row_by_id(tab: str, id_col: str, id_val: str) -> bool:
    w = ws(tab)
    row_idx, _ = find_row_index_by_id(tab, id_col, id_val)
    if row_idx is None and tab == 'transactions' and str(id_col).lower() == 'id':
        for alt in ('txid', 'TxId', 'TXID'):
            row_idx, _ = find_row_index_by_id(tab, alt, id_val)
            if row_idx is not None:
                break
    if row_idx is None:
        return False
    gs_retry(lambda: w.delete_rows(row_idx))
    return True


def cached_df(tab: str, force: bool = False) -> pd.DataFrame:
    """Return a cached copy of a tab DataFrame (read from Google Sheets).

    On any Sheets error, we log to stdout and return an empty DataFrame with expected headers.
    """
    now = time.time()
    if (not force) and tab in _cache and (now - _cache[tab][0] < CACHE_TTL):
        return _cache[tab][1].copy()

    # Lightweight retry for transient Google Sheets quota bursts.
    def _is_quota_error(exc: Exception) -> bool:
        s = str(exc)
        return ('429' in s) or ('Quota exceeded' in s) or ('Read requests' in s)

    try:
        last_exc: Optional[Exception] = None
        df: Optional[pd.DataFrame] = None

        for attempt in range(3):
            try:
                df = read_df(tab)
                break
            except Exception as e:
                last_exc = e
                if _is_quota_error(e) and attempt < 2:
                    time.sleep(1.0 * (2 ** attempt))  # 1s, 2s
                    continue
                break

        if df is None:
            raise last_exc or RuntimeError('Unknown Sheets read error')

        if tab == 'transactions':
            # Support the legacy "wide" sheet layout (category columns)
            # by converting it into the app's long ledger format.
            before_cols = list(df.columns)
            df = wide_transactions_to_long(df)
            print(f"[MyFin] transactions loaded: rows={len(df)} cols={list(df.columns)} (source cols={before_cols})")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print('GOOGLE_SHEETS_READ_ERROR', tab, str(e))
        print(tb)
        # If we already have cached data, keep serving it (stale-but-usable)
        if tab in _cache:
            try:
                ui.notify(f'Google Sheets temporarily unavailable for {tab}. Showing cached data.', type='warning')
            except Exception:
                pass
            return _cache[tab][1].copy()
        try:
            ui.notify(f'Google Sheets read failed for {tab}: {e}', type='negative')
        except Exception:
            pass
        df = pd.DataFrame(columns=TABS.get(tab, []))

    _cache[tab] = (now, df.copy())
    return df

def invalidate(*tabs: str) -> None:

    for t in tabs:
        _cache.pop(t, None)


# -----------------------------
# Rules + Category inference
# -----------------------------
def load_rules(force: bool = False) -> List[Tuple[str, str]]:
    """Load category rules from BOTH:

    1) Primary **rules** sheet (keyword/category)
    2) Legacy/admin **rules_text** sheet (Key/Category) if it exists

    Rules sheet has priority; rules_text acts as fallback.
    Missing/empty rules_text never errors.
    """

    # --- primary Rules sheet ---
    df = cached_df('rules', force=force)

    primary: list[tuple[str, str]] = []
    if not df.empty:
        cols = list(df.columns)
        lmap = {str(c).strip().lower(): c for c in cols}

        keyword_col = None
        for k in ['keyword', 'keywords', 'key', 'keys', 'rule', 'match', 'pattern']:
            if k in lmap:
                keyword_col = lmap[k]
                break

        category_col = None
        for k in ['category', 'cat', 'label', 'bucket', 'type']:
            if k in lmap:
                category_col = lmap[k]
                break

        if keyword_col is None or category_col is None:
            log(f"Rules sheet missing expected columns. Found: {cols}")
        else:
            for _, r in df.iterrows():
                raw_kw = str(r.get(keyword_col, '')).strip()
                cat = str(r.get(category_col, '')).strip()
                if not raw_kw or not cat or raw_kw.lower() == 'nan' or cat.lower() == 'nan':
                    continue

                parts = [p.strip() for p in re.split(r"[;,]", raw_kw) if p.strip()]
                for p in parts:
                    primary.append((p.lower(), cat))

    # --- legacy/admin rules_text sheet ---
    admin: list[tuple[str, str]] = []
    adf = read_df_optional('rules_text')
    if adf is not None and not adf.empty:
        cols = list(adf.columns)
        lmap = {str(c).strip().lower(): c for c in cols}

        key_col = None
        for k in ['key', 'keyword', 'keywords', 'rules', 'rule']:
            if k in lmap:
                key_col = lmap[k]
                break

        cat_col = None
        for k in ['category', 'cat', 'label', 'bucket', 'type']:
            if k in lmap:
                cat_col = lmap[k]
                break

        if key_col and cat_col:
            for _, r in adf.iterrows():
                raw_kw = str(r.get(key_col, '')).strip()
                cat = str(r.get(cat_col, '')).strip()
                if not raw_kw or not cat or raw_kw.lower() == 'nan' or cat.lower() == 'nan':
                    continue
                parts = [p.strip() for p in re.split(r"[;,]", raw_kw) if p.strip()]
                for p in parts:
                    admin.append((p.lower(), cat))

    return primary + admin



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
    rdf = cached_df("recurring")
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
    rdf = cached_df("recurring")
    if rdf.empty:
        return 0

    tx = cached_df("transactions")
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
    u = os.environ.get('APP_USER') or os.environ.get('APP_USERNAME') or 'admin'
    p = os.environ.get('APP_PASS') or os.environ.get('APP_PASSWORD') or 'admin'
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

.q-field__native, .q-field__input, .q-field__label, .q-field__prefix, .q-field__suffix, .q-select__dropdown-icon {
  color: var(--mf-text) !important;
}
.q-field__control-container {
  color: var(--mf-text) !important;
}
.q-field__append, .q-field__prepend {
  color: var(--mf-muted) !important;
}

.q-field__bottom, .q-field__hint, .q-field__messages, .text-grey, .text-grey-7 {
  color: var(--mf-muted) !important;
}

/* Force dark surfaces for Quasar components that default to white */
.q-menu, .q-dialog__inner > div, .q-card {
  background: rgba(12, 18, 32, 0.96) !important;
  color: var(--mf-text) !important;
}
.q-table__container {
  background: rgba(255,255,255,0.04) !important;
  border: 1px solid var(--mf-border) !important;
  border-radius: 14px !important;
}
.q-table__top, .q-table__bottom {
  background: transparent !important;
  color: var(--mf-text) !important;
}
.q-table thead tr th {
  color: var(--mf-text) !important;
  background: rgba(255,255,255,0.06) !important;
}
.q-table tbody td {
  color: var(--mf-text) !important;
}
.q-table tbody tr:nth-child(odd) {
  background: rgba(255,255,255,0.02) !important;
}
.q-table tbody tr:hover {
  background: rgba(46,125,255,0.10) !important;
}
.q-btn {
  text-transform: none !important;
}

.mf-top-menu { display: none; }
@media (max-width: 899px) {
  .mf-top-menu { display: inline-flex; }
}

.mf-bottom-nav {
  position: fixed;
  bottom: 10px;
  left: 10px;
  right: 10px;
  z-index: 1000;
}
@media (min-width: 900px) {
  .mf-bottom-nav { display: none; }
}

.tile {
  cursor: pointer;
  transition: transform .12s ease, background .12s ease;
}
.tile:hover { transform: translateY(-2px); background: rgba(255,255,255,0.07) !important; }

/* Glassy dialogs (used for Add sub-flows like receipt scan) */
.q-dialog__backdrop {
  backdrop-filter: blur(10px) !important;
  background: rgba(0,0,0,0.55) !important;
}
.q-dialog__inner > div {
  background: rgba(16, 23, 40, 0.70) !important;
  border: 1px solid rgba(255,255,255,0.14) !important;
  box-shadow: 0 24px 70px rgba(0,0,0,0.55) !important;
}
.q-dialog__inner > div .q-card {
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
}

/* Nicer KPI blocks */
.kpi {
  background: linear-gradient(180deg, rgba(255,255,255,0.08), rgba(255,255,255,0.03)) !important;
  border: 1px solid rgba(255,255,255,0.12) !important;
}
.kpi .kpi-value { letter-spacing: 0.2px; }

/* Budget progress bar */
.mf-progress {
  height: 10px;
  border-radius: 999px;
  background: rgba(255,255,255,0.10);
  overflow: hidden;
}
.mf-progress > div {
  height: 100%;
  background: rgba(46,125,255,0.85);
  border-radius: 999px;
}
"""
ui.add_head_html(f"<style>{BANK_CSS}</style>", shared=True)

# Client-side OCR (free): used only when user scans a receipt on Expense form.
ui.add_head_html(
    "<script src='https://cdn.jsdelivr.net/npm/tesseract.js@5/dist/tesseract.min.js'></script>",
    shared=True,
)


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

    drawer = ui.left_drawer(value=False).classes("bg-transparent")
    with drawer:
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
                nav_button("Admin", "settings", "/admin")

    
    with ui.page_sticky(position="top-left", x_offset=14, y_offset=14):
        ui.button(icon="menu").props("round").on("click", drawer.toggle).classes("mf-top-menu")

    with ui.page_sticky(position="bottom-left", x_offset=18, y_offset=18):
        ui.button(icon="menu").props("round").on("click", drawer.toggle)


    with ui.column().classes("w-full max-w-[1100px] mx-auto p-3 gap-3"):
        content_fn()


    # Bottom navigation for mobile
    with ui.footer().classes('my-card q-pa-xs mf-bottom-nav'):
        with ui.row().classes('w-full justify-around items-center'):
            ui.button('Dashboard', icon='dashboard').props('flat').on('click', lambda: nav_to('/'))
            ui.button('Add', icon='add_circle').props('flat').on('click', lambda: nav_to('/add'))
            ui.button('Admin', icon='settings').props('flat').on('click', lambda: nav_to('/admin'))




# -----------------------------
# Shared actions
# -----------------------------
def refresh_all():
    invalidate("transactions", "cards", "recurring", "rules")
    ui.notify("Refreshed", type="positive")


def owners_list() -> List[str]:
    # Phase 2+: treat everything as family-wide (no per-person owner split)
    return ["Family"]


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
    with ui.column().classes('w-full max-w-[640px] mx-auto mt-12 p-4 gap-4'):
        with ui.card().classes('my-card p-8'):
            with ui.row().classes('w-full items-center justify-between'):
                with ui.row().classes('items-center gap-3'):
                    ui.label('💳').classes('text-3xl')
                    with ui.column().classes('gap-0'):
                        ui.label('Welcome to MyFin').classes('text-2xl font-bold')
                        ui.label('Sign in to continue').classes('text-sm').style('color: var(--mf-muted)')
                ui.badge('Secure').style('background: rgba(46,125,255,0.18); color: var(--mf-text); border: 1px solid var(--mf-border);')
            ui.separator().classes('my-4 opacity-30')
            ui.label('Use your admin credentials.').classes('text-sm').style('color: var(--mf-muted)')
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


        # --- Pay-period view (smarter than calendar month for end-of-month salaries) ---
        # Build a combined payday calendar (Salary 1: semimonthly, Salary 2: biweekly anchor)
        try:
            start_d = today()
            # compute paydays ~90 days around today to find previous/next
            window_start = start_d - dt.timedelta(days=60)
            window_end = start_d + dt.timedelta(days=60)
            all_pays: list[dt.date] = []
            yy, mm = window_start.year, window_start.month
            for _ in range(6):
                for p in abhi_pay_dates_for_month(yy, mm):
                    if window_start <= p <= window_end:
                        all_pays.append(p)
                mm += 1
                if mm == 13:
                    yy += 1
                    mm = 1
            for p in wife_pay_dates_between(window_start, window_end):
                if window_start <= p <= window_end:
                    all_pays.append(p)
            all_pays = sorted(set(all_pays))
            prev_pay = max([p for p in all_pays if p <= start_d], default=None)
            next_pay = min([p for p in all_pays if p > start_d], default=None)
            if prev_pay is None:
                prev_pay = start_d - dt.timedelta(days=14)
            if next_pay is None:
                next_pay = start_d + dt.timedelta(days=14)
            pp_start = prev_pay
            pp_end = next_pay

            ptx = tx[(tx['date_parsed'] >= pp_start) & (tx['date_parsed'] < pp_end)].copy()
            ptyp = ptx['type_l']
            pamt = ptx['amount_num']
            # broaden type matching
            income_pp = pamt[ptyp.isin(['credit','income'])].sum()
            expense_pp = pamt[ptyp.isin(['debit','expense'])].sum()
            invest_pp = pamt[ptyp.isin(['investment'])].sum()
            net_pp = income_pp - expense_pp - invest_pp
        except Exception:
            pp_start = today() - dt.timedelta(days=14)
            pp_end = today() + dt.timedelta(days=14)
            income_pp = expense_pp = invest_pp = net_pp = 0.0

        # Expenses for this month (reused by budgets + breakdown)
        spend = mtx[mtx["type_l"].isin(["debit", "expense"])].copy()
        if not spend.empty:
            if "category" not in spend.columns:
                spend["category"] = "Uncategorized"
            spend["category"] = spend["category"].astype(str).replace("", "Uncategorized")

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

        with ui.row().classes('w-full gap-3'):
            for label, val in [
                ('Income (pay period)', income_pp),
                ('Expenses (pay period)', expense_pp),
                ('Investments (pay period)', invest_pp),
                ('Net (pay period)', net_pp),
            ]:
                with ui.card().classes('my-card p-4 kpi w-full'):
                    ui.label(label).classes('text-sm').style('color: var(--mf-muted)')
                    ui.label(currency(val)).classes('text-2xl font-bold')
                    ui.label(f"{pp_start.strftime('%b %d')} → {pp_end.strftime('%b %d')}").classes('text-xs').style('color: var(--mf-muted)')


        # Quick actions + data quality
        unc_count = 0
        try:
            if "category" in mtx.columns:
                unc_count = int((mtx["category"].astype(str).str.strip().replace("", "Uncategorized") == "Uncategorized").sum())
        except Exception:
            unc_count = 0

        # Phase 4.2: in-app notifications (budget + uncategorized)
        try:
            if unc_count > 0:
                ui.notify(f'You have {unc_count} Uncategorized items this month.', type='warning')
        except Exception:
            pass

        with ui.row().classes("w-full gap-3"):
            with ui.card().classes("my-card p-4 w-full"):
                ui.label("Quick actions").classes("text-lg font-bold")
                with ui.row().classes("gap-2"):
                    ui.button("Add expense", icon="add").props("unelevated").on("click", lambda: nav_to('/add?mode=expense'))
                    ui.button("Add income", icon="add").props("outline").on("click", lambda: nav_to('/add?mode=income'))
                    ui.button("Transactions", icon="receipt_long").props("flat").on("click", lambda: nav_to('/tx'))
            with ui.card().classes("my-card p-4 w-full"):
                ui.label("Data quality").classes("text-lg font-bold")
                ui.label(f"Uncategorized this month: {unc_count}").classes("text-sm").style("color: var(--mf-muted)")
                def _go_fix_uncat() -> None:
                    app.storage.user['tx_quick_filter'] = 'uncat'
                    nav_to('/tx')
                ui.button("Fix now", icon="construction").props("outline").on("click", _go_fix_uncat)

        # Budgets (Phase 4)
        budgets = read_df_optional('budgets')
        if budgets is not None and not budgets.empty and (not spend.empty) and "category" in spend.columns:
            # Map budgets
            bcols = {str(c).strip().lower(): c for c in budgets.columns}
            c_cat = bcols.get('category') or bcols.get('cat')
            c_budget = bcols.get('budget_monthly') or bcols.get('monthly_budget') or bcols.get('budget')
            if c_cat and c_budget:
                bmap: dict[str, float] = {}
                for _, r in budgets.iterrows():
                    k = str(r.get(c_cat, '')).strip()
                    if not k:
                        continue
                    bmap[k] = parse_money(r.get(c_budget, 0), default=0.0)
                if bmap:
                    with ui.card().classes('my-card p-5'):
                        ui.label('Budgets (this month)').classes('text-lg font-bold')
                        # build progress list for categories that have a budget
                        spend_by_cat = spend.groupby('category', as_index=False)['amount_num'].sum()
                        # show only budgeted categories
                        rows = []
                        for _, r in spend_by_cat.iterrows():
                            cat = str(r['category'])
                            if cat in bmap and bmap[cat] > 0:
                                rows.append((cat, float(r['amount_num']), float(bmap[cat])))
                        # include budget categories with 0 spend yet
                        present = set([x[0] for x in rows])
                        for cat, bud in bmap.items():
                            if cat not in present and bud > 0:
                                rows.append((cat, 0.0, float(bud)))
                        rows.sort(key=lambda x: (x[1]/x[2]) if x[2] else 0.0, reverse=True)
                        if not rows:
                            ui.label('No budget categories matched your spending yet.').style('color: var(--mf-muted)')
                        else:
                            # Phase 4.2: in-app budget alerts
                            try:
                                alerts80 = [(c, s, b) for c, s, b in rows if b and (s/b) >= 0.80 and (s/b) < 1.0]
                                alerts100 = [(c, s, b) for c, s, b in rows if b and (s/b) >= 1.0]
                                if alerts100:
                                    ui.notify(f'Over budget: {alerts100[0][0]} ({currency(alerts100[0][1])} / {currency(alerts100[0][2])})', type='negative')
                                elif alerts80:
                                    ui.notify(f'Budget warning (80%+): {alerts80[0][0]} ({currency(alerts80[0][1])} / {currency(alerts80[0][2])})', type='warning')
                            except Exception:
                                pass

                            for cat, spent_amt, bud_amt in rows[:10]:
                                pct = min(1.0, spent_amt / bud_amt) if bud_amt else 0.0
                                with ui.row().classes('w-full items-center justify-between'):
                                    ui.label(cat).classes('text-sm')
                                    ui.label(f"{currency(spent_amt)} / {currency(bud_amt)}").classes('text-xs').style('color: var(--mf-muted)')
                                ui.linear_progress(value=pct).props('size=10px')

        # Upcoming paydays
        start = today()
        end = start + dt.timedelta(days=45)
        pays: List[Tuple[str, dt.date]] = []
        y, m = start.year, start.month
        for _ in range(3):
            for p in abhi_pay_dates_for_month(y, m):
                if start <= p <= end:
                    pays.append(("Salary 1", p))
            m += 1
            if m == 13:
                y += 1
                m = 1
        for p in wife_pay_dates_between(start, end):
            if start <= p <= end:
                pays.append(("Salary 2", p))
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
            if spend.empty:
                ui.label("No expenses this month.").style("color: var(--mf-muted)")
            else:
                agg = spend.groupby("category", as_index=False)["amount_num"].sum()
                fig = px.pie(agg, names="category", values="amount_num", hole=0.55)
                fig.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color="rgba(255,255,255,0.88)",
                )
                ui.plotly(fig).classes("w-full")

        # Top merchants (best-effort from Notes)
        with ui.card().classes("my-card p-5"):
            ui.label("Top merchants (this month)").classes("text-lg font-bold")
            if spend.empty or "notes" not in spend.columns:
                ui.label("No merchant data available.").style("color: var(--mf-muted)")
            else:
                def _merchant_from_notes(n: str) -> str:
                    s = str(n or "").strip()
                    if not s:
                        return "(blank)"
                    # common separators: '|', '-', '•'
                    for sep in ("|", "•", "-"):
                        if sep in s:
                            s = s.split(sep, 1)[0].strip()
                    s = re.sub(r"\s+", " ", s)
                    return (s[:28] + "…") if len(s) > 28 else s

                spend["_merchant"] = spend["notes"].apply(_merchant_from_notes)
                topm = spend.groupby("_merchant", as_index=False)["amount_num"].sum().sort_values("amount_num", ascending=False)
                if topm.empty:
                    ui.label("No merchant data available.").style("color: var(--mf-muted)")
                else:
                    rows = []
                    for _, r in topm.head(8).iterrows():
                        rows.append({"merchant": r["_merchant"], "spend": currency(float(r["amount_num"]))})
                    ui.table(
                        columns=[
                            {"name": "merchant", "label": "Merchant", "field": "merchant", "align": "left"},
                            {"name": "spend", "label": "Spend", "field": "spend", "align": "right"},
                        ],
                        rows=rows,
                        row_key="merchant",
                    ).classes("w-full")

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

    def open_add_dialog(entry_type: str, *, preset_category: str | None = None, preset_method: str | None = None, preset_account: str | None = None):
        rules = load_rules()
        owners = owners_list()
        accounts = accounts_list()
        categories = categories_list()
        methods = methods_list()

        # Remember last-used method/account for Expense (Debit) so you don't reselect every time.
        last_debit_method = str(app.storage.user.get('last_debit_method', '') or '').strip()
        last_debit_account = str(app.storage.user.get('last_debit_account', '') or '').strip()

        dlg = ui.dialog()
        with dlg, ui.card().classes("my-card p-5 w-[620px] max-w-[95vw]"):
            ui.label(f"Add: {entry_type}").classes("text-lg font-bold")

            d_date = ui.input("Date", value=today().isoformat()).props("type=date").classes("w-full")
            d_amount = ui.number("Amount", value=0.0, format="%.2f").classes("w-full")

            is_debit = entry_type.lower() == 'debit'
            default_method = ("Card" if is_debit else "Other")
            # Presets override remembered defaults.
            method_default = (preset_method or (last_debit_method if (is_debit and last_debit_method in methods) else default_method))
            account_default = (preset_account or (last_debit_account if (is_debit and last_debit_account in accounts) else (accounts[0] if accounts else "")))
            d_method = ui.select(methods, value=method_default, label="Method").classes("w-full")
            d_account = ui.select(accounts or [""], value=account_default, label="Account").classes("w-full")
            d_category = ui.select(categories, value="Uncategorized", label="Category").classes("w-full")
            d_notes = ui.textarea("Notes", value="").classes("w-full")
            d_rec = ui.checkbox("Mark as recurring (creates template for future cycles only)")

            # Receipt scan (Expense only): opens camera on mobile, runs free OCR in the browser (tesseract.js)
            if entry_type.lower() == 'debit':
                scan_state: Dict[str, Any] = {"data_url": None}

                scan_dlg = ui.dialog()
                parsed_state: Dict[str, Any] = {"parsed": None}
                with scan_dlg, ui.card().classes('my-card p-4 w-[720px] max-w-[95vw]'):
                    # Keep action buttons visible on mobile by making the content area scrollable.
                    with ui.column().classes('w-full max-h-[70vh] overflow-y-auto pr-1'):
                        ui.label('Scan receipt').classes('text-lg font-bold')
                        ui.label('Tip: on iPhone, this will prompt for camera access.').classes('text-xs').style('color: var(--mf-muted)')

                        preview = ui.image('').classes('w-full rounded').style('display:none')

                        # Parsed preview (filled after OCR)
                        with ui.card().classes('my-card p-3 w-full').style('display:none') as parsed_card:
                            ui.label('Detected fields (review before applying)').classes('text-sm font-bold')
                            pv_merchant = ui.input('Merchant', value='').props('readonly').classes('w-full')
                            pv_date = ui.input('Date', value='').props('readonly').classes('w-full')
                            pv_amount = ui.input('Total amount', value='').props('readonly').classes('w-full')
                            pv_last4 = ui.input('Card last-4', value='').props('readonly').classes('w-full')
                            pv_conf = ui.label('').classes('text-xs').style('color: var(--mf-muted)')

                        raw_out = ui.textarea('OCR text (debug)', value='').props('readonly').classes('w-full')
                        raw_out.style('max-height: 160px')

                        async def _on_upload(e: Any) -> None:
                            """Store uploaded image as a data URL for client-side OCR (tesseract.js).

                            NOTE: On Render/iOS, NiceGUI's upload content reader is async.
                            If we don't await it, we end up passing a coroutine (not bytes) and the
                            OCR pipeline fails with: "a bytes-like object is required, not 'coroutine'".
                            """

                            async def _read_bytes(obj: Any) -> Optional[bytes]:
                                if obj is None:
                                    return None
                                if isinstance(obj, (bytes, bytearray)):
                                    return bytes(obj)
                                if hasattr(obj, 'read'):
                                    res = obj.read()
                                    if asyncio.iscoroutine(res):
                                        res = await res
                                    if isinstance(res, (bytes, bytearray)):
                                        return bytes(res)
                                return None

                            try:
                                data: Optional[bytes] = None
                                if hasattr(e, 'content'):
                                    data = await _read_bytes(getattr(e, 'content'))
                                if data is None and hasattr(e, 'file'):
                                    f = getattr(e, 'file')
                                    data = await _read_bytes(f)
                                    if data is None and hasattr(f, 'file'):
                                        data = await _read_bytes(getattr(f, 'file'))
                                if data is None and isinstance(e, dict):
                                    c = e.get('content') or e.get('file')
                                    data = await _read_bytes(c)

                                if not data:
                                    raise ValueError('no file bytes received')

                                mime = getattr(e, 'type', None) or getattr(e, 'mime_type', None) or 'image/jpeg'
                                scan_state['data_url'] = f"data:{mime};base64,{base64.b64encode(data).decode('utf-8')}"
                                preview.set_source(scan_state['data_url'])
                                preview.style('display:block')
                                raw_out.value = ''
                                parsed_state['parsed'] = None
                                parsed_card.style('display:none')
                                apply_btn.disable()
                            except Exception as ex:
                                ui.notify(f'Upload failed: {ex}', type='negative')

                        ui.upload(on_upload=_on_upload, auto_upload=True, label='Capture / Upload receipt')                         .props("accept='image/*' capture='environment'")                         .classes('w-full')

                        async def _run_ocr() -> None:
                            if not scan_state.get('data_url'):
                                ui.notify('Please upload a receipt image first.', type='warning')
                                return
                            ui.notify('Scanning…', type='info')
                            img_literal = json.dumps(str(scan_state.get('data_url', '')))
                            js = f"""
                                // Client-side OCR (tesseract.js).
                                // We downscale large images first to avoid timeouts on mobile Safari.
                                const img = {img_literal};
                                if (!window.Tesseract) {{ return {{ ok: false, error: 'tesseract.js not loaded' }}; }}
                                const downscale = async (dataUrl) => new Promise((resolve) => {{
                                  const im = new Image();
                                  im.onload = () => {{
                                    try {{
                                      const maxW = 1200;
                                      const maxH = 1600;
                                      let w = im.width, h = im.height;
                                      const scale = Math.min(1, maxW / w, maxH / h);
                                      w = Math.max(1, Math.floor(w * scale));
                                      h = Math.max(1, Math.floor(h * scale));
                                      const c = document.createElement('canvas');
                                      c.width = w; c.height = h;
                                      const ctx = c.getContext('2d');
                                      ctx.drawImage(im, 0, 0, w, h);
                                      resolve(c.toDataURL('image/jpeg', 0.85));
                                    }} catch (e) {{
                                      resolve(dataUrl);
                                    }}
                                  }};
                                  im.onerror = () => resolve(dataUrl);
                                  im.src = dataUrl;
                                }});
                                try {{
                                  const small = await downscale(img);
                                  const res = await Tesseract.recognize(small, 'eng');
                                  return {{ ok: true, text: res.data.text || '' }};
                                }} catch (e) {{
                                  return {{ ok: false, error: String(e) }};
                                }}
                            """
                            try:
                                # Mobile/browser OCR can easily take several seconds; 1s default is too low.
                                result = await ui.run_javascript(js, timeout=60.0)
                            except TimeoutError:
                                ui.notify('OCR timed out (slow device/network). Try retaking closer or smaller image.', type='negative')
                                return
                            except Exception as ex:
                                ui.notify(f'OCR failed: {ex}', type='negative')
                                return

                            if not result or not isinstance(result, dict) or not result.get('ok'):
                                err = (result or {}).get('error', 'Unknown OCR error') if isinstance(result, dict) else 'Unknown OCR error'
                                ui.notify(f'OCR failed: {err}', type='negative')
                                return

                            text = str(result.get('text') or '')
                            raw_out.value = text

                            parsed = parse_receipt_text(text)
                            parsed_state['parsed'] = parsed

                            merch = str(parsed.get('merchant') or '').strip()
                            last4 = str(parsed.get('card_last4') or '').strip()
                            rdate = parsed.get('date')
                            amt = parsed.get('amount')
                            conf = float(parsed.get('amount_confidence') or 0.0)
                            src = str(parsed.get('amount_source') or '')

                            # Update preview UI
                            pv_merchant.value = merch
                            pv_date.value = (rdate.isoformat() if rdate else '')
                            pv_amount.value = (f"{float(amt):.2f}" if amt is not None else '')
                            pv_last4.value = last4
                            pv_conf.text = f"Amount confidence: {conf:.1f}/10 (source: {src})" + (" — please double-check" if conf < 3.0 else "")
                            parsed_card.style('display:block')
                            apply_btn.enable()

                            if conf < 3.0:
                                ui.notify('Low confidence TOTAL detected — verify amount before applying.', type='warning')
                            else:
                                ui.notify('Scan complete. Review and tap Apply.', type='positive')

                        def _apply_to_form() -> None:
                            parsed = parsed_state.get('parsed') or {}
                            if not parsed:
                                ui.notify('Nothing to apply yet.', type='warning')
                                return

                            merch = str(parsed.get('merchant') or '').strip()
                            last4 = str(parsed.get('card_last4') or '').strip()
                            rdate = parsed.get('date')
                            amt = parsed.get('amount')
                            conf = float(parsed.get('amount_confidence') or 0.0)

                            if rdate:
                                d_date.value = rdate.isoformat()
                            if amt is not None:
                                # Even when low confidence, pre-fill but warn. User can edit before saving.
                                try:
                                    d_amount.value = float(amt)
                                except Exception:
                                    pass

                            # Notes hint
                            if merch or last4:
                                prefix = []
                                if merch:
                                    prefix.append(merch)
                                if last4:
                                    prefix.append(f"****{last4}")
                                hint = ' '.join(prefix)
                                if not str(d_notes.value or '').strip():
                                    d_notes.value = hint
                                else:
                                    d_notes.value = f"{hint} | {d_notes.value}"

                            # Try auto-pick account from detected last-4 (optional).
                            # If no mapping exists / no match, we keep the remembered default selection.
                            if last4:
                                try:
                                    cards_df = cached_df('cards', force=True)
                                    acct = pick_account_from_last4(cards_df, last4)
                                    if acct and (acct in accounts):
                                        d_account.value = acct
                                        # Most receipts are card-based; set method if available.
                                        if 'Card' in methods:
                                            d_method.value = 'Card'
                                except Exception:
                                    pass

                            # Refresh category suggestion with updated notes
                            _refresh_suggestion()
                            if conf < 3.0:
                                ui.notify('Applied, but amount confidence was low — please verify before saving.', type='warning')
                            else:
                                ui.notify('Applied scan results. Please review and save.', type='positive')
                            scan_dlg.close()

                    # Sticky footer so buttons don't get pushed below the upload card on mobile
                    with ui.row().classes('w-full items-center gap-2 sticky bottom-0').style('background: rgba(8,12,20,0.92); backdrop-filter: blur(8px); padding: 10px; border-top: 1px solid rgba(255,255,255,0.06)'):
                        ui.button('Run scan', on_click=_run_ocr).props('unelevated').classes('flex-1')
                        apply_btn = ui.button('Apply', on_click=_apply_to_form).props('unelevated')
                        apply_btn.classes('flex-1')
                        apply_btn.disable()
                        ui.button('Close', on_click=scan_dlg.close).props('outline')

                ui.button('Scan receipt', on_click=scan_dlg.open).props('outline').classes('w-full')

            # --- Live category suggestion (Option B): show suggestion while typing, apply on save unless user overrides ---
            category_touched = {"v": False}
            suggest_label = ui.label("").classes("text-xs")
            suggest_label.style("color: var(--mf-muted)")

            def _refresh_suggestion(_: Any = None) -> None:
                active_rules = rules
                if not active_rules:
                    # Try once to load rules (in case sheet headers were fixed after app boot)
                    active_rules = load_rules(force=True)
                if not active_rules:
                    suggest_label.text = "Suggested category: Uncategorized (no rules loaded)"
                    if not category_touched["v"]:
                        d_category.value = "Uncategorized"
                    return
                suggestion = infer_category(str(d_notes.value or ""), active_rules) or "Uncategorized"
                suggest_label.text = f"Suggested category: {suggestion}"
                if not category_touched["v"]:
                    d_category.value = suggestion

            # mark manual override
            d_category.on('update:model-value', lambda e: category_touched.__setitem__('v', True))
            # refresh suggestion on notes changes
            d_notes.on('update:model-value', _refresh_suggestion)
            _refresh_suggestion()

            # Apply presets (used for special flows like LOC withdrawal/repayment)
            if preset_method is not None:
                d_method.value = preset_method
                d_method.disable()
            if preset_account is not None:
                d_account.value = preset_account
            if preset_category is not None:
                d_category.value = preset_category

            def autofill():
                # manual button: set category based on current notes
                category_touched["v"] = True
                # force-refresh rules so updates in the sheet are picked up
                fresh_rules = load_rules(force=True)  # force refresh so sheet updates are picked up
                if not fresh_rules:
                    ui.notify('No rules loaded (check Rules sheet columns). Keeping Uncategorized.', type='warning')
                    d_category.value = 'Uncategorized'
                    return
                d_category.value = infer_category(d_notes.value or "", fresh_rules) or "Uncategorized"
                ui.notify("Category updated", type="positive")

            ui.button("Auto-category", on_click=autofill).props("flat")

            def save():

                dd = parse_date(d_date.value) or today()

                amt = float(to_float(d_amount.value))

                owner = "Family"

                method = str(d_method.value or "Other").strip()

                account = str(d_account.value or "").strip()

                # Remember last-used card/account for Expenses (Debit) so next time it's preselected.
                try:
                    if entry_type.lower() == 'debit':
                        if method:
                            app.storage.user['last_debit_method'] = method
                        if account:
                            app.storage.user['last_debit_account'] = account
                except Exception:
                    pass

                category = str(d_category.value or "Uncategorized").strip()

                notes = str(d_notes.value or "").strip()


                try:

                    # Build tx id (unique)

                    tx_id = sha16(f"{owner}|{dd.isoformat()}|{entry_type}|{amt}|{method}|{account}|{category}|{notes}|{dt.datetime.now().isoformat()}")


                    rec_id = ""

                    if d_rec.value:

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


                    append_tx(

                        tx_id=tx_id,

                        date_=dd,

                        owner=owner,

                        type_=entry_type,

                        amount=amt,

                        method=method,

                        account=account,

                        category=category,

                        notes=notes,

                        recurring_id=rec_id,

                    )


                    # refresh in-memory cache so the new entry shows up immediately
                    invalidate('transactions')
                    invalidate('recurring')

                    ui.notify("Saved", type="positive")

                    dlg.close()


                except Exception as e:

                    ui.notify(f"Save failed: {e}", type="negative")


            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Cancel", on_click=dlg.close).props("flat")
                ui.button("Save", on_click=save).props("unelevated")

        dlg.open()

    def content():
        with ui.card().classes("my-card p-5"):
            ui.label("Quick Add").classes("text-lg font-bold")
            ui.label("Tap a tile to add an entry.").classes("text-sm").style("color: var(--mf-muted)")

            tiles = [
                ("Expense", "remove_shopping_cart", "Debit", {}),
                ("Income", "payments", "Credit", {}),
                ("Investment", "savings", "Investment", {}),
                ("Credit Card Repayment", "credit_score", "CC Repay", {}),
                ("LOC Withdrawal", "account_balance", "LOC Draw", {"preset_category": "LOC Utilization", "preset_method": "Card", "preset_account": "Line of Credit"}),
                ("LOC Repayment", "swap_horiz", "LOC Repay", {"preset_category": "Repayment", "preset_method": "Bank", "preset_account": "Line of Credit"}),
            ]

            with ui.row().classes("w-full gap-3"):
                for label, icon, etype, kw in tiles:
                    with ui.card().classes("my-card p-4 tile w-full"):
                        ui.label(label).classes("font-bold")
                        ui.icon(icon).classes("text-2xl")
                        ui.button("Add", on_click=lambda e=etype, k=kw: open_add_dialog(e, **k)).props("flat").classes("mt-2")

        with ui.card().classes("my-card p-5"):
            ui.label("Today’s auto status").classes("text-lg font-bold")
            ui.label("Recurring entries will be created only when the due date arrives.").style("color: var(--mf-muted)")
            ui.button("Run recurring generation now", on_click=lambda: ui.notify(f"Created {generate_recurring_for_date(today())} entries", type="positive")).props("flat")

    shell(content)




@ui.page("/admin")
def admin_page() -> None:
    if not require_login():
        nav_to("/login")
        return

    def content() -> None:
        with ui.card().classes("my-card p-5"):
            ui.label("Admin").classes("text-lg font-bold")
            ui.label("Manage rules, cards, recurring templates, and fix mistakes.").style("color: var(--mf-muted)")

            with ui.column().classes("w-full gap-3 mt-3"):
                ui.button("Keyword Rules", on_click=lambda: nav_to("/rules")).props("unelevated").classes("w-full")
                ui.button("Cards", on_click=lambda: nav_to("/cards")).props("unelevated").classes("w-full")
                ui.button("Recurring Templates", on_click=lambda: nav_to("/recurring")).props("unelevated").classes("w-full")
                ui.button("Transactions (Fix Mistakes)", on_click=lambda: nav_to("/tx")).props("unelevated").classes("w-full")
                ui.button("Budgets", on_click=lambda: nav_to("/budgets")).props("unelevated").classes("w-full")
                ui.button("Data Tools (Import/Backup)", on_click=lambda: nav_to("/data_tools")).props("unelevated").classes("w-full")

        with ui.card().classes("my-card p-5"):
            ui.label("Locks").classes("text-lg font-bold")
            ui.label("Locking is enforced by your Transactions sheet’s locked_month column (if present). Use sheet-side admin for now.").style("color: var(--mf-muted)")

    shell(content)

@ui.page("/tx")
def transactions_page():
    # Keep track of the currently selected row (so Edit/Delete buttons work consistently)
    selected_row: Dict[str, Any] = {'row': None}
    if not require_login():
        nav_to("/login")
        return

    def content():
        # Data source / debug panel (helps verify we are reading the correct spreadsheet and tabs)
        try:
            ss = get_spreadsheet()
            ensure_tabs()
            with ui.expansion('Data Source (debug)', icon='info').classes('w-full'):
                ui.label(f'Spreadsheet: {ss.title}').classes('text-sm')
                ui.label(f'Spreadsheet ID: {SPREADSHEET_ID or "(opened by name)"}').classes('text-sm')
                # Show discovered worksheet titles + row counts
                for k in ['transactions', 'cards', 'rules', 'recurring']:
                    try:
                        w = ws(k)
                        n_rows = len(w.get_all_values()) - 1
                        ui.label(f'Tab "{k}" -> worksheet "{w.title}": {max(n_rows, 0)} data rows').classes('text-sm')
                    except Exception as e:
                        ui.label(f'Tab "{k}": ERROR: {e}').classes('text-sm text-red-300')
        except Exception as e:
            ui.label(f'Data source error: {e}').classes('text-sm text-red-300')

        tx = cached_df("transactions")
        # Selection uses `row_key='id'`. If `id` is empty for older data, every row shares the
        # same key (""), which makes the table behave like "select all".
        # We therefore backfill `id` from the legacy `TxId` (or `txid`) column when needed.
        if not tx.empty:
            try:
                if "id" in tx.columns:
                    ids = tx["id"].astype(str).fillna("")
                    missing = ids.str.strip() == ""
                else:
                    tx["id"] = ""
                    missing = pd.Series([True] * len(tx), index=tx.index)

                legacy_col = None
                for cand in ["TxId", "txid", "TXID"]:
                    if cand in tx.columns:
                        legacy_col = cand
                        break

                if legacy_col is not None and missing.any():
                    tx.loc[missing, "id"] = tx.loc[missing, legacy_col].astype(str)

                # Final fallback: generate deterministic ids for any still-missing rows
                ids2 = tx["id"].astype(str).fillna("")
                still_missing = ids2.str.strip() == ""
                if still_missing.any():
                    tx.loc[still_missing, "id"] = [f"row_{i}" for i in range(still_missing.sum())]
            except Exception:
                # Never break the page due to id normalization
                pass
        if tx.empty:
            with ui.card().classes("my-card p-5"):
                ui.label("No transactions").classes("text-lg font-bold")
            return

        tx["date_parsed"] = tx["date"].apply(parse_date)
        tx = tx[tx["date_parsed"].notna()].copy()
        tx = tx.sort_values("date_parsed", ascending=False)

        types = sorted({t for t in tx["type"].astype(str).tolist() if t.strip()})

        # -----------------------------
        # Phase 4 helpers (Transactions)
        # -----------------------------
        def _export_csv(last_view: Dict[str, Any]) -> None:
            df = last_view.get('df')
            if df is None or getattr(df, 'empty', True):
                ui.notify('Nothing to export (adjust filters first).', type='warning')
                return
            try:
                out = df.drop(columns=['date_parsed', 'amount_num'], errors='ignore').copy()
                csv_text = out.to_csv(index=False)
                ui.download(csv_text.encode('utf-8'), filename=f"transactions_{datetime.date.today().isoformat()}.csv")
            except Exception as e:
                ui.notify(f'Export failed: {e}', type='negative')

        def _compute_duplicates(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame()
            base = df.copy()
            for c in ('date', 'amount', 'notes'):
                if c not in base.columns:
                    base[c] = ''
            # Normalize amount to numeric when possible
            base['_amt'] = base.get('amount', '').apply(to_float)
            base['_key'] = base['date'].astype(str).str.strip() + '|' + base['_amt'].astype(str) + '|' + base['notes'].astype(str).str.strip()
            dup = base[base.duplicated('_key', keep=False)].copy()
            if dup.empty:
                return pd.DataFrame()
            return dup.sort_values(['date', '_amt'], ascending=[False, True])

        def _show_duplicates(last_view: Dict[str, Any]) -> None:
            df = last_view.get('df')
            dup = _compute_duplicates(df) if df is not None else pd.DataFrame()
            if dup.empty:
                ui.notify('No duplicates found in the current filtered view.', type='positive')
                return
            with ui.dialog() as d, ui.card().classes('my-card p-4 w-[92vw] max-w-5xl'):
                ui.label(f'Duplicates found: {len(dup)} rows').classes('text-lg font-bold')
                ui.label('Duplicates are detected by Date + Amount + Notes.').classes('text-sm').style('color: var(--mf-muted)')
                rows = dup.drop(columns=['_key'], errors='ignore').head(200).to_dict(orient='records')
                ui.table(
                    columns=[
                        {"name": "date", "label": "Date", "field": "date"},
                        {"name": "type", "label": "Type", "field": "type"},
                        {"name": "amount", "label": "Amount", "field": "amount"},
                        {"name": "category", "label": "Category", "field": "category"},
                        {"name": "notes", "label": "Notes", "field": "notes"},
                        {"name": "id", "label": "ID", "field": "id"},
                    ],
                    rows=rows,
                    row_key='id',
                ).classes('w-full')
                ui.button('Close', on_click=d.close).props('flat')
            d.open()

        def _apply_category_selected(table_ref, category_val: str) -> None:
            if not getattr(table_ref, 'selected', None):
                ui.notify('Select a transaction row first.', type='warning')
                return
            row = table_ref.selected[0]
            rid = str(row.get('id', '')).strip()
            if not rid:
                ui.notify('Selected row has no id; cannot update.', type='negative')
                return
            ok = update_row_by_id('transactions', 'id', rid, {'category': category_val or 'Uncategorized'})
            if not ok:
                ui.notify('Update failed. Please refresh and try again.', type='negative')
                return
            invalidate('transactions')
            ui.notify('Category updated.', type='positive')
            nav_to('/tx')

        with ui.card().classes("my-card p-5"):
            ui.label("Transactions").classes("text-lg font-bold")
            f_type = ui.select(["All"] + types, value="All", label="Type").classes("w-full")
            f_text = ui.input("Search notes/category/account").classes("w-full")
            # Quick filter (e.g., from Dashboard "Fix now")
            try:
                if app.storage.user.get('tx_quick_filter') == 'uncat':
                    f_text.value = 'Uncategorized'
                    app.storage.user.pop('tx_quick_filter', None)
            except Exception:
                pass
            sort_opts = ["Date (new → old)", "Date (old → new)", "Amount (high → low)", "Amount (low → high)"]
            f_sort = ui.select(sort_opts, value=sort_opts[0], label="Sort").classes("w-full")
            # Date range filter (defaults to last 30 days)
            try:
                _today = datetime.date.today()
                _from = (_today - datetime.timedelta(days=30)).isoformat()
                _to = _today.isoformat()
            except Exception:
                _from = ''
                _to = ''
            with ui.row().classes('w-full items-center gap-2'):
                f_from = ui.input('From').props('type=date dense outlined').classes('w-40')
                f_to = ui.input('To').props('type=date dense outlined').classes('w-40')
            f_from.value = _from
            f_to.value = _to

            # Phase 4: export + quick fix tools (wired after the table is created)
            last_view: Dict[str, Any] = {'df': None}

            # Table: show compact columns by default (mobile-friendly). Use Details to view/edit full row.
            with ui.element('div').classes('w-full overflow-x-auto'):
                table = ui.table(columns=[
                    {"name": "date", "label": "Date", "field": "date"},
                    {"name": "type", "label": "Type", "field": "type"},
                    {"name": "amount", "label": "Amount", "field": "amount", "align": "right"},
                    {"name": "category", "label": "Category", "field": "category"},
                ], rows=[], row_key="id", selection='single').classes("w-full")
                table.props('dense flat bordered')

            def _open_details(row: Dict[str, Any]) -> None:
                with ui.dialog() as d, ui.card().classes('my-card p-4 w-[92vw] max-w-3xl'):
                    ui.label('Transaction details').classes('text-lg font-bold')
                    def line(k, v):
                        with ui.row().classes('w-full justify-between'):
                            ui.label(k).classes('text-xs').style('color: var(--mf-muted)')
                            ui.label(v).classes('text-sm')
                    line('Date', str(row.get('date','')))
                    line('Type', str(row.get('type','')))
                    line('Amount', str(row.get('amount','')))
                    line('Method', str(row.get('method','')))
                    line('Account', str(row.get('account','')))
                    line('Category', str(row.get('category','')))
                    ui.separator().classes('my-2 opacity-30')
                    ui.label('Notes').classes('text-xs').style('color: var(--mf-muted)')
                    ui.label(str(row.get('notes',''))).classes('text-sm')
                    with ui.row().classes('w-full justify-end gap-2 mt-3'):
                        ui.button('Edit', icon='edit', on_click=lambda: (d.close(), open_edit(row))).props('unelevated')
                        ui.button('Close', on_click=d.close).props('flat')
                d.open()
            # Make tapping a row select it (helps on mobile where the checkbox can be fiddly).
            def _on_row_click(e):
                row = e.args.get('row') if isinstance(e.args, dict) else None
                if row is not None:
                    table.selected = [row]
                    selected_row['row'] = row

            table.on('rowClick', _on_row_click)

            # Category quick-apply (useful for fixing Uncategorized)
            try:
                rules_df = cached_df('rules')
                cats = sorted({str(x).strip() for x in rules_df.get('category', pd.Series([])).tolist() if str(x).strip()})
            except Exception:
                cats = []
            try:
                existing_cats = sorted({str(x).strip() for x in tx.get('category', pd.Series([])).tolist() if str(x).strip()})
            except Exception:
                existing_cats = []
            cat_choices = ['Uncategorized'] + sorted({*cats, *existing_cats})

            with ui.row().classes('w-full items-center justify-between gap-2 mt-2'):
                with ui.row().classes('gap-2 items-center'):
                    ui.button('Export CSV', icon='download').props('outline').on('click', lambda: _export_csv(last_view))
                    ui.button('Show duplicates', icon='difference').props('flat').on('click', lambda: _show_duplicates(last_view))
                with ui.row().classes('gap-2 items-center'):
                    fix_cat = ui.select(cat_choices, value=cat_choices[0], label='Quick set category').classes('w-64')
                    ui.button('Apply to selected', icon='label').props('unelevated').on('click', lambda: _apply_category_selected(table, fix_cat.value))

            def refresh_table():
                df = tx.copy()
                if f_type.value != "All":
                    df = df[df["type"].astype(str) == f_type.value]
                # Date filter (inclusive)
                try:
                    d_from = parse_date(f_from.value) if f_from.value else None
                    d_to = parse_date(f_to.value) if f_to.value else None
                except Exception:
                    d_from = d_to = None
                if d_from or d_to:
                    def _in_range(x):
                        try:
                            dx = parse_date(x)
                        except Exception:
                            return False
                        if d_from and dx < d_from:
                            return False
                        if d_to and dx > d_to:
                            return False
                        return True
                    df = df[df['date'].apply(_in_range)]
                q = (f_text.value or "").strip().lower()
                if q:
                    hay = (df["notes"].astype(str) + " " + df["category"].astype(str) + " " + df["account"].astype(str)).str.lower()
                    df = df[hay.str.contains(q, na=False)]
                # Sorting
                try:
                    sort_choice = f_sort.value or "Date (new → old)"
                except Exception:
                    sort_choice = "Date (new → old)"

                if "Amount" in sort_choice:
                    df["__amt"] = df["amount"].apply(to_float)
                    ascending = "low → high" in sort_choice
                    df = df.sort_values(by="__amt", ascending=ascending)
                    df = df.drop(columns=["__amt"], errors="ignore")
                else:
                    # Date sorting uses parsed date
                    if "date_parsed" not in df.columns:
                        df["date_parsed"] = df["date"].apply(parse_date)
                    ascending = "old → new" in sort_choice
                    df = df.sort_values(by="date_parsed", ascending=ascending)

                # keep a copy of the current filtered/sorted view for export & diagnostics
                try:
                    last_view['df'] = df.copy()
                except Exception:
                    last_view['df'] = None

                df = df.head(250)
                df["amount"] = df["amount"].apply(lambda x: currency(to_float(x)))
                table.rows = df.to_dict(orient="records")
                table.update()

            f_type.on("update:model-value", lambda e: refresh_table())
            f_text.on("update:model-value", lambda e: refresh_table())
            f_sort.on("update:model-value", lambda e: refresh_table())
            f_from.on("update:model-value", lambda e: refresh_table())
            f_to.on("update:model-value", lambda e: refresh_table())

            refresh_table()

            # Edit/Delete
            def open_edit(row: Dict[str, Any]):
                dlg = ui.dialog()
                with dlg, ui.card().classes("my-card p-5 w-[720px] max-w-[95vw]"):
                    ui.label("Edit transaction").classes("text-lg font-bold")
                    tid = str(row.get("id", "")).strip()

                    e_date = ui.input("Date", value=str(row.get("date", ""))).props("type=date").classes("w-full")
                    e_type = ui.input("Type", value=str(row.get("type", ""))).classes("w-full")
                    e_amount = ui.number("Amount", value=to_float(row.get("amount", 0))).classes("w-full")
                    e_method = ui.input("Method", value=str(row.get("method", ""))).classes("w-full")
                    e_account = ui.input("Account", value=str(row.get("account", ""))).classes("w-full")
                    e_category = ui.input("Category", value=str(row.get("category", ""))).classes("w-full")
                    e_notes = ui.textarea("Notes", value=str(row.get("notes", ""))).classes("w-full")

                    def save_edit():
                        ok = update_row_by_id("transactions", "id", tid, {
                            "date": (parse_date(e_date.value) or today()).isoformat(),
                            # owner is kept for backward compatibility but hidden from the UI
                            "owner": str(row.get("owner", "Family")) or "Family",
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
                def _current_row() -> Optional[Dict[str, Any]]:
                    if table.selected:
                        return table.selected[0]
                    return selected_row.get('row')

                ui.button(
                    "Edit selected",
                    on_click=lambda: open_edit(_current_row()) if _current_row() else ui.notify("Select a row", type="warning"),
                ).props("flat")
                ui.button(
                    "Delete selected",
                    on_click=lambda: open_delete(_current_row()) if _current_row() else ui.notify("Select a row", type="warning"),
                ).props("flat")

    shell(content)


@ui.page("/cards")
def cards_page() -> None:
    if not require_login():
        nav_to('/login')
        return

    def content() -> None:
        ui.label('Cards').classes('text-2xl font-semibold').style('color: var(--mf-text);')
        ui.label('Limits and billing details from Google Sheets.').classes('text-sm').style('color: var(--mf-muted);')

        df = cached_df('cards')
        if df.empty:
            ui.label('No cards found in the "cards" sheet.').classes('text-sm').style('color: var(--mf-muted);')
            return

        # Accept both old and new schemas
        def pick(col_candidates, default=''):
            for c in col_candidates:
                if c in df.columns:
                    return df[c]
            return [default] * len(df)

        names = pick(['card_name', 'name', 'account', 'Account'], default='Card')
        emojis = pick(['emoji', 'Emoji'], default='💳')
        methods = pick(['method_name', 'method', 'Method'], default='')
        billing_days = pick(['billing_day', 'BillingDay', 'billingday'], default='')
        limits = pick(['max_limit', 'limit', 'Limit'], default='')

        grid = ui.row().classes('w-full q-col-gutter-md')
        grid.style('flex-wrap: wrap;')

        for i in range(len(df)):
            name = str(names[i]).strip() or 'Card'
            emoji = str(emojis[i]).strip() or '💳'
            method = str(methods[i]).strip()
            bd = str(billing_days[i]).strip()
            lim = parse_money(limits[i])

            # Utilization (approx): sum Debit spending by method_name minus CC Repay where account matches card
            tx = cached_df('transactions')
            txu = tx.copy()
            if not txu.empty:
                txu['type_l'] = txu.get('type','').astype(str).str.strip().str.lower()
                txu['amount_num'] = txu.get('amount','').apply(to_float)
                txu['date_parsed'] = txu.get('date','').apply(parse_date)
            method_key = str(method).strip()
            card_key = str(name).strip()
            # billing cycle start
            cycle_start = None
            try:
                bd_int = int(float(bd)) if str(bd).strip() else None
            except Exception:
                bd_int = None
            if bd_int and bd_int >= 1 and bd_int <= 31:
                today_d = today()
                y,m = today_d.year, today_d.month
                # last statement day
                import calendar as _cal
                last_day = _cal.monthrange(y,m)[1]
                stmt_this = dt.date(y,m, min(bd_int,last_day))
                if today_d >= stmt_this:
                    last_stmt = stmt_this
                else:
                    pm = m-1
                    py = y
                    if pm==0:
                        pm=12; py=y-1
                    last_day2 = _cal.monthrange(py,pm)[1]
                    last_stmt = dt.date(py,pm, min(bd_int,last_day2))
                cycle_start = last_stmt + dt.timedelta(days=1)
            if cycle_start is None:
                cycle_start = today() - dt.timedelta(days=30)

            util_used = 0.0
            util_paid = 0.0
            if not txu.empty:
                scope = txu[txu['date_parsed'].notna() & (txu['date_parsed'] >= cycle_start)].copy()
                # spending: Debit/Expense on this method
                spend_mask = scope['type_l'].isin(['debit','expense']) & (scope.get('method','').astype(str).str.strip() == method_key)
                util_used = float(scope.loc[spend_mask, 'amount_num'].sum())
                # repayments: CC Repay variants where account equals card name (best-effort)
                repay_mask = scope['type_l'].isin(['cc repay','cc_repay','credit card repay','credit card repayment','cc repayment']) & (scope.get('account','').astype(str).str.strip() == card_key)
                util_paid = float(scope.loc[repay_mask, 'amount_num'].sum())
            balance = max(0.0, util_used - util_paid)
            remaining = max(0.0, (lim - balance)) if lim else 0.0
            pct = (balance/lim) if lim else 0.0
            pct = max(0.0, min(1.0, pct))


            with grid:
                with ui.card().classes('my-card').style('width: 320px; max-width: 100%;'):
                    with ui.row().classes('items-center justify-between'):
                        ui.label(f'{emoji} {name}').classes('text-lg font-semibold').style('color: var(--mf-text);')
                        if method:
                            ui.badge(method).classes('q-pa-xs').style('background: rgba(46,125,255,0.18); color: var(--mf-text); border: 1px solid var(--mf-border);')

                    with ui.row().classes('items-center q-gutter-md'):
                        with ui.column().classes('q-gutter-xs'):
                            ui.label('Billing day').classes('text-xs').style('color: var(--mf-muted);')
                            ui.label(bd or '—').classes('text-sm').style('color: var(--mf-text);')

                        with ui.column().classes('q-gutter-xs'):
                            ui.label('Limit').classes('text-xs').style('color: var(--mf-muted);')
                            ui.label(f'${lim:,.2f}' if lim else '—').classes('text-sm').style('color: var(--mf-text);')

                    ui.separator().classes('my-3 opacity-30')
                    with ui.row().classes('w-full items-center justify-between'):
                        ui.label('Used').classes('text-xs').style('color: var(--mf-muted)')
                        ui.label(currency(balance)).classes('text-sm font-semibold')
                    with ui.element('div').classes('w-full mf-progress'):
                        ui.element('div').style(f'width: {pct*100:.1f}%;')
                    with ui.row().classes('w-full items-center justify-between mt-2'):
                        ui.label('Remaining').classes('text-xs').style('color: var(--mf-muted)')
                        ui.label(currency(remaining) if lim else '—').classes('text-sm')
                    ui.label(f'Cycle start: {cycle_start.isoformat()}').classes('text-xs').style('color: var(--mf-muted)')


    shell(content)


@ui.page("/recurring")
def recurring_page():
    if not require_login():
        nav_to("/login")
        return

    def content():
        rdf = cached_df("recurring")
        with ui.card().classes("my-card p-5"):
            ui.label("Recurring templates").classes("text-lg font-bold")
            ui.label("Templates only. Transactions get created when the due date arrives.").style("color: var(--mf-muted)")

            if rdf.empty:
                ui.label("No templates yet. Mark an Add entry as recurring to create one.").style("color: var(--mf-muted)")
                return

            rdf2 = rdf.copy()
            rdf2["active"] = rdf2["active"].astype(str)

            # Phase 4: preview upcoming recurring posts (next 45 days)
            try:
                start_d = today()
                end_d = start_d + dt.timedelta(days=45)
                upcoming: list[dict[str, Any]] = []
                for _, r in rdf2.iterrows():
                    is_active = str(r.get('active', 'TRUE')).strip().upper() in ('TRUE', '1', 'YES', 'Y')
                    if not is_active:
                        continue
                    try:
                        dom = int(float(str(r.get('day_of_month', '1')).strip() or '1'))
                    except Exception:
                        dom = 1
                    y, m = start_d.year, start_d.month
                    # compute next due date this month or next
                    for _ in range(2):
                        last_day = calendar.monthrange(y, m)[1]
                        dd = min(max(1, dom), last_day)
                        due = dt.date(y, m, dd)
                        if due < start_d:
                            # move to next month
                            m += 1
                            if m == 13:
                                y += 1
                                m = 1
                            continue
                        if due <= end_d:
                            upcoming.append({
                                'due': due.isoformat(),
                                'type': str(r.get('type', '')),
                                'amount': currency(to_float(r.get('amount', 0))),
                                'category': str(r.get('category', '')),
                                'notes': str(r.get('notes', ''))[:28],
                            })
                        break
                upcoming = sorted(upcoming, key=lambda x: x['due'])
                with ui.card().classes('my-card p-4 mt-3'):
                    ui.label('Upcoming recurring (next 45 days)').classes('text-md font-bold')
                    if not upcoming:
                        ui.label('No upcoming posts found.').style('color: var(--mf-muted)')
                    else:
                        ui.table(
                            columns=[
                                {'name': 'due', 'label': 'Due', 'field': 'due'},
                                {'name': 'type', 'label': 'Type', 'field': 'type'},
                                {'name': 'amount', 'label': 'Amount', 'field': 'amount'},
                                {'name': 'category', 'label': 'Category', 'field': 'category'},
                                {'name': 'notes', 'label': 'Notes', 'field': 'notes'},
                            ],
                            rows=upcoming[:20],
                            row_key='due',
                        ).classes('w-full')
            except Exception:
                pass
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
        rdf = cached_df("rules")
        with ui.card().classes("my-card p-5"):
            ui.label("Rules").classes("text-lg font-bold")
            ui.label("Keyword → category mapping used for Auto-category in Add.").style("color: var(--mf-muted)")

            table = ui.table(columns=[
                {"name": "keyword", "label": "Keyword", "field": "keyword"},
                {"name": "category", "label": "Category", "field": "category"},
            ], rows=rdf.to_dict(orient="records") if not rdf.empty else [], row_key="keyword").classes("w-full")

            # Phase 4: quick rule tester
            with ui.card().classes('my-card p-4 mt-4'):
                ui.label('Test a rule').classes('text-md font-bold')
                sample = ui.input('Paste merchant/notes text here').classes('w-full')
                result = ui.label('')
                def _test():
                    rules = load_rules(force=True)
                    cat = infer_category(sample.value or '', rules)
                    result.text = f"Matched category: {cat}"
                ui.button('Test', icon='science', on_click=_test).props('outline')

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

            

            def _load_selected_rule() -> None:
                if not table.selected:
                    ui.notify('Select a row', type='warning')
                    return
                r = table.selected[0]
                k.value = str(r.get('keyword', '') or '')
                c.value = str(r.get('category', '') or '')

            def edit_rule() -> None:
                if not table.selected:
                    ui.notify('Select a row', type='warning')
                    return
                orig_kw = str(table.selected[0].get('keyword', '') or '').strip()
                new_kw = str(k.value or '').strip()
                new_cat = str(c.value or '').strip()
                if not orig_kw:
                    ui.notify('Selected row missing keyword', type='negative')
                    return
                if not new_kw:
                    ui.notify('Keyword required', type='warning')
                    return
                if not new_cat:
                    ui.notify('Category required', type='warning')
                    return
                try:
                    if new_kw.lower() == orig_kw.lower():
                        ok = update_row_by_id('rules', 'keyword', orig_kw, {'keyword': new_kw, 'category': new_cat})
                        if not ok:
                            ui.notify('Update failed', type='negative')
                            return
                    else:
                        # keyword changed: delete old + add new (keeps sheet simple)
                        if not delete_row_by_id('rules', 'keyword', orig_kw):
                            ui.notify('Could not replace rule (delete failed)', type='negative')
                            return
                        append_row('rules', {'keyword': new_kw, 'category': new_cat})
                    invalidate('rules')
                    ui.notify('Rule updated', type='positive')
                    nav_to('/rules')
                except Exception as e:
                    ui.notify(f'Edit failed: {e}', type='negative')
            with ui.row().classes("gap-2 mt-3"):
                ui.button('Load selected', on_click=_load_selected_rule).props('flat')
                ui.button('Save changes', on_click=edit_rule).props('unelevated')
                ui.button('Add rule', on_click=add_rule).props('outline')
                ui.button('Delete selected', on_click=del_rule).props('flat')

    shell(content)



# =============================
# Phase 4.2 additions
# - Budgets editor UI
# - Data Tools: CSV import + Backup/Restore
# - Merchant cleanup helper
# =============================

import io
import zipfile

OPTIONAL_SHEETS = {
    'budgets': ['category', 'budget_monthly'],
}


def ensure_optional_sheet(title: str, headers: list[str]) -> bool:
    """Ensure an optional worksheet exists. Returns True if exists/created."""
    ss = get_spreadsheet()
    want = title.strip().lower()
    for w in ss.worksheets():
        if w.title.strip().lower() == want:
            return True
    if not ALLOW_CREATE_MISSING_SHEETS:
        return False
    w = ss.add_worksheet(title=title, rows=1000, cols=max(10, len(headers)))
    try:
        w.append_row(headers)
    except Exception:
        pass
    return True

def force_create_optional_sheet(title: str, headers: list[str]) -> bool:
    '''Create an optional worksheet even when ALLOW_CREATE_MISSING_SHEETS is disabled.

    This is used to provide a one-click setup UX (e.g., Budgets) so users never see hard errors.
    '''
    ss = get_spreadsheet()
    want = title.strip().lower()
    for w in ss.worksheets():
        if w.title.strip().lower() == want:
            return True
    w = ss.add_worksheet(title=title, rows=1000, cols=max(10, len(headers)))
    try:
        w.append_row(headers)
    except Exception:
        pass
    return True



def write_df_to_sheet(sheet_title: str, df: pd.DataFrame, headers: list[str]) -> None:
    """Overwrite a worksheet with headers + df rows (USER_ENTERED)."""
    ss = get_spreadsheet()
    # locate sheet
    w = None
    for ws_ in ss.worksheets():
        if ws_.title.strip().lower() == sheet_title.strip().lower():
            w = ws_
            break
    if w is None:
        ok = ensure_optional_sheet(sheet_title, headers)
        if not ok:
            raise RuntimeError(f'Missing sheet: {sheet_title}')
        for ws_ in ss.worksheets():
            if ws_.title.strip().lower() == sheet_title.strip().lower():
                w = ws_
                break
    assert w is not None

    # normalize df to headers
    out = df.copy()
    for h in headers:
        if h not in out.columns:
            out[h] = ''
    out = out[headers]

    values = [headers]
    for _, r in out.iterrows():
        row = []
        for h in headers:
            v = r.get(h, '')
            if isinstance(v, dt.date):
                row.append(v.isoformat())
            else:
                row.append('' if v is None else str(v))
        values.append(row)

    gs_retry(lambda: w.clear())
    gs_retry(lambda: w.update(values, value_input_option='USER_ENTERED'))


def make_backup_zip() -> bytes:
    """Create a zip containing CSVs for core + optional sheets."""
    ss = get_spreadsheet()
    sheet_titles = [w.title for w in ss.worksheets()]

    def get_df_for_title(title: str) -> pd.DataFrame:
        try:
            # core tabs through cached_df to preserve transformations
            t = title.strip().lower()
            if t in ('transactions', 'cards', 'rules', 'recurring'):
                return cached_df(t, force=True)
            return read_df_optional(title)
        except Exception:
            return pd.DataFrame()

    buff = io.BytesIO()
    with zipfile.ZipFile(buff, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        for title in sheet_titles:
            df = get_df_for_title(title)
            if df is None:
                continue
            csv_bytes = df.to_csv(index=False).encode('utf-8')
            safe = re.sub(r'[^a-zA-Z0-9_\-]+', '_', title.strip())
            z.writestr(f'{safe}.csv', csv_bytes)
        meta = {
            'created_at': now_iso(),
            'spreadsheet': ss.title,
            'spreadsheet_id': SPREADSHEET_ID,
        }
        z.writestr('backup_meta.json', json.dumps(meta, indent=2).encode('utf-8'))
    return buff.getvalue()


def parse_uploaded_csv(content: bytes) -> pd.DataFrame:
    try:
        s = content.decode('utf-8', errors='ignore')
        return pd.read_csv(io.StringIO(s))
    except Exception:
        # fallback for Excel-ish CSVs
        return pd.read_csv(io.BytesIO(content))


def normalize_merchant_from_notes(notes: str) -> str:
    s = str(notes or '').strip()
    if not s:
        return ''
    # merchant usually is first chunk
    for sep in ('|', '•'):
        if sep in s:
            s = s.split(sep, 1)[0].strip()
            break
    # remove common trailing store markers
    s = re.sub(r'\s*#\s*\d+\b', '', s)
    s = re.sub(r'\b(store|st)\s*\d+\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\b\d{3,}\b', '', s)  # remove long numeric tokens
    s = re.sub(r'\s+', ' ', s).strip()
    # title-case known merchants
    return s


def apply_merchant_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if 'notes' not in out.columns:
        return out
    def _clean(n: str) -> str:
        full = str(n or '')
        merch = normalize_merchant_from_notes(full)
        if not merch:
            return full
        # replace first segment only
        # split by separators, keep remainder
        rest = ''
        for sep in ('|', '•'):
            if sep in full:
                head, tail = full.split(sep, 1)
                rest = sep + tail
                break
        else:
            # try hyphen only when it looks like "MERCHANT - details"
            if ' - ' in full:
                head, tail = full.split(' - ', 1)
                rest = ' - ' + tail
        return merch + rest
    out['notes'] = out['notes'].apply(_clean)
    return out


@ui.page('/budgets')
def budgets_page() -> None:
    if not require_login():
        nav_to('/login')
        return

    def content() -> None:
        with ui.card().classes('my-card p-5'):
            ui.label('Budgets').classes('text-lg font-bold')
            ui.label('Create and manage monthly budgets per category.').style('color: var(--mf-muted)')

            ok = ensure_optional_sheet('budgets', OPTIONAL_SHEETS['budgets'])
            if not ok:
                ui.label('Budgets are not set up yet.').classes('text-sm').style('color: var(--mf-muted)')
                ui.label('Tap Initialize to create the Budgets sheet in Google Sheets.').classes('text-sm').style('color: var(--mf-muted)')
                def _init_budgets():
                    try:
                        force_create_optional_sheet('budgets', OPTIONAL_SHEETS['budgets'])
                        ui.notify('Budgets sheet created.', type='positive')
                        nav_to('/budgets')
                    except Exception as e:
                        ui.notify(f'Could not create Budgets sheet: {e}', type='negative')
                with ui.row().classes('gap-2 mt-3'):
                    ui.button('Initialize Budgets', icon='auto_fix_high', on_click=_init_budgets).props('unelevated')
                    ui.button('Back', icon='arrow_back', on_click=lambda: nav_to('/admin')).props('flat')
                return

            budgets = read_df_optional('budgets')
            if budgets is None or budgets.empty:
                budgets = pd.DataFrame(columns=OPTIONAL_SHEETS['budgets'])

            # Normalize columns
            cols = {str(c).strip().lower(): c for c in budgets.columns}
            c_cat = cols.get('category') or cols.get('cat')
            c_budget = cols.get('budget_monthly') or cols.get('monthly_budget') or cols.get('budget')
            if c_cat and c_cat != 'category':
                budgets['category'] = budgets[c_cat]
            if c_budget and c_budget != 'budget_monthly':
                budgets['budget_monthly'] = budgets[c_budget]
            for h in OPTIONAL_SHEETS['budgets']:
                if h not in budgets.columns:
                    budgets[h] = ''
            budgets = budgets[OPTIONAL_SHEETS['budgets']].copy()

            table = ui.table(
                columns=[
                    {'name': 'category', 'label': 'Category', 'field': 'category', 'align': 'left'},
                    {'name': 'budget_monthly', 'label': 'Monthly Budget', 'field': 'budget_monthly', 'align': 'right'},
                ],
                rows=budgets.to_dict(orient='records'),
                row_key='category',
                selection='single',
            ).classes('w-full')

            cat_in = ui.input('Category').classes('w-full')
            bud_in = ui.number('Monthly budget', value=0.0, format='%.2f').classes('w-full')

            def _refresh() -> None:
                b = read_df_optional('budgets')
                if b is None or b.empty:
                    b = pd.DataFrame(columns=OPTIONAL_SHEETS['budgets'])
                cols = {str(c).strip().lower(): c for c in b.columns}
                c_cat2 = cols.get('category') or cols.get('cat')
                c_budget2 = cols.get('budget_monthly') or cols.get('monthly_budget') or cols.get('budget')
                if c_cat2 and c_cat2 != 'category':
                    b['category'] = b[c_cat2]
                if c_budget2 and c_budget2 != 'budget_monthly':
                    b['budget_monthly'] = b[c_budget2]
                for h in OPTIONAL_SHEETS['budgets']:
                    if h not in b.columns:
                        b[h] = ''
                b = b[OPTIONAL_SHEETS['budgets']].copy()
                table.rows = b.to_dict(orient='records')
                table.update()

            def _load_selected() -> None:
                if not table.selected:
                    ui.notify('Select a row first.', type='warning');
                    return
                r = table.selected[0]
                cat_in.value = str(r.get('category', ''))
                bud_in.value = to_float(r.get('budget_monthly', 0))

            def _save() -> None:
                cat = str(cat_in.value or '').strip()
                if not cat:
                    ui.notify('Category required.', type='warning');
                    return
                bud = float(to_float(bud_in.value))
                # upsert by category
                existing = read_df_optional('budgets')
                if existing is None or existing.empty:
                    existing = pd.DataFrame(columns=OPTIONAL_SHEETS['budgets'])
                cols = {str(c).strip().lower(): c for c in existing.columns}
                c_cat3 = cols.get('category') or cols.get('cat')
                c_budget3 = cols.get('budget_monthly') or cols.get('monthly_budget') or cols.get('budget')
                if c_cat3 and c_cat3 != 'category':
                    existing['category'] = existing[c_cat3]
                if c_budget3 and c_budget3 != 'budget_monthly':
                    existing['budget_monthly'] = existing[c_budget3]
                for h in OPTIONAL_SHEETS['budgets']:
                    if h not in existing.columns:
                        existing[h] = ''
                existing = existing[OPTIONAL_SHEETS['budgets']].copy()
                # update/append
                mask = existing['category'].astype(str).str.strip().str.lower() == cat.lower()
                if mask.any():
                    existing.loc[mask, 'budget_monthly'] = str(bud)
                else:
                    existing = pd.concat([existing, pd.DataFrame([{'category': cat, 'budget_monthly': str(bud)}])], ignore_index=True)
                write_df_to_sheet('budgets', existing, OPTIONAL_SHEETS['budgets'])
                ui.notify('Saved.', type='positive')
                _refresh()

            def _delete() -> None:
                if not table.selected:
                    ui.notify('Select a row first.', type='warning');
                    return
                cat = str(table.selected[0].get('category', '')).strip()
                if not cat:
                    return
                existing = read_df_optional('budgets')
                if existing is None or existing.empty:
                    return
                cols = {str(c).strip().lower(): c for c in existing.columns}
                c_cat3 = cols.get('category') or cols.get('cat')
                if c_cat3 and c_cat3 != 'category':
                    existing['category'] = existing[c_cat3]
                existing = existing.copy()
                existing = existing[existing['category'].astype(str).str.strip().str.lower() != cat.lower()]
                write_df_to_sheet('budgets', existing, OPTIONAL_SHEETS['budgets'])
                ui.notify('Deleted.', type='positive')
                _refresh()

            with ui.row().classes('gap-2 mt-3'):
                ui.button('Load selected', on_click=_load_selected).props('flat')
                ui.button('Save / Upsert', on_click=_save).props('unelevated')
                ui.button('Delete selected', on_click=_delete).props('outline')

    shell(content)


@ui.page('/data_tools')
def data_tools_page() -> None:
    if not require_login():
        nav_to('/login')
        return

    def content() -> None:
        with ui.card().classes('my-card p-5'):
            ui.label('Data Tools').classes('text-lg font-bold')
            ui.label('Import CSV, backup/restore, and merchant cleanup.').style('color: var(--mf-muted)')

        # Backup
        with ui.card().classes('my-card p-5'):
            ui.label('Backup').classes('text-lg font-bold')
            ui.label('Download a zip backup of all sheets as CSV.').style('color: var(--mf-muted)')
            def _download_backup() -> None:
                try:
                    b = make_backup_zip()
                    ui.download(b, filename=f'myfin_backup_{datetime.date.today().isoformat()}.zip')
                except Exception as e:
                    ui.notify(f'Backup failed: {e}', type='negative')
            ui.button('Download backup zip', icon='archive', on_click=_download_backup).props('unelevated')

        # Restore
        with ui.card().classes('my-card p-5'):
            ui.label('Restore').classes('text-lg font-bold')
            ui.label('Upload a backup zip from this app to overwrite sheets.').style('color: var(--mf-muted)')
            confirm = ui.input('Type RESTORE to enable overwrite').classes('w-full')
            upload_zip = ui.upload(label='Upload backup zip', auto_upload=True).props('accept=.zip').classes('w-full')

            async def _on_zip_upload(e):
                if str(confirm.value).strip().upper() != 'RESTORE':
                    ui.notify('Type RESTORE first (safety check).', type='warning');
                    return
                try:
                    content = e.content.read() if hasattr(e, 'content') else None
                    if content is None:
                        # NiceGUI upload event provides `content` bytes on some versions
                        content = e
                    zdata = content if isinstance(content, (bytes, bytearray)) else bytes(content)
                    with zipfile.ZipFile(io.BytesIO(zdata), 'r') as z:
                        names = z.namelist()
                        # overwrite core tabs if present
                        overwritten = []
                        for core in ('transactions', 'cards', 'rules', 'recurring', 'budgets'):
                            fname = f'{core}.csv'
                            # tolerate sanitized names
                            cand = None
                            for n in names:
                                if n.lower() == fname:
                                    cand = n; break
                            if cand is None:
                                continue
                            df = parse_uploaded_csv(z.read(cand))
                            if core in TABS:
                                headers = sheet_headers(core)
                                write_df_to_sheet(core, df, headers)
                                invalidate(core)
                            else:
                                headers = OPTIONAL_SHEETS.get(core, list(df.columns))
                                ensure_optional_sheet(core, headers)
                                write_df_to_sheet(core, df, headers)
                            overwritten.append(core)
                        ui.notify('Restored: ' + ', '.join(overwritten) if overwritten else 'No matching CSVs found in zip.', type='positive')
                except Exception as ex:
                    ui.notify(f'Restore failed: {ex}', type='negative')

            upload_zip.on('upload', _on_zip_upload)

        # CSV import (append)
        with ui.card().classes('my-card p-5'):
            ui.label('Import Transactions CSV').classes('text-lg font-bold')
            ui.label('Append rows from a CSV into Transactions. CSV should include at least date, type, amount.').style('color: var(--mf-muted)')
            upload_csv = ui.upload(label='Upload CSV', auto_upload=True).props('accept=.csv').classes('w-full')

            async def _on_csv_upload(e):
                try:
                    content = e.content.read() if hasattr(e, 'content') else None
                    if content is None:
                        content = e
                    data = content if isinstance(content, (bytes, bytearray)) else bytes(content)
                    df = parse_uploaded_csv(data)
                    if df is None or df.empty:
                        ui.notify('CSV is empty.', type='warning');
                        return
                    # normalize columns
                    colmap = {str(c).strip().lower(): c for c in df.columns}
                    def pick(*names):
                        for n in names:
                            if n in colmap:
                                return colmap[n]
                        return None
                    c_date = pick('date', 'transaction_date')
                    c_type = pick('type', 'transaction_type')
                    c_amount = pick('amount', 'amt', 'value')
                    if not (c_date and c_type and c_amount):
                        ui.notify('CSV must include date, type, amount columns.', type='negative');
                        return
                    # optional
                    c_method = pick('method')
                    c_account = pick('account')
                    c_category = pick('category')
                    c_notes = pick('notes', 'note', 'description')

                    count = 0
                    for _, r in df.iterrows():
                        d = parse_date(r.get(c_date))
                        if not d:
                            continue
                        t = str(r.get(c_type, '')).strip()
                        amt = to_float(r.get(c_amount))
                        if not t:
                            continue
                        txid = str(r.get('id', '')).strip() or str(uuid.uuid4())
                        append_tx(
                            id=txid,
                            date=d.isoformat(),
                            owner='Family',
                            type=t,
                            amount=amt,
                            method=str(r.get(c_method, '')) if c_method else '',
                            account=str(r.get(c_account, '')) if c_account else '',
                            category=str(r.get(c_category, '')) if c_category else '',
                            notes=str(r.get(c_notes, '')) if c_notes else '',
                            is_recurring=False,
                            recurring_id='',
                            created_at=now_iso(),
                        )
                        count += 1
                    invalidate('transactions')
                    ui.notify(f'Imported {count} rows into Transactions.', type='positive')
                except Exception as ex:
                    ui.notify(f'Import failed: {ex}', type='negative')

            upload_csv.on('upload', _on_csv_upload)

        # Merchant cleanup suggestions
        with ui.card().classes('my-card p-5'):
            ui.label('Merchant cleanup').classes('text-lg font-bold')
            ui.label('Normalize merchant text inside Notes (best-effort).').style('color: var(--mf-muted)')
            ui.label('This updates existing Transactions notes by cleaning the first merchant segment.').classes('text-sm').style('color: var(--mf-muted)')

            def _preview_cleanup() -> None:
                tx = cached_df('transactions', force=True)
                if tx.empty or 'notes' not in tx.columns:
                    ui.notify('No notes found.', type='warning');
                    return
                sample = tx[['id', 'date', 'notes']].head(50).copy()
                sample['cleaned_notes'] = apply_merchant_cleanup(sample)['notes']
                rows = sample.to_dict(orient='records')
                with ui.dialog() as d, ui.card().classes('my-card p-4 w-[92vw] max-w-5xl'):
                    ui.label('Preview (first 50 rows)').classes('text-lg font-bold')
                    ui.table(
                        columns=[
                            {'name': 'date', 'label': 'Date', 'field': 'date'},
                            {'name': 'notes', 'label': 'Notes', 'field': 'notes'},
                            {'name': 'cleaned_notes', 'label': 'Cleaned Notes', 'field': 'cleaned_notes'},
                            {'name': 'id', 'label': 'ID', 'field': 'id'},
                        ],
                        rows=rows,
                        row_key='id',
                    ).classes('w-full')
                    ui.button('Close', on_click=d.close).props('flat')
                d.open()

            def _apply_cleanup_all() -> None:
                tx = cached_df('transactions', force=True)
                if tx.empty or 'notes' not in tx.columns:
                    ui.notify('No notes found.', type='warning');
                    return
                cleaned = apply_merchant_cleanup(tx)
                # apply updates row-by-row (safe; but can be slower)
                updated = 0
                for _, r in cleaned.iterrows():
                    rid = str(r.get('id', '')).strip()
                    if not rid:
                        continue
                    new_notes = str(r.get('notes', ''))
                    # compare with original to avoid write spam
                    orig_notes = str(tx.loc[tx['id'].astype(str) == rid, 'notes'].iloc[0]) if (tx['id'].astype(str) == rid).any() else None
                    if orig_notes is not None and new_notes != orig_notes:
                        if update_row_by_id('transactions', 'id', rid, {'notes': new_notes}):
                            updated += 1
                invalidate('transactions')
                ui.notify(f'Updated {updated} notes.', type='positive')

            with ui.row().classes('gap-2 mt-2'):
                ui.button('Preview', icon='preview', on_click=_preview_cleanup).props('outline')
                ui.button('Apply cleanup to all', icon='auto_fix_high', on_click=_apply_cleanup_all).props('unelevated')

    shell(content)


# -----------------------------
# Boot
# -----------------------------
# -----------------------------
# Boot
# -----------------------------
def bootstrap() -> None:
    ensure_tabs()
    # One-time migration: older rows often have the unique id stored in `TxId` while
    # the newer logic edits by `id`. Backfill `id` from `TxId` so Edit works.
    _migrate_transactions_id_column()

bootstrap()

ui.run(
    host="0.0.0.0",
    port=PORT,
    storage_secret=STORAGE_SECRET or "PLEASE_SET_NICEGUI_STORAGE_SECRET",
    title=APP_TITLE,
)
