# ======================================
# FinTrackr App – Phase 4.6A (REAL FIX BUILD)
# Changes vs P4.5: Dashboard hero, Rules selection, OCR toast timeout, richer palette
# ======================================

# ==============================
# FinTrackr App – Phase 4.5 (P4.4 + P4.5 combined)
# Base: Myfin_NICEGUI_VF2_P4_2 (last stable)
# Changes: Budgets setup UX, Transactions table mobile UX, Rules edit, Cards utilization bars,
#          Dashboard pay-period view, Premium login styling
# ==============================

"""
FinTrackr — NiceGUI Stable
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
APP_VERSION = '6.1'


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
# --- NiceGUI Html sanitize compatibility (prevents TypeError on some NiceGUI versions)
try:
    from nicegui.elements.html import Html as _NiceHtml
    import inspect as _inspect
    _sig = _inspect.signature(_NiceHtml.__init__)
    if 'sanitize' in _sig.parameters and _sig.parameters['sanitize'].default is _inspect._empty:
        _orig_init = _NiceHtml.__init__
        def _patched_init(self, content: str = '', *args, sanitize: bool = True, **kwargs):
            return _orig_init(self, content, *args, sanitize=sanitize, **kwargs)
        _NiceHtml.__init__ = _patched_init  # type: ignore
except Exception:
    pass
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse



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

APP_TITLE = "FinTrackr"
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


def _normalize_month_key(m: str) -> str:
    m = (m or "").strip()
    if not m:
        return ""
    if len(m) >= 7 and m[4] == "-":
        return m[:7]
    return m

def list_locked_months() -> set[str]:
    try:
        ensure_tabs()
        df = cached_df("locks")
        if df.empty:
            return set()
        col_m = None
        col_l = None
        for c in df.columns:
            lc = str(c).strip().lower()
            if lc in ("month", "mkey", "month_key"):
                col_m = c
            if lc in ("locked", "is_locked", "lock"):
                col_l = c
        if col_m is None:
            return set()
        locked = set()
        for _, r in df.iterrows():
            mk = _normalize_month_key(str(r.get(col_m, "")))
            if not mk:
                continue
            v = str(r.get(col_l, "true") if col_l else "true").strip().lower()
            if v in ("1", "true", "yes", "y", "locked"):
                locked.add(mk)
        return locked
    except Exception:
        return set()

def is_month_locked(month_key_str: str) -> bool:
    mk = _normalize_month_key(month_key_str)
    return bool(mk) and (mk in list_locked_months())

def set_month_lock(month_key_str: str, locked: bool) -> bool:
    mk = _normalize_month_key(month_key_str)
    if not mk:
        return False
    try:
        ensure_tabs()
        try:
            w = ws("locks")
        except Exception:
            # Create the optional 'locks' sheet on demand (Phase 5.12)
            ss = get_spreadsheet()
            w = ss.add_worksheet(title="locks", rows=500, cols=5)
            w.append_row(TABS.get("locks", ["month", "locked"]))
            invalidate_cache("locks")
            # Rebuild worksheet map
            global _tabs_ready
            _tabs_ready = False
            ensure_tabs()
            w = ws("locks")
        df = cached_df("locks", force=True)

        row_idx = None
        if not df.empty:
            month_col = None
            for c in df.columns:
                if str(c).strip().lower() == "month":
                    month_col = c
                    break
            if month_col:
                for i, r in df.iterrows():
                    if _normalize_month_key(str(r.get(month_col, ""))) == mk:
                        row_idx = int(i) + 2
                        break

        if row_idx is None:
            gs_retry(lambda: w.append_row([mk, "TRUE" if locked else "FALSE"]))
        else:
            gs_retry(lambda: w.update_acell(f"B{row_idx}", "TRUE" if locked else "FALSE"))

        invalidate_cache("locks")
        return True
    except Exception:
        return False

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



def parse_amount(s: Any) -> float:
    """Parse currency/amount-like values into a positive float (best-effort).
    Returns 0.0 when parsing fails."""
    try:
        if s is None:
            return 0.0
        t = str(s).strip()
        if not t or t.lower() == 'nan':
            return 0.0
        # remove common currency markers & thousands separators
        t = t.replace('CAD', '').replace('$', '').replace(',', '').strip()
        # keep digits, minus, dot only
        t = re.sub(r'[^0-9\.-]', '', t)
        if not t or t in {'.','-','-.'}:
            return 0.0
        v = float(t)
        return abs(v)
    except Exception:
        return 0.0

def wide_transactions_to_long(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a 'wide' Transactions sheet into the app's long format.

    If the sheet already contains 'type' and 'amount' columns, this returns df unchanged.
    Otherwise it looks for common FinTrackr columns like:
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
    "locks": ["month", "locked"],
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
        print(f"[FinTrackr] Failed to open spreadsheet. id={bool(SPREADSHEET_ID)} name={SPREADSHEET_NAME!r}: {e}")
        raise

    # Helpful diagnostics in logs so we can confirm the app is reading the correct file.
    try:
        titles = [w.title for w in _ss.worksheets()]
        print(f"[FinTrackr] Opened spreadsheet: '{_ss.title}' | worksheets={titles}")
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
            # 'locks' is optional (introduced in Phase 5.12). If it doesn't exist yet,
            # we treat it as unlocked-by-default and do NOT fail deployment.
            if tab == 'locks' and not ALLOW_CREATE_MISSING_SHEETS:
                continue
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
            + ".\nFix: rename your sheets to match (Transactions, Rules, Cards, Recurring, Budgets, Admin, Locks) "
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
            print(f"[FinTrackr] transactions loaded: rows={len(df)} cols={list(df.columns)} (source cols={before_cols})")

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
# Passkeys / Face ID (Phase 5.6)
# -----------------------------
# Implements WebAuthn (Passkeys) with server-side verification (ES256) and local persistence.
# Notes:
# - Requires HTTPS on Render (you have it).
# - Stores credential public keys in a local JSON file (persisted on Render disk while service is up).
# - Existing username/password login remains as fallback.
#
# Security model:
# - On successful passkey assertion verification, sets app.storage.user["logged_in"]=True.


# Backward-compatible alias used by header buttons
def do_logout() -> None:
    """Logout handler used by UI buttons."""
    logout()

import json
import base64
import hashlib
import secrets
from typing import Dict, Any, Tuple, Optional, List

_PASSKEYS_PATH = os.environ.get("MYFIN_PASSKEYS_PATH", "myfin_passkeys.json")
_RP_ID = os.environ.get("MYFIN_RP_ID")  # optional override (e.g., your custom domain)
_RP_NAME = os.environ.get("MYFIN_RP_NAME", "FinTrackr")
_ORIGIN = os.environ.get("MYFIN_ORIGIN")  # optional override (e.g., https://nishanthajay.com)

def _b64url_enc(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b'=').decode('ascii')

def _b64url_dec(s: str) -> bytes:
    pad = '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode('ascii'))

def _load_passkeys() -> Dict[str, Any]:
    try:
        with open(_PASSKEYS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_passkeys(data: Dict[str, Any]) -> None:
    try:
        with open(_PASSKEYS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

# ---- Minimal CBOR decoder (enough for WebAuthn attestationObject + COSE keys) ----
class _CBOR:
    def __init__(self, b: bytes):
        self.b = b
        self.i = 0

    def _read(self, n: int) -> bytes:
        if self.i + n > len(self.b):
            raise ValueError("CBOR: out of range")
        out = self.b[self.i:self.i+n]
        self.i += n
        return out

    def _read_int(self, ai: int) -> int:
        if ai < 24:
            return ai
        if ai == 24:
            return int.from_bytes(self._read(1), "big")
        if ai == 25:
            return int.from_bytes(self._read(2), "big")
        if ai == 26:
            return int.from_bytes(self._read(4), "big")
        if ai == 27:
            return int.from_bytes(self._read(8), "big")
        raise ValueError("CBOR: indefinite/int unsupported")

    def decode(self) -> Any:
        ib = self._read(1)[0]
        mt = ib >> 5
        ai = ib & 0x1F

        if mt == 0:  # unsigned int
            return self._read_int(ai)
        if mt == 1:  # negative int
            return -1 - self._read_int(ai)
        if mt == 2:  # bytes
            n = self._read_int(ai)
            return self._read(n)
        if mt == 3:  # text
            n = self._read_int(ai)
            return self._read(n).decode("utf-8", errors="strict")
        if mt == 4:  # array
            n = self._read_int(ai)
            return [self.decode() for _ in range(n)]
        if mt == 5:  # map
            n = self._read_int(ai)
            m = {}
            for _ in range(n):
                k = self.decode()
                v = self.decode()
                m[k] = v
            return m
        if mt == 6:  # tag
            _ = self._read_int(ai)
            return self.decode()
        if mt == 7:
            if ai == 20: return False
            if ai == 21: return True
            if ai == 22: return None
        raise ValueError(f"CBOR: unsupported major={mt} ai={ai}")

def _cbor_load(b: bytes) -> Any:
    return _CBOR(b).decode()

def _sha256(b: bytes) -> bytes:
    return hashlib.sha256(b).digest()

def _get_rp_id(request: Request) -> str:
    if _RP_ID:
        return _RP_ID
    host = request.headers.get("host", "")
    # strip port
    return host.split(":")[0] if host else ""

def _get_origin(request: Request) -> str:
    if _ORIGIN:
        return _ORIGIN
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("host") or request.url.hostname or ""
    return f"{proto}://{host}"

def _parse_authenticator_data(ad: bytes) -> Dict[str, Any]:
    if len(ad) < 37:
        raise ValueError("authData too short")
    rp_id_hash = ad[0:32]
    flags = ad[32]
    sign_count = int.from_bytes(ad[33:37], "big")
    rest = ad[37:]
    return {"rpIdHash": rp_id_hash, "flags": flags, "signCount": sign_count, "rest": rest}

def _extract_credential_from_authdata(rest: bytes) -> Tuple[bytes, bytes, int]:
    # attestedCredentialData: AAGUID(16) + credIdLen(2) + credId + COSE key (CBOR)
    if len(rest) < 18:
        raise ValueError("attestedCredentialData too short")
    aaguid = rest[:16]
    cred_len = int.from_bytes(rest[16:18], "big")
    if len(rest) < 18 + cred_len:
        raise ValueError("credId truncated")
    cred_id = rest[18:18+cred_len]
    cose = rest[18+cred_len:]
    return cred_id, cose, cred_len

def _cose_to_public_key(cose_key: Any) -> Tuple[str, bytes]:
    # Support EC2 P-256 (kty=2, crv=1, x=-2, y=-3)
    # cose_key is a dict with int keys.
    if not isinstance(cose_key, dict):
        raise ValueError("COSE key not a map")
    kty = cose_key.get(1)
    alg = cose_key.get(3)
    if kty != 2 or alg != -7:
        raise ValueError("Unsupported key type/alg")
    crv = cose_key.get(-1)
    x = cose_key.get(-2)
    y = cose_key.get(-3)
    if crv != 1 or not isinstance(x, (bytes, bytearray)) or not isinstance(y, (bytes, bytearray)):
        raise ValueError("Unsupported curve or coords")
    # Build uncompressed point 0x04 || X || Y
    pub = b"\x04" + bytes(x) + bytes(y)
    return "ES256", pub

def _verify_es256(pub_uncompressed: bytes, data: bytes, sig: bytes) -> bool:
    # signature from authenticator is DER encoded ECDSA over SHA-256
    try:
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature
        from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
        from cryptography.exceptions import InvalidSignature

        # parse uncompressed point
        if len(pub_uncompressed) != 65 or pub_uncompressed[0] != 0x04:
            return False
        x = int.from_bytes(pub_uncompressed[1:33], "big")
        y = int.from_bytes(pub_uncompressed[33:65], "big")
        pub_numbers = ec.EllipticCurvePublicNumbers(x, y, ec.SECP256R1())
        pub_key = pub_numbers.public_key()

        pub_key.verify(sig, data, ec.ECDSA(hashes.SHA256()))
        return True
    except Exception:
        return False

def _webauthn_challenge() -> bytes:
    return secrets.token_bytes(32)

def _check_origin_and_type(client_data: Dict[str, Any], expected_chal: bytes, expected_origin: str, typ: str) -> None:
    if client_data.get("type") != typ:
        raise ValueError("clientData.type mismatch")
    chal = client_data.get("challenge")
    if not chal:
        raise ValueError("missing challenge")
    if _b64url_dec(chal) != expected_chal:
        raise ValueError("challenge mismatch")
    if client_data.get("origin") != expected_origin:
        raise ValueError("origin mismatch")

# ---- API routes ----

@app.get("/api/passkeys/options/register")
async def passkeys_options_register(request: Request, username: str):
    username = (username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username required")
    rp_id = _get_rp_id(request)
    origin = _get_origin(request)

    chal = _webauthn_challenge()
    app.storage.user["pk_reg_chal"] = _b64url_enc(chal)
    app.storage.user["pk_reg_user"] = username
    app.storage.user["pk_origin"] = origin
    app.storage.user["pk_rp_id"] = rp_id

    # user.id must be stable bytes; we derive from username
    user_id = _sha256(username.encode("utf-8"))[:16]

    opts = {
        "challenge": _b64url_enc(chal),
        "rp": {"name": _RP_NAME, "id": rp_id},
        "user": {"id": _b64url_enc(user_id), "name": username, "displayName": username},
        "pubKeyCredParams": [{"type": "public-key", "alg": -7}],  # ES256
        "timeout": 60000,
        "attestation": "none",
        "authenticatorSelection": {
            "residentKey": "preferred",
            "userVerification": "preferred",
        },
    }
    return JSONResponse(opts)

@app.post("/api/passkeys/verify/register")
async def passkeys_verify_register(request: Request):
    payload = await request.json()
    chal_b64 = app.storage.user.get("pk_reg_chal")
    username = app.storage.user.get("pk_reg_user")
    origin = app.storage.user.get("pk_origin")
    rp_id = app.storage.user.get("pk_rp_id")

    if not chal_b64 or not username or not origin or not rp_id:
        raise HTTPException(status_code=400, detail="missing registration context")

    expected_chal = _b64url_dec(chal_b64)

    try:
        client_data_json = _b64url_dec(payload["response"]["clientDataJSON"])
        client_data = json.loads(client_data_json.decode("utf-8"))
        _check_origin_and_type(client_data, expected_chal, origin, "webauthn.create")

        att_obj = _cbor_load(_b64url_dec(payload["response"]["attestationObject"]))
        auth_data = att_obj.get("authData")
        if not isinstance(auth_data, (bytes, bytearray)):
            raise ValueError("missing authData")

        ad = _parse_authenticator_data(bytes(auth_data))
        if ad["rpIdHash"] != _sha256(rp_id.encode("utf-8")):
            raise ValueError("rpIdHash mismatch")
        # flags: bit 0x40 indicates attestedCredentialData present
        if (ad["flags"] & 0x40) == 0:
            raise ValueError("no attested credential data")

        cred_id, cose_bytes, _ = _extract_credential_from_authdata(ad["rest"])
        cose_key = _cbor_load(cose_bytes)
        alg_name, pub = _cose_to_public_key(cose_key)

        store = _load_passkeys()
        u = store.get(username, {})
        # store one credential per user for now (simple)
        u["credential_id"] = _b64url_enc(cred_id)
        u["public_key_uncompressed"] = _b64url_enc(pub)
        u["alg"] = alg_name
        u["sign_count"] = int(ad["signCount"])
        store[username] = u
        _save_passkeys(store)

        return JSONResponse({"ok": True})
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"register verify failed: {e}")

@app.get("/api/passkeys/options/authenticate")
async def passkeys_options_authenticate(request: Request, username: str):
    username = (username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username required")
    rp_id = _get_rp_id(request)
    origin = _get_origin(request)

    store = _load_passkeys()
    u = store.get(username)
    if not u or not u.get("credential_id"):
        raise HTTPException(status_code=404, detail="no passkey registered")

    chal = _webauthn_challenge()
    app.storage.user["pk_auth_chal"] = _b64url_enc(chal)
    app.storage.user["pk_auth_user"] = username
    app.storage.user["pk_origin"] = origin
    app.storage.user["pk_rp_id"] = rp_id

    opts = {
        "challenge": _b64url_enc(chal),
        "rpId": rp_id,
        "timeout": 60000,
        "userVerification": "preferred",
        "allowCredentials": [{"type": "public-key", "id": u["credential_id"]}],
    }
    return JSONResponse(opts)

@app.post("/api/passkeys/verify/authenticate")
async def passkeys_verify_authenticate(request: Request):
    payload = await request.json()
    chal_b64 = app.storage.user.get("pk_auth_chal")
    username = app.storage.user.get("pk_auth_user")
    origin = app.storage.user.get("pk_origin")
    rp_id = app.storage.user.get("pk_rp_id")

    if not chal_b64 or not username or not origin or not rp_id:
        raise HTTPException(status_code=400, detail="missing auth context")

    expected_chal = _b64url_dec(chal_b64)

    store = _load_passkeys()
    u = store.get(username)
    if not u:
        raise HTTPException(status_code=404, detail="no passkey registered")

    try:
        client_data_json = _b64url_dec(payload["response"]["clientDataJSON"])
        client_data = json.loads(client_data_json.decode("utf-8"))
        _check_origin_and_type(client_data, expected_chal, origin, "webauthn.get")

        auth_data = _b64url_dec(payload["response"]["authenticatorData"])
        sig = _b64url_dec(payload["response"]["signature"])

        ad = _parse_authenticator_data(auth_data)
        if ad["rpIdHash"] != _sha256(rp_id.encode("utf-8")):
            raise ValueError("rpIdHash mismatch")

        # Verify signature over (authenticatorData || SHA256(clientDataJSON))
        signed = auth_data + _sha256(client_data_json)

        pub = _b64url_dec(u["public_key_uncompressed"])
        if not _verify_es256(pub, signed, sig):
            raise ValueError("signature invalid")

        # signCount check (best-effort)
        prev = int(u.get("sign_count") or 0)
        if ad["signCount"] > 0 and ad["signCount"] < prev:
            # possible cloned authenticator; still allow but warn
            pass
        u["sign_count"] = int(ad["signCount"])
        store[username] = u
        _save_passkeys(store)

        # mark user logged in for this session
        app.storage.user["logged_in"] = True

        return JSONResponse({"ok": True})
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"auth verify failed: {e}")



def passkey_login(username: str = "") -> None:
    """Trigger a passkey login flow in the browser and log in on success."""
    u = (username or "").strip() or (os.environ.get('APP_USER') or os.environ.get('APP_USERNAME') or 'admin')
    js = """
    (async () => {{
      try {{
        const u = {json.dumps("%%U%%")};
        const optRes = await fetch(`/api/passkeys/options/authenticate?username=${{encodeURIComponent(u)}}`);
        if(!optRes.ok){{
          const t = await optRes.text();
          throw new Error(t || 'Failed to get auth options');
        }}
        const opts = await optRes.json();
        const b64urlToBuf = (s) => {{
          s = (s||'').replace(/-/g,'+').replace(/_/g,'/');
          while(s.length % 4) s += '=';
          return Uint8Array.from(atob(s), c => c.charCodeAt(0));
        }};
        const bufToB64url = (buf) => btoa(String.fromCharCode(...new Uint8Array(buf)))
            .replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/g,'');

        const publicKey = {{
          challenge: b64urlToBuf(opts.challenge),
          timeout: opts.timeout || 60000,
          rpId: opts.rpId,
          userVerification: opts.userVerification || 'preferred',
        }};
        if (opts.allowCredentials) {{
          publicKey.allowCredentials = opts.allowCredentials.map(c => ({{
            type: c.type,
            id: b64urlToBuf(c.id),
            transports: c.transports || undefined,
          }}));
        }}

        const assertion = await navigator.credentials.get({{ publicKey }});
        const data = {{
          id: assertion.id,
          rawId: bufToB64url(assertion.rawId),
          type: assertion.type,
          response: {{
            clientDataJSON: bufToB64url(assertion.response.clientDataJSON),
            authenticatorData: bufToB64url(assertion.response.authenticatorData),
            signature: bufToB64url(assertion.response.signature),
            userHandle: assertion.response.userHandle ? bufToB64url(assertion.response.userHandle) : null,
          }}
        }};

        const vRes = await fetch(`/api/passkeys/verify/authenticate`, {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify(data)}});
        if(!vRes.ok){{
          const t = await vRes.text();
          throw new Error(t || 'Auth verify failed');
        }}
        // Server sets session -> reload to enter app
        location.href = '/';
      }} catch(e) {{
        alert(`Passkey login failed: ${{e.message||e}}`);
      }}
    }})();
    """.replace("%%U%%", u)
    ui.run_javascript(js)


# -----------------------------
# UI Theme
# -----------------------------
BANK_CSS = r"""
:root {
  --mf-bg: #070A12;
  --mf-bg-2: #0B1020;
  --mf-surface: rgba(255,255,255,0.05);
  --mf-surface-2: rgba(255,255,255,0.08);
  --mf-menu-bg: rgba(255,255,255,0.09);
  --mf-border: rgba(255,255,255,0.12);
  --mf-text: rgba(255,255,255,0.92);
  --mf-muted: rgba(255,255,255,0.62);
  --mf-accent: #5B8CFF;
  --mf-accent2: #46E6A6;
  --mf-good: #22c55e;
  --mf-bad: #ef4444;
  --mf-warn: #fbbf24;
  --mf-g1: rgba(91,140,255,0.22);
  --mf-g2: rgba(70,230,166,0.12);
  --mf-card-top: rgba(255,255,255,0.10);
  --mf-card-bottom: rgba(255,255,255,0.05);
  --mf-card-border: rgba(255,255,255,0.14);
}


html.mf-light {
  --mf-bg: #F4F6FB;
  --mf-bg-2: #EEF2FA;
  --mf-surface: rgba(0,0,0,0.04);
  --mf-surface-2: rgba(0,0,0,0.06);
  --mf-border: rgba(0,0,0,0.10);
  --mf-text: rgba(10,12,20,0.92);
  --mf-muted: rgba(10,12,20,0.62);
  --mf-menu-bg: rgba(255,255,255,0.92);
  --mf-g1: rgba(91,140,255,0.16);
  --mf-g2: rgba(70,230,166,0.10);
  --mf-card-top: rgba(255,255,255,0.88);
  --mf-card-bottom: rgba(255,255,255,0.72);
  --mf-card-border: rgba(0,0,0,0.10);
  --mf-card-shadow: 0 20px 55px rgba(0,0,0,0.14);
}
body, .q-layout, .q-page {
  background: radial-gradient(1200px 700px at 18% 12%, var(--mf-g1), transparent 60%),
              radial-gradient(900px 600px at 82% 18%, var(--mf-g2), transparent 58%),
              radial-gradient(900px 600px at 80% 20%, rgba(34,197,94,0.12), transparent 55%),
              radial-gradient(900px 600px at 40% 90%, rgba(251,191,36,0.08), transparent 55%),
              var(--mf-bg) !important;
  color: var(--mf-text) !important;
}

.my-card {
  background: var(--mf-card-bg, linear-gradient(180deg, var(--mf-card-top), var(--mf-card-bottom))) !important;
  border: 1px solid var(--mf-card-border) !important;
  border-radius: 24px !important;
  box-shadow:
    var(--mf-card-shadow, 0 20px 55px rgba(0,0,0,0.42)),
    inset 0 1px 0 rgba(255,255,255,0.12);
  backdrop-filter: blur(16px);
  -webkit-backdrop-filter: blur(16px);
  overflow: hidden;
  position: relative;
}
.my-card::before{
  content:"";
  position:absolute; inset:-2px;
  background:
    radial-gradient(500px 220px at 20% 0%, rgba(255,255,255,0.14), transparent 60%),
    radial-gradient(420px 240px at 80% 20%, rgba(91,140,255,0.18), transparent 65%),
    radial-gradient(420px 240px at 70% 90%, rgba(70,230,166,0.10), transparent 70%);
  pointer-events:none;
  opacity:0.9;
}

/* 5.5: Issuer-tinted bank glass for Cards tiles */
/* 5.10: Stronger issuer gradients (no logos; just bolder brand-matching color) */
.my-card.mf-issuer-ct{
  background:
    linear-gradient(135deg,
      rgba(239,68,68,0.42) 0%,
      rgba(239,68,68,0.18) 26%,
      rgba(0,0,0,0.00) 62%),
    linear-gradient(180deg, var(--mf-card-top), var(--mf-card-bottom)) !important;
  border-color: rgba(239,68,68,0.22) !important;
}
.my-card.mf-issuer-rbc{
  background:
    linear-gradient(135deg,
      rgba(59,130,246,0.48) 0%,
      rgba(59,130,246,0.18) 30%,
      rgba(0,0,0,0.00) 64%),
    linear-gradient(180deg, var(--mf-card-top), var(--mf-card-bottom)) !important;
  border-color: rgba(59,130,246,0.22) !important;
}
.my-card.mf-issuer-loc{
  background:
    linear-gradient(135deg,
      rgba(148,163,184,0.40) 0%,
      rgba(148,163,184,0.16) 30%,
      rgba(0,0,0,0.00) 66%),
    linear-gradient(180deg, var(--mf-card-top), var(--mf-card-bottom)) !important;
  border-color: rgba(148,163,184,0.18) !important;
}

.my-card.mf-issuer-ct::before{
  background:
    radial-gradient(520px 240px at 18% 0%, rgba(255,255,255,0.18), transparent 62%),
    radial-gradient(520px 260px at 82% 18%, rgba(251,191,36,0.14), transparent 68%),
    radial-gradient(520px 260px at 70% 92%, rgba(148,163,184,0.14), transparent 72%);
}
.my-card.mf-issuer-rbc::before{
  background:
    radial-gradient(520px 240px at 18% 0%, rgba(255,255,255,0.18), transparent 62%),
    radial-gradient(520px 260px at 82% 18%, rgba(59,130,246,0.20), transparent 68%),
    radial-gradient(520px 260px at 70% 92%, rgba(14,165,233,0.12), transparent 72%);
}
.my-card.mf-issuer-loc::before{
  background:
    radial-gradient(520px 240px at 18% 0%, rgba(255,255,255,0.18), transparent 62%),
    radial-gradient(520px 260px at 82% 18%, rgba(99,102,241,0.18), transparent 68%),
    radial-gradient(520px 260px at 70% 92%, rgba(16,185,129,0.10), transparent 72%);
}
.my-card > * { position: relative; }
.my-card:hover{
  transform: translateY(-1px);
  box-shadow:
    0 26px 70px rgba(0,0,0,0.48),
    inset 0 1px 0 rgba(255,255,255,0.14);
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
  background: var(--mf-menu-bg, var(--mf-surface-2)) !important;
  color: var(--mf-text) !important;
}

/* Make dropdowns readable on light themes (Safari/Quasar menus) */
.q-menu { 
  border: 1px solid var(--mf-border) !important;
  backdrop-filter: blur(18px);
  -webkit-backdrop-filter: blur(18px);
}

/* Theme-menu portal fix: force q-menu to light surface when html has mf-light */
html.mf-light .q-menu,
html.mf-light .q-menu.mf-menu-light {
  background: var(--mf-menu-bg) !important;
  color: var(--mf-text) !important;
}
html.mf-light .q-menu .q-item,
html.mf-light .q-menu .q-item__label,
html.mf-light .q-menu .q-item__section {
  color: var(--mf-text) !important;
}
html.mf-light .q-menu .q-item:hover {
  background: rgba(17,24,39,0.08) !important;
}
.q-item, .q-item__label, .q-item__section, .q-field__native, .q-field__label, .q-field__prefix, .q-field__suffix {
  color: var(--mf-text) !important;
}
.q-item--active, .q-item--active .q-item__label { 
  color: var(--mf-text) !important;
}


/* Light theme: Quasar menus can render with dark inline defaults on iOS Safari */
html.mf-light .q-menu,
html.mf-light .q-menu .q-list {
  background: var(--mf-menu-bg) !important;
  color: var(--mf-text) !important;
}
html.mf-light .q-menu .q-item,
html.mf-light .q-menu .q-item__label,
html.mf-light .q-menu .q-item__section {
  color: var(--mf-text) !important;
}
html.mf-light .q-menu .q-item:hover,
html.mf-light .q-menu .q-item.q-manual-focusable--focused {
  background: rgba(91,140,255,0.18) !important;
}


/* iOS Safari: allow dialogs to scroll fully (reach Save/Cancel) */
.q-dialog__inner > div {
  max-height: min(92vh, 92dvh);
  overflow-y: auto;
  -webkit-overflow-scrolling: touch;
}


html.mf-light .q-menu.q-dark,
html.mf-light .q-menu.q-dark .q-list {
  background: var(--mf-menu-bg) !important;
  color: var(--mf-text) !important;
}
html.mf-light .q-menu.q-dark .q-item,
html.mf-light .q-menu.q-dark .q-item__label,
html.mf-light .q-menu.q-dark .q-item__section {
  color: var(--mf-text) !important;
}

/* ---- Phase 5.14.5 HF4: iOS/mobile QSelect uses a dialog/bottom-sheet, not a q-menu.
   Force light-surface + readable text in those overlays when using a light theme. ---- */
html.mf-light .q-dialog__inner > div,
html.mf-light .q-bottom-sheet,
html.mf-light .q-select__dialog {
  background: var(--mf-menu-bg, #FFFFFF) !important;
  color: var(--mf-text, rgba(10,12,20,0.92)) !important;
  border: 1px solid var(--mf-border, rgba(0,0,0,0.10)) !important;
}

html.mf-light .q-dialog__inner > div .q-item,
html.mf-light .q-dialog__inner > div .q-item__label,
html.mf-light .q-dialog__inner > div .q-item__section,
html.mf-light .q-bottom-sheet .q-item,
html.mf-light .q-bottom-sheet .q-item__label,
html.mf-light .q-bottom-sheet .q-item__section,
html.mf-light .q-select__dialog .q-item,
html.mf-light .q-select__dialog .q-item__label,
html.mf-light .q-select__dialog .q-item__section {
  color: var(--mf-text, rgba(10,12,20,0.92)) !important;
}

html.mf-light .q-dialog__inner > div .q-item:hover,
html.mf-light .q-bottom-sheet .q-item:hover,
html.mf-light .q-select__dialog .q-item:hover,
html.mf-light .q-dialog__inner > div .q-item.q-manual-focusable--focused,
html.mf-light .q-bottom-sheet .q-item.q-manual-focusable--focused,
html.mf-light .q-select__dialog .q-item.q-manual-focusable--focused {
  background: rgba(17,24,39,0.06) !important;
}


/* Budgets: never show raw decimal label inside progress bars */
.mf-budget-bar .q-linear-progress__label,
.mf-budget-bar .q-linear-progress__label--internal,
.mf-budget-bar .q-linear-progress__label--external {
  display: none !important;
}
.q-item:hover, .q-item.q-manual-focusable--focused {
  background: rgba(120,160,255,0.14) !important;
}
.q-item:hover .q-item__label {
  color: var(--mf-text) !important;
}



/* 5.12.3: Ensure form fields and icons are readable in both themes */
.q-field__control, .q-field__native, .q-field__label, .q-field__marginal, .q-select__dropdown-icon,
.q-field__append, .q-field__prepend, .q-icon, .q-btn, .q-btn__content, .q-btn__content * {
  color: var(--mf-text) !important;
}
.q-field--filled .q-field__control, .q-field--outlined .q-field__control {
  background: rgba(0,0,0,0.00) !important;
}
html.mf-light .q-field--filled .q-field__control,
html.mf-light .q-field--outlined .q-field__control {
  background: rgba(255,255,255,0.60) !important;
}
html.mf-light .my-card::before { opacity: 0.45; }

/* Progress labels (Budgets) */
.q-linear-progress__label {
  color: var(--mf-text) !important;
  font-weight: 700;
}

/* Select field readability + active option highlight */
.q-field__control, .q-field__native, .q-select__dropdown-icon {
  color: var(--mf-text) !important;
}
.q-field__control {
  background: rgba(255,255,255,0.06);
}
html.mf-light .q-field__control {
  background: rgba(0,0,0,0.03) !important;
}
.q-item--active {
  background: rgba(91,140,255,0.16) !important;
}



/* Light-mode safety: prevent Quasar 'dark' surfaces from forcing dark menus/dialogs */
html.mf-light .q-menu--dark,
html.mf-light .q-dialog__inner--minimized > div.q-card,
html.mf-light .q-dialog__inner > div.q-card,
html.mf-light .q-card--dark {
  background: linear-gradient(180deg, var(--mf-card-top), var(--mf-card-bottom)) !important;
  color: var(--mf-text) !important;
}
html.mf-light .q-item,
html.mf-light .q-item__label,
html.mf-light .q-field__native,
html.mf-light .q-field__label,
html.mf-light .q-field__marginal,
html.mf-light .q-btn__content {
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


/* ================================
   Phase 5.2 Shell Layout (bank-style)
   ================================ */
.mf-shell { display: flex; min-height: 100vh; width: 100%; }
.mf-rail {
  width: 92px;
  position: fixed;
  left: 18px;
  top: 18px;
  height: calc(100vh - 36px);
  padding: 0;
  z-index: 50;
  transform: translateX(-130%);
  transition: transform 180ms ease;
}
.mf-nav-open .mf-rail { transform: translateX(0); }

.mf-backdrop{
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.55);
  backdrop-filter: blur(2px);
  -webkit-backdrop-filter: blur(2px);
  z-index: 40;
  display: none;
}
.mf-nav-open .mf-backdrop{ display:block; }
.mf-rail-card{
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: 14px;
  border: 1px solid var(--mf-border);
  background: var(--mf-surface);
  backdrop-filter: blur(14px);
  -webkit-backdrop-filter: blur(14px);
  border-radius: 18px;
  box-shadow: 0 8px 26px rgba(0,0,0,0.35);
  padding: 14px;
}
.mf-brand{
  height:44px;
  display:flex;
  align-items:center;
  justify-content:center;
  border-radius: 14px;
  border: 1px solid var(--mf-border);
  background: rgba(255,255,255,0.04);
  font-weight: 900;
  letter-spacing: 0.8px;
  user-select: none;
}
.mf-navbtn .q-btn__content{ flex-direction: column !important; gap: 6px; }
.mf-navbtn{
  width: 100%;
  min-height: 58px;
  border-radius: 14px !important;
  border: 1px solid transparent !important;
  text-transform: none !important;
}
.mf-navbtn.is-active{
  background: var(--mf-g1) !important;
  border: 1px solid rgba(255,255,255,0.18) !important;
}
.mf-navbtn .q-btn__content span { font-size: 11px; opacity: 0.78; }

.mf-main { flex: 1; padding: 38px; }
.mf-header{
  height: 64px;
  display:flex;
  align-items:center;
  justify-content: space-between;
  gap: 18px;
  max-width: 1180px;
  margin: 0 auto 26px auto;
}
.mf-title .t1 { font-size: 18px; font-weight: 900; }
.mf-title .t2 { font-size: 12px; color: var(--mf-muted); }
.mf-canvas{
  max-width: 1180px;
  margin: 0 auto;
  display:flex;
  flex-direction: column;
  gap: 26px;
}

@media (max-width: 900px){
  .mf-rail{ padding: 10px; }
  .mf-main{ padding: 18px 10px; }
  .mf-navbtn .q-btn__content span { display:none; }
  .mf-navbtn { min-height: 46px; }
}




/* 5.4.1: Cards widgets full-width on mobile */
.mf-card-widget { width: 100%; max-width: 100%; }

.mf-card-emph{
  border: 1px solid rgba(91,140,255,0.22) !important;
  box-shadow:
    0 24px 70px rgba(0,0,0,0.50),
    0 0 0 1px rgba(91,140,255,0.12) inset,
    inset 0 1px 0 rgba(255,255,255,0.14) !important;
}


@media (max-width: 600px){
  .mf-card-widget { width: 100% !important; }
}
/* 5.2.6: Responsive theme control + prevent menu clipping */
.mf-header { overflow: visible !important; }
.mf-canvas { overflow: visible !important; }
.mf-hide-mobile { display: block; }
.mf-show-mobile { display: none; }
@media (max-width: 600px){
  .mf-hide-mobile { display: none !important; }
  .mf-show-mobile { display: inline-flex !important; }
  /* reduce header crowding */
  .mf-header { height: auto !important; padding-top: 10px !important; padding-bottom: 10px !important; }
}
.q-menu { z-index: 99999 !important; }

/* Mobile full-bleed adjustments (5.2.2) */
@media (max-width: 600px){
  .mf-header, .mf-canvas { max-width: none !important; width: 100% !important; margin: 0 !important; }
  .mf-main { padding-left: 0 !important; padding-right: 0 !important; }
  .mf-canvas { padding-left: 0 !important; padding-right: 0 !important; }
  .mf-header { padding-left: 10px !important; padding-right: 10px !important; }
}

/* Stronger issuer tint + variants */
.my-card.mf-issuer-ct { border-color: rgba(251,191,36,0.35) !important; }
.my-card.mf-issuer-rbc { border-color: rgba(59,130,246,0.35) !important; }
.my-card.mf-issuer-loc { border-color: rgba(16,185,129,0.30) !important; }

.my-card.mf-ct-black::after{
  content:"";
  position:absolute; left:-60px; top:-60px;
  width:220px; height:220px;
  background: radial-gradient(circle at 40% 40%, rgba(0,0,0,0.45), transparent 60%),
              radial-gradient(circle at 70% 70%, rgba(251,191,36,0.18), transparent 62%);
  transform: rotate(20deg);
  opacity:0.9;
  pointer-events:none;
}
.my-card.mf-ct-grey::after{
  content:"";
  position:absolute; left:-60px; top:-60px;
  width:220px; height:220px;
  background: radial-gradient(circle at 40% 40%, rgba(148,163,184,0.22), transparent 60%),
              radial-gradient(circle at 70% 70%, rgba(251,191,36,0.14), transparent 62%);
  transform: rotate(20deg);
  opacity:0.9;
  pointer-events:none;
}
.my-card.mf-rbc-blue::after{
  content:"";
  position:absolute; right:-80px; top:-80px;
  width:260px; height:260px;
  background: radial-gradient(circle at 35% 35%, rgba(59,130,246,0.22), transparent 60%),
              radial-gradient(circle at 70% 70%, rgba(14,165,233,0.14), transparent 65%);
  transform: rotate(-14deg);
  opacity:0.9;
  pointer-events:none;
}

/* Light theme: ensure dropdown/list text stays readable */
.q-menu, .q-item, .q-item__label { color: var(--mf-text) !important; }
.q-field__native, .q-field__input { color: var(--mf-text) !important; }

/* Ensure dropdown/menu option text stays readable across light/dark themes */
.q-menu, .q-menu .q-item__label, .q-menu .q-item__section {
  color: var(--mf-text) !important;
}

/* iOS: smoother scrolling inside dialogs */
.mf-scroll {
  -webkit-overflow-scrolling: touch;
}


/* --- 5.12.4 fixes: dropdown + dialog + progress label --- */

/* Make selects & inputs readable in BOTH themes */
.q-field__native, .q-field__input, .q-field__label, .q-field__bottom, .q-field__messages,
.q-select__dropdown-icon, .q-field__append .q-icon, .q-field__prepend .q-icon {
  color: var(--mf-text) !important;
}
.q-field__control, .q-field__marginal {
  color: var(--mf-text) !important;
}

/* Dropdown menu readability + highlight */
.q-menu, .q-menu .q-list {
  background: var(--mf-menu-bg) !important;
  backdrop-filter: blur(14px);
  border: 1px solid var(--mf-border) !important;
}
.q-item, .q-item .q-item__label, .q-item .q-item__section {
  color: var(--mf-text) !important;
}
.q-item--active, .q-item--active .q-item__label {
  color: var(--mf-text) !important;
  background: rgba(120,160,255,0.18) !important;
}
.q-item:hover, .q-item.q-manual-focusable--focused {
  background: rgba(120,160,255,0.14) !important;
}

/* Dialog cards must follow theme surface (fix light theme dark dialog) */
.q-dialog .my-card, .q-dialog .q-card.my-card {
  background: linear-gradient(180deg, var(--mf-card-top), var(--mf-card-bottom)) !important;
  border: 1px solid var(--mf-card-border) !important;
  color: var(--mf-text) !important;
}

/* Remove any numeric label rendered inside progress bars */
.q-linear-progress__label { display: none !important; }
"""
ui.add_head_html("<style>" + BANK_CSS + """
/* Budget progress: hide numeric overlay label */
.mf-budget .q-linear-progress__label{display:none !important;}
/* Light theme: force dropdown menus to render light */
html.mf-light .q-menu, html.mf-light .q-menu.q-dark{background: var(--mf-menu-bg) !important; color: var(--mf-text) !important;}
html.mf-light .q-menu .q-list{background: var(--mf-menu-bg) !important; color: var(--mf-text) !important;}
html.mf-light .q-menu .q-item__label{color: var(--mf-text) !important;}
html.mf-light .q-item:hover{background: rgba(120,160,255,0.14) !important;}

/* ==============================
   Phase 6.1 – Dashboard-only UI polish (scoped)
   ============================== */
html.mf-light .dash-scope .q-card,
html.mf-light .dash-scope .my-card{
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  background: rgba(255,255,255,0.78) !important;
  border: 1px solid rgba(0,0,0,0.06) !important;
  border-radius: 14px !important;
  box-shadow: 0 10px 24px rgba(0,0,0,0.06) !important;
}
html.mf-dark .dash-scope .q-card,
html.mf-dark .dash-scope .my-card{
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  background: rgba(18,22,28,0.72) !important;
  border: 1px solid rgba(255,255,255,0.08) !important;
  border-radius: 14px !important;
  box-shadow: 0 12px 28px rgba(0,0,0,0.38) !important;
}
.dash-scope .q-card, .dash-scope .my-card{ transition: transform 140ms ease, box-shadow 140ms ease; }
.dash-scope .q-card:hover, .dash-scope .my-card:hover{ transform: translateY(-1px); }
</style>""", shared=True)

ui.add_head_html(r'''
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg%20xmlns%3D%22http%3A//www.w3.org/2000/svg%22%20viewBox%3D%220%200%2064%2064%22%3E%0A%3Cdefs%3E%3ClinearGradient%20id%3D%22g%22%20x1%3D%220%22%20y1%3D%220%22%20x2%3D%221%22%20y2%3D%221%22%3E%0A%3Cstop%20offset%3D%220%22%20stop-color%3D%22%235B8CFF%22/%3E%3Cstop%20offset%3D%221%22%20stop-color%3D%22%2346E6A6%22/%3E%0A%3C/linearGradient%3E%3C/defs%3E%0A%3Crect%20width%3D%2264%22%20height%3D%2264%22%20rx%3D%2214%22%20fill%3D%22%23070A12%22/%3E%0A%3Cpath%20d%3D%22M18%2044V20h10c9%200%2016%205%2016%2012s-7%2012-16%2012H18zm6-6h4c6%200%2010-3%2010-6s-4-6-10-6h-4v12z%22%20fill%3D%22url%28%23g%29%22/%3E%0A%3C/svg%3E">
<style>
/* Remove ugly yellow background on browser autofill (Safari/Chrome) */
input:-webkit-autofill,
textarea:-webkit-autofill,
select:-webkit-autofill{
  -webkit-text-fill-color: var(--mf-text) !important;
  box-shadow: 0 0 0px 1000px var(--mf-bg-2) inset !important;
  transition: background-color 9999s ease-in-out 0s;
  caret-color: var(--mf-text) !important;
  background-color: var(--mf-bg-2) !important;
}
</style>
''', shared=True)

ui.add_head_html(
    """<script>
(function(){
  window.__mfBooting = true;

  const THEMES = {
    "Midnight Blue": {
      "--mf-bg":"#070A12", "--mf-bg-2":"#0B1020",
      "--mf-surface":"rgba(255,255,255,0.06)", "--mf-surface-2":"rgba(255,255,255,0.09)",
      "--mf-border":"rgba(255,255,255,0.10)", "--mf-text":"rgba(255,255,255,0.92)", "--mf-muted":"rgba(255,255,255,0.62)",
      "--mf-accent":"#5B8CFF", "--mf-accent2":"#46E6A6",
      "--mf-g1":"rgba(91,140,255,0.22)", "--mf-g2":"rgba(70,230,166,0.12)",
      "--mf-card-top":"rgba(255,255,255,0.10)", "--mf-card-bottom":"rgba(255,255,255,0.05)", "--mf-card-border":"rgba(255,255,255,0.14)"
    },
    "Emerald Gold": {
      "--mf-bg":"#050B0A", "--mf-bg-2":"#071613",
      "--mf-surface":"rgba(255,255,255,0.055)", "--mf-surface-2":"rgba(255,255,255,0.085)",
      "--mf-border":"rgba(255,255,255,0.11)", "--mf-text":"rgba(255,255,255,0.92)", "--mf-muted":"rgba(255,255,255,0.62)",
      "--mf-accent":"#22C55E", "--mf-accent2":"#FBBF24",
      "--mf-g1":"rgba(34,197,94,0.20)", "--mf-g2":"rgba(251,191,36,0.12)",
      "--mf-card-top":"rgba(255,255,255,0.10)", "--mf-card-bottom":"rgba(255,255,255,0.05)", "--mf-card-border":"rgba(255,255,255,0.14)"
    },
    "Graphite Rose": {
      "--mf-bg":"#07070A", "--mf-bg-2":"#0E0A12",
      "--mf-surface":"rgba(255,255,255,0.055)", "--mf-surface-2":"rgba(255,255,255,0.085)",
      "--mf-border":"rgba(255,255,255,0.11)", "--mf-text":"rgba(255,255,255,0.92)", "--mf-muted":"rgba(255,255,255,0.62)",
      "--mf-accent":"#F472B6", "--mf-accent2":"#A78BFA",
      "--mf-g1":"rgba(244,114,182,0.16)", "--mf-g2":"rgba(167,139,250,0.12)",
      "--mf-card-top":"rgba(255,255,255,0.10)", "--mf-card-bottom":"rgba(255,255,255,0.05)", "--mf-card-border":"rgba(255,255,255,0.14)"
    },

    // Light bank themes
    "Arctic Light": {
      "--mf-bg":"#F5F7FB", "--mf-bg-2":"#EEF2FF",
      "--mf-surface":"rgba(17,24,39,0.04)", "--mf-surface-2":"rgba(17,24,39,0.06)",
      "--mf-border":"rgba(17,24,39,0.10)", "--mf-text":"rgba(17,24,39,0.92)", "--mf-muted":"rgba(17,24,39,0.60)",
      "--mf-accent":"#1D4ED8", "--mf-accent2":"#0EA5E9",
      "--mf-g1":"rgba(29,78,216,0.10)", "--mf-g2":"rgba(14,165,233,0.08)",
      "--mf-card-top":"rgba(255,255,255,0.88)", "--mf-card-bottom":"rgba(255,255,255,0.72)", "--mf-card-border":"rgba(17,24,39,0.10)"
    },
    "Mint Light": {
      "--mf-bg":"#F2FBF7", "--mf-bg-2":"#E7F7F0",
      "--mf-surface":"rgba(17,24,39,0.04)", "--mf-surface-2":"rgba(17,24,39,0.06)",
      "--mf-border":"rgba(17,24,39,0.10)", "--mf-text":"rgba(17,24,39,0.92)", "--mf-muted":"rgba(17,24,39,0.60)",
      "--mf-accent":"#059669", "--mf-accent2":"#10B981",
      "--mf-g1":"rgba(5,150,105,0.10)", "--mf-g2":"rgba(16,185,129,0.08)",
      "--mf-card-top":"rgba(255,255,255,0.90)", "--mf-card-bottom":"rgba(255,255,255,0.74)", "--mf-card-border":"rgba(17,24,39,0.10)"
    },

    "Rose Light": {
      "--mf-bg":"#FFF5F7", "--mf-bg-2":"#FFE4EA",
      "--mf-surface":"rgba(17,24,39,0.04)", "--mf-surface-2":"rgba(17,24,39,0.06)",
      "--mf-border":"rgba(17,24,39,0.10)", "--mf-text":"rgba(17,24,39,0.92)", "--mf-muted":"rgba(17,24,39,0.60)",
      "--mf-accent":"#DB2777", "--mf-accent2":"#F43F5E",
      "--mf-g1":"rgba(219,39,119,0.10)", "--mf-g2":"rgba(244,63,94,0.08)",
      "--mf-card-top":"rgba(255,255,255,0.90)", "--mf-card-bottom":"rgba(255,255,255,0.74)", "--mf-card-border":"rgba(17,24,39,0.10)"
    },
    "Sand Gold": {
      "--mf-bg":"#FBF7EF", "--mf-bg-2":"#F7EEDD",
      "--mf-surface":"rgba(17,24,39,0.04)", "--mf-surface-2":"rgba(17,24,39,0.06)",
      "--mf-border":"rgba(17,24,39,0.10)", "--mf-text":"rgba(17,24,39,0.92)", "--mf-muted":"rgba(17,24,39,0.60)",
      "--mf-accent":"#B45309", "--mf-accent2":"#D97706",
      "--mf-g1":"rgba(180,83,9,0.08)", "--mf-g2":"rgba(217,119,6,0.08)",
      "--mf-card-top":"rgba(255,255,255,0.88)", "--mf-card-bottom":"rgba(255,255,255,0.72)", "--mf-card-border":"rgba(17,24,39,0.10)"
    }
    ,
    "Slate Light": {
      "--mf-bg":"#F6F7FA", "--mf-bg-2":"#E9EEF8",
      "--mf-surface":"rgba(17,24,39,0.04)", "--mf-surface-2":"rgba(17,24,39,0.06)",
      "--mf-border":"rgba(17,24,39,0.10)", "--mf-text":"rgba(17,24,39,0.92)", "--mf-muted":"rgba(17,24,39,0.60)",
      "--mf-accent":"#334155", "--mf-accent2":"#2563EB",
      "--mf-g1":"rgba(51,65,85,0.10)", "--mf-g2":"rgba(37,99,235,0.08)",
      "--mf-card-top":"rgba(255,255,255,0.90)", "--mf-card-bottom":"rgba(255,255,255,0.74)", "--mf-card-border":"rgba(17,24,39,0.10)"
    }
  };

  window.
  window.mfFixPlotlyText = function(){
    try{
      const cs = getComputedStyle(document.documentElement);
      const text = (cs.getPropertyValue('--mf-text') || '#111').trim();
      const muted = (cs.getPropertyValue('--mf-muted') || 'rgba(17,24,39,0.65)').trim();
      const relayout = {
        'font.color': text,
        'legend.font.color': text,
        'title.font.color': text,
        'xaxis.tickfont.color': text,
        'yaxis.tickfont.color': text,
        'xaxis.title.font.color': text,
        'yaxis.title.font.color': text,
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
      };
      document.querySelectorAll('.js-plotly-plot').forEach(g=>{
        try{ Plotly && Plotly.relayout(g, relayout); }catch(e){}
      });
    }catch(e){}
  };

  // WebAuthn register must run in the same user gesture; call this directly from an onclick handler
      window.mfPasskeyRegister = async function(username) {
      try {
        username = (username || '').trim();
        if (!username) {
          alert('Please enter a username for passkey.');
          return;
        }
        if (!('credentials' in navigator) || !window.PublicKeyCredential) {
          alert('Passkeys are not supported in this browser/device.');
          return;
        }
        const toast = (msg) => { try { (window.mfToast ? window.mfToast(msg) : console.log(msg)); } catch(e) {} };

        const bufToB64Url = (buf) => btoa(String.fromCharCode(...new Uint8Array(buf)))
          .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
        const b64UrlToBuf = (b64url) => {
          const pad = '='.repeat((4 - (b64url.length % 4)) % 4);
          const b64 = (b64url + pad).replace(/-/g, '+').replace(/_/g, '/');
          const str = atob(b64);
          const bytes = new Uint8Array(str.length);
          for (let i = 0; i < str.length; i++) bytes[i] = str.charCodeAt(i);
          return bytes.buffer;
        };

        toast('Opening Face ID / Passkey prompt...');
        const optRes = await fetch('/api/passkeys/options/register?username=' + encodeURIComponent(username));
        if (!optRes.ok) throw new Error('Could not start passkey registration.');
        const options = await optRes.json();

        // Convert base64url fields to ArrayBuffer as required by WebAuthn
        options.challenge = b64UrlToBuf(options.challenge);
        if (options.user && options.user.id) options.user.id = b64UrlToBuf(options.user.id);
        if (options.excludeCredentials) {
          options.excludeCredentials = options.excludeCredentials.map(c => ({...c, id: b64UrlToBuf(c.id)}));
        }

        const cred = await navigator.credentials.create({ publicKey: options });
        if (!cred) throw new Error('Passkey creation was cancelled.');

        const payload = {
          id: cred.id,
          rawId: bufToB64Url(cred.rawId),
          type: cred.type,
          response: {
            clientDataJSON: bufToB64Url(cred.response.clientDataJSON),
            attestationObject: bufToB64Url(cred.response.attestationObject),
          }
        };
        if (cred.response.getTransports) payload.response.transports = cred.response.getTransports();

        const verRes = await fetch('/api/passkeys/verify/register?username=' + encodeURIComponent(username), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        if (!verRes.ok) {
          const t = await verRes.text();
          throw new Error('Passkey registration failed: ' + (t || verRes.status));
        }
        toast('Passkey registered ✓');
        alert('Passkey registered successfully for ' + username);
      } catch (e) {
        console.error(e);
        alert(String(e && e.message ? e.message : e));
      }
    };

window.mfSetTheme = function(name){
    try{
      const t = THEMES[name] || THEMES["Midnight Blue"];
      const root = document.documentElement;
      Object.keys(t).forEach(k => root.style.setProperty(k, t[k]));

      // detect light themes
      const LIGHT_THEMES = new Set(["Frost", "Sand Gold", "Slate Light"]);
      const isLight = LIGHT_THEMES.has(name) || (String(name||"").toLowerCase().includes("light"));
      // Keep Quasar/NiceGUI in sync with the selected theme (fixes dark dropdowns/dialogs on light themes)
      try {
        if (window.Quasar && window.Quasar.Dark && typeof window.Quasar.Dark.set === 'function') {
          window.Quasar.Dark.set(!isLight);
        }
      } catch (e) {}
      document.documentElement.classList.toggle('q-dark', !isLight);
      document.documentElement.classList.toggle('q-light', isLight);
      document.body.classList.toggle('q-dark', !isLight);
      document.body.classList.toggle('body--dark', !isLight);
      document.body.classList.toggle('body--light', isLight);

      // menu background needs stronger contrast on light themes (Safari especially)
      root.style.setProperty('--mf-menu-bg', isLight ? '#FFFFFF' : 'rgba(10,14,24,0.92)');
      // mark theme on <html> for CSS targeting
      root.classList.toggle('mf-light', isLight);
      root.classList.toggle('mf-dark', !isLight);
      
      // Force dropdown menus (q-menu) to match theme, especially inside dialogs on iOS Safari
      try {
        if (!window.__mfMenuObserver) {
          const fixMenuNode = (node) => {
            if (!node || !node.classList) return;
            if (!node.classList.contains('q-menu')) return;

            const root = document.documentElement;
            const light = root.classList.contains('mf-light');

            if (light) {
              node.classList.remove('q-dark');
              node.classList.add('mf-menu-light');

              // Quasar sometimes keeps dark inline defaults on iOS Safari; force inline light surface.
              try {
                const cs = getComputedStyle(root);
                const bg = (cs.getPropertyValue('--mf-menu-bg') || '#FFFFFF').trim();
                const text = (cs.getPropertyValue('--mf-text') || 'rgba(10,12,20,0.92)').trim();
                const border = (cs.getPropertyValue('--mf-border') || 'rgba(0,0,0,0.10)').trim();

                node.style.background = bg;
                node.style.color = text;
                node.style.border = '1px solid ' + border;

                const list = node.querySelector('.q-list');
                if (list) {
                  list.style.background = bg;
                  list.style.color = text;
                }
                node.querySelectorAll('.q-item, .q-item__label, .q-item__section').forEach((el) => {
                  el.style.color = text;
                });
              } catch (e) {}
            } else {
              node.classList.remove('mf-menu-light');
              node.classList.add('q-dark');
              // clean inline styles
              node.style.background = '';
              node.style.color = '';
              node.style.border = '';
              const list = node.querySelector('.q-list');
              if (list) {
                list.style.background = '';
                list.style.color = '';
              }
              node.querySelectorAll('.q-item, .q-item__label, .q-item__section').forEach((el) => {
                el.style.color = '';
              });
            }
          };
      
          const scanMenus = () => {
            document.querySelectorAll('.q-menu').forEach(fixMenuNode);
          };
      
          window.__mfScanMenus = scanMenus;
          window.__mfFixMenuNode = fixMenuNode;

          window.__mfMenuObserver = new MutationObserver((mutations) => {
            for (const m of mutations) {
              for (const n of (m.addedNodes || [])) {
                fixMenuNode(n);
                if (n && n.querySelectorAll) {
                  n.querySelectorAll('.q-menu').forEach(fixMenuNode);
                }
              }
            }
          });
          window.__mfMenuObserver.observe(document.body, { childList: true, subtree: true });
          setTimeout(scanMenus, 50);
          try {
            if (!window.__mfMenuEvents) {
              window.__mfMenuEvents = true;
              const kick = () => { try { window.__mfScanMenus && window.__mfScanMenus(); } catch(e) {} };
              ['click','touchstart','focusin','keydown'].forEach((ev) => document.addEventListener(ev, () => setTimeout(kick, 0), true));
            }
          } catch (e) {}
        }
      } catch (e) {}

      localStorage.setItem("mf_theme", name);
      if(!window.__mfBooting){ localStorage.setItem("mf_theme_user","1"); }

      window.__mfThemeName = name;
      // Re-scan menus after applying theme (Quasar may reuse existing q-menu nodes)
      setTimeout(()=>{ try{ window.__mfScanMenus && window.__mfScanMenus(); } catch(e) {} }, 60);

      // Fix Plotly text colors after theme is applied
      setTimeout(()=>{ try{ window.mfFixPlotlyText && window.mfFixPlotlyText(); }catch(e){} }, 60);
    }catch(e){}
  };

  // Apply saved theme ASAP
  try{
    const saved = localStorage.getItem("mf_theme");
    if(saved){ window.mfSetTheme(saved); }
    else {
      // Default to system preference: Dark -> Emerald Gold, Light -> Sand Gold
      try{
        const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
        window.mfSetTheme(prefersDark ? "Emerald Gold" : "Sand Gold");
      }catch(e){
        window.mfSetTheme("Emerald Gold");
      }
    }
    try{ setTimeout(()=>{ window.mfFixPlotlyText && window.mfFixPlotlyText(); }, 120);}catch(e){}
    // finish booting
    window.__mfBooting = false;
    // If user never picked a theme manually, follow system preference changes
    try{
      if(!(localStorage.getItem("mf_theme_user")==="1") && window.matchMedia){
        const mq = window.matchMedia('(prefers-color-scheme: dark)');
        const handler = (e)=>{ try{ window.mfSetTheme(e.matches ? "Emerald Gold" : "Sand Gold"); }catch(_e){} };
        if(mq && mq.addEventListener){ mq.addEventListener('change', handler); }
        else if(mq && mq.addListener){ mq.addListener(handler); }
      }
    }catch(e){}

  }catch(e){
    try{ window.mfSetTheme("Emerald Gold"); }catch(_){}
  }
})();
</script>""",
    shared=True,
)


# Client-side OCR (free): used only when user scans a receipt on Expense form.
ui.add_head_html(
    "<script src='https://cdn.jsdelivr.net/npm/tesseract.js@5/dist/tesseract.min.js'></script>",
    shared=True,
)



# -----------------------------
# Theme helpers (server-side)
# -----------------------------
def current_theme_name() -> str:
    try:
        return str(app.storage.user.get("theme") or "Midnight Blue")
    except Exception:
        return "Midnight Blue"

def is_light_theme_name(name: str) -> bool:
    n = (name or "").lower()
    return ("light" in n) or (n in ("arctic light", "slate light", "sand gold", "frost", "pearl mint"))

def plotly_font_color() -> str:
    return "rgba(17,24,39,0.88)" if is_light_theme_name(current_theme_name()) else "rgba(255,255,255,0.88)"

def plotly_template() -> str:
    return "plotly_white" if is_light_theme_name(current_theme_name()) else "plotly_dark"


# -----------------------------
# Layout
# -----------------------------

# -----------------------------
# Global Search
# -----------------------------
def open_search_dialog() -> None:
    """Open a search dialog and jump to Transactions with the query prefilled."""
    with ui.dialog() as d, ui.card().classes("my-card p-5 w-full max-w-lg"):
        ui.label("Search transactions").classes("text-lg font-bold")
        q = ui.input(placeholder="Merchant, category, account, amount...").props("dense outlined").classes("w-full")
        with ui.row().classes("w-full justify-end gap-2 mt-2"):
            ui.button("Cancel", on_click=d.close).props("flat")
            def _go():
                query = (q.value or "").strip()
                if not query:
                    ui.notify("Type something to search", type="warning")
                    return
                try:
                    app.storage.user["tx_search_prefill"] = query
                except Exception:
                    pass
                d.close()
                nav_to("/tx")
            ui.button("Search", icon="search", on_click=_go).props("unelevated")
    d.open()

def topbar():
    with ui.row().classes("w-full items-center justify-between px-3 py-2"):
        with ui.row().classes("items-center gap-3"):
            ui.label("💳").classes("text-2xl")
            with ui.column().classes("gap-0"):
                ui.label(APP_TITLE).classes("text-lg font-bold")
                ui.label(APP_SUBTITLE).classes("text-xs").style("color: var(--mf-muted)")
        with ui.row().classes("items-center gap-2"):
            ui.button("Refresh", on_click=lambda: refresh_all()).props("outline icon=refresh").classes("text-sm")
            ui.button("Logout", on_click=logout).props("outline icon=logout").classes("text-sm")

def nav_button(label: str, icon: str, path: str):
    ui.button(label, on_click=lambda: nav_to(path)).props(f"flat icon={icon}").classes("w-full")

def shell(content_fn, *, active_path: str = ""):
    """Phase 5.2 shell: bank-style rail + header + canvas.
    Keeps Phase 4 logic intact and only wraps presentation.
    """
    # NOTE: do NOT use ui.open(); some NiceGUI versions on Render don't have it.
    # Use nav_to() or normal links.

    # Active path detection (best-effort)
    try:
        if not active_path:
            active_path = ui.context.client.page.path  # type: ignore[attr-defined]
    except Exception:
        pass

    def nav_btn(label: str, icon: str, href: str) -> None:
        cls = "mf-navbtn" + (" is-active" if href == active_path else "")
        def go() -> None:
            # use your Phase 4 router helper (no ui.open)
            try:
                nav_to(href)
            except Exception:
                pass
            # close overlay after navigation (mobile + desktop)
            ui.run_javascript("document.documentElement.classList.remove('mf-nav-open')")
        ui.button(label, icon=icon).props("flat").classes(cls).on("click", go)

    with ui.element("div").classes("mf-shell"):
        # Backdrop overlay (tap to close)
        ui.element("div").classes("mf-backdrop").on("click", lambda: ui.run_javascript("document.documentElement.classList.remove(\'mf-nav-open\')"))

        # Left rail
        with ui.element("div").classes("mf-rail"):
            with ui.element("div").classes("mf-rail-card"):
                ui.label("FinTrackr").classes("mf-brand")
                ui.separator().props("dark").classes("opacity-20 my-1")

                nav_btn("Home", "dashboard", "/")
                nav_btn("Add", "add_circle", "/add")
                nav_btn("Tx", "receipt_long", "/tx")
                nav_btn("Cards", "credit_card", "/cards")
                nav_btn("Rules", "rule", "/rules")
                nav_btn("Admin", "settings", "/admin")

                ui.separator().props("dark").classes("opacity-20 my-1")
                ui.label("Phase 5.2").classes("text-xs").style("color: var(--mf-muted); text-align:center;")

        # Main
        with ui.element("main").classes("mf-main"):
            with ui.element("div").classes("mf-header"):
                with ui.row().classes("items-center justify-between w-full"):
                    # LEFT: hamburger + title
                    with ui.row().classes("items-center gap-3"):
                        ui.button("", icon="menu").props("flat round dense").style(
                            "border: 1px solid var(--mf-border); background: var(--mf-surface);"
                        ).on("click", lambda: ui.run_javascript("document.documentElement.classList.toggle('mf-nav-open')"))
                        with ui.element("div").classes("mf-title"):
                            ui.link("FinTrackr", "/").classes("t1 text-2xl md:text-3xl").style("color: inherit; text-decoration: none;")
                            
                    # RIGHT: theme + actions
                    with ui.row().classes("items-center gap-2"):
                        # Theme control (desktop: inline select, mobile: palette dialog)
                        def _open_theme_dialog():
                            with ui.dialog() as td, ui.card().classes("my-card p-4 w-full max-w-sm"):
                                ui.label("Theme").classes("text-base font-bold")
                                # Theme chooser (button list instead of dropdown; avoids iOS Safari dark menu rendering)
                                themes = ['Midnight Blue', 'Emerald Gold', 'Graphite Rose', 'Arctic Light', 'Slate Light', 'Sand Gold']
                                cur = app.storage.user.get('theme')
                                if not cur:
                                    # Auto theme: dark at night, light in daytime (based on server TIMEZONE)
                                    try:
                                        h = now().hour
                                    except Exception:
                                        h = datetime.datetime.now().hour
                                    cur = 'Midnight Blue' if (h >= 19 or h < 7) else 'Arctic Light'
                                    app.storage.user['theme'] = cur
                                else:
                                    cur = str(cur)

                                with ui.column().classes("w-full mt-2 gap-2"):
                                    for tname in themes:
                                        is_cur = (tname == cur)
                                        btn = ui.button(
                                            tname,
                                            on_click=lambda tn=tname: (
                                                app.storage.user.__setitem__('theme', tn),
                                                ui.run_javascript(f"window.mfSetTheme({tn!r})"),
                                                td.close(),
                                            ),
                                        ).classes("w-full justify-start")
                                        btn.props("unelevated" if is_cur else "outline")
                                        btn.style("border-radius: 12px; padding: 10px 12px;")
                                with ui.row().classes("justify-end w-full mt-2"):
                                    ui.button("Close").props("flat").on("click", td.close)
                            td.open()

                        _theme_names = ['Midnight Blue', 'Emerald Gold', 'Graphite Rose', 'Arctic Light', 'Slate Light', 'Sand Gold']
                        theme_select = ui.select(
                            _theme_names,
                            value=(app.storage.user.get('theme') or 'Midnight Blue'),
                            on_change=lambda e: (app.storage.user.__setitem__('theme', e.value), ui.run_javascript(f"window.mfSetTheme({e.value!r})")),
                        ).props("dense outlined").classes("mf-hide-mobile").style(
                            "min-width: 190px; background: var(--mf-surface); border-radius: 12px;"
                        )

                        async def _sync_theme_select() -> None:
                            try:
                                saved = await ui.run_javascript('return localStorage.getItem("mf_theme")')
                                if saved and str(saved) in _theme_names:
                                    theme_select.value = str(saved)
                            except Exception:
                                pass
                        # Theme selection sync handled on user interaction to avoid timer issues on fast navigation
                        ui.button("", icon="palette").props("flat round dense").classes("mf-show-mobile").style(
                            "border: 1px solid var(--mf-border); background: var(--mf-surface);"
                        ).on("click", _open_theme_dialog)
                        ui.run_javascript('window.mfSetTheme(localStorage.getItem(\"mf_theme\") || \"Midnight Blue\")')
                        ui.button("", icon="refresh").props("flat round dense").style(
                            "border: 1px solid var(--mf-border); background: var(--mf-surface);"
                        ).on("click", lambda: ui.navigate.to(ui.context.client.page.path))
                        ui.button("", icon="logout").props("flat round dense").style(
                            "border: 1px solid var(--mf-border); background: var(--mf-surface);"
                        ).on("click", do_logout)
                        ui.button("", icon="search").props("flat round dense").style(
                            "border: 1px solid var(--mf-border); background: var(--mf-surface);"
                        ).on("click", lambda: open_search_dialog())
                        ui.button("Add", icon="add").props("unelevated").style(
                            "background: var(--mf-accent); color: #071022; border-radius: 12px; font-weight: 900;"
                        ).on("click", lambda: nav_to("/add"))

            with ui.element("div").classes("mf-canvas"):
                content_fn()



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
                        ui.label('Welcome to FinTrackr').classes('text-2xl font-bold')
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
        with ui.element('div').classes('dash-scope w-full'):
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
            ui.label(f"Your financial snapshot · {mkey}").classes('text-sm font-medium').style('color: var(--mf-muted); margin: 2px 0 12px 2px;')
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

            # --- Phase 4.6A: Hero summary (reduces tile clutter, feels like a bank app) ---
            try:
                days_to_next = (next_pay - start_d).days if next_pay else None
            except Exception:
                days_to_next = None

            with ui.card().classes('my-card p-5 mf-budget'):
                ui.label('Overview').classes('text-xs uppercase').style('color: var(--mf-muted); letter-spacing: 0.12em')
                with ui.row().classes('w-full items-end justify-between gap-4'):
                    with ui.column().classes('gap-1'):
                        ui.label('Pay period net').classes('text-sm').style('color: var(--mf-muted)')
                        ui.label(currency(net_pp)).classes('text-4xl font-extrabold')
                        ui.label(f"{pp_start.strftime('%b %d')} → {pp_end.strftime('%b %d')}").classes('text-xs').style('color: var(--mf-muted)')
                    with ui.column().classes('items-end gap-1'):
                        if next_pay:
                            ui.label('Next payday').classes('text-sm').style('color: var(--mf-muted)')
                            ui.label(next_pay.strftime('%a, %b %d')).classes('text-xl font-bold')
                            if days_to_next is not None:
                                ui.badge(f"In {days_to_next} days").style('background: rgba(32,201,151,0.18); color: var(--mf-text); border: 1px solid var(--mf-border);')
                ui.separator().classes('my-3 opacity-20')
                with ui.row().classes('gap-2'):
                    ui.button('Add expense', icon='add').props('unelevated').on('click', lambda: nav_to('/add?mode=expense'))
                    ui.button('Add income', icon='add').props('outline').on('click', lambda: nav_to('/add?mode=income'))
                    ui.button('View transactions', icon='receipt_long').props('flat').on('click', lambda: nav_to('/tx'))

            # KPI tiles (bank-style grid, 5.2.2)
            with ui.element("div").classes("grid grid-cols-2 md:grid-cols-4 gap-3 w-full"):
                for label, val, icon in [
                    ("Income (this month)", income, "trending_up"),
                    ("Expenses (this month)", expense, "trending_down"),
                    ("Investments (this month)", invest, "savings"),
                    ("Net (this month)", net, "insights"),
                ]:
                    _lbl = label.lower()
                    _col = "rgba(34,197,94,0.95)" if "income" in _lbl else ("rgba(239,68,68,0.95)" if "expense" in _lbl else ("rgba(59,130,246,0.95)" if "invest" in _lbl else "rgba(168,85,247,0.92)"))
                    with ui.card().classes("my-card p-4 w-full").style("min-height: 110px;"):
                        with ui.row().classes("items-center justify-between"):
                            ui.label(label).classes("text-xs uppercase").style("color: var(--mf-muted); letter-spacing: .12em")
                            ui.icon(icon).style("color: var(--mf-muted)")
                        ui.label(currency(val)).classes("text-2xl font-bold mt-1").style(f"color: {_col};")
                        ui.label(mkey).classes("text-xs").style("color: var(--mf-muted)")

            with ui.row().classes('w-full gap-3'):
                # Pay period tiles (grid, 5.2.2)
                with ui.element("div").classes("grid grid-cols-2 md:grid-cols-4 gap-3 w-full"):
                    for label, val, icon in [
                        ('Income (pay period)', income_pp, "payments"),
                        ('Expenses (pay period)', expense_pp, "receipt_long"),
                        ('Investments (pay period)', invest_pp, "account_balance"),
                        ('Net (pay period)', net_pp, "timeline"),
                    ]:
                        with ui.card().classes('my-card p-4 w-full').style("min-height: 110px;"):
                            with ui.row().classes("items-center justify-between"):
                                ui.label(label).classes('text-xs uppercase').style('color: var(--mf-muted); letter-spacing: .12em')
                                ui.icon(icon).style("color: var(--mf-muted)")
                            ui.label(currency(val)).classes('text-2xl font-bold mt-1')
                            ui.label(f"{pp_start.strftime('%b %d')} → {pp_end.strftime('%b %d')}").classes('text-xs').style('color: var(--mf-muted)')


            # Quick actions + data quality
            # Phase 4.6A: Quick actions moved into the Overview card to reduce clutter
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
                                    with ui.row().classes('w-full items-start justify-between'):
                                        ui.label(cat).classes('text-sm')
                                        with ui.column().classes('items-end'):
                                            ui.label(f"{int(round(pct*100))}%").classes('text-xs font-bold').style('color: var(--mf-text)')
                                            ui.label(f"{currency(spent_amt)} / {currency(bud_amt)}").classes('text-xs').style('color: var(--mf-muted)')
                                    ui.linear_progress(value=pct, show_value=False).classes('mf-budget-bar').props('size=10px')

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
                ui.label("Upcoming salary").classes("text-lg font-bold")
                if not pays:
                    ui.label("No paydays in the next 45 days.").style("color: var(--mf-muted)")
                else:
                    # Group paydays by person (Salary 1 = Nishanth, Salary 2 = Indhu)
                    grouped = {"Nishanth": [], "Indhu": []}
                    for who, d in pays:
                        if who == "Salary 1":
                            grouped["Nishanth"].append(d)
                        elif who == "Salary 2":
                            grouped["Indhu"].append(d)
                    for k in grouped:
                        grouped[k] = sorted(set(grouped[k]))

                    def _salary_card(name: str, dates: list):
                        next_d = next((x for x in dates if x >= today()), None)
                        if not next_d:
                            return
                        days = (next_d - today()).days
                        with ui.card().classes("my-card p-4 w-full").style("background: rgba(255,255,255,0.045);"):
                            with ui.row().classes("items-center justify-between"):
                                ui.label(f"{name}'s salary").classes("text-sm font-bold")
                                ui.badge(f"In {days} days").style(
                                    "background: rgba(255,255,255,0.10); color: var(--mf-text); border: 1px solid var(--mf-border);"
                                )
                            ui.label(next_d.strftime("%a, %b %d")).classes("text-2xl font-extrabold mt-1")
                            # show 2 upcoming dates (optional)
                            upcoming = [x for x in dates if x >= today()][:2]
                            if len(upcoming) > 1:
                                ui.label("Next: " + ", ".join([x.strftime("%b %d") for x in upcoming[1:]])).classes("text-xs").style("color: var(--mf-muted)")

                    with ui.element("div").classes("grid grid-cols-1 md:grid-cols-2 gap-3 w-full"):
                        _salary_card("Nishanth", grouped.get("Nishanth", []))
                        _salary_card("Indhu", grouped.get("Indhu", []))

            # Spending breakdown
            with ui.card().classes("my-card p-5"):
                ui.label("Spending breakdown (this month)").classes("text-lg font-bold")
                if spend.empty:
                    ui.label("No expenses this month.").style("color: var(--mf-muted)")
                else:
                    agg = spend.groupby("category", as_index=False)["amount_num"].sum()
                    fig = px.pie(agg, names="category", values="amount_num", hole=0.55, template=plotly_template())
                    # Ensure text stays readable across light/dark themes
                    fig.update_traces(textfont_color=plotly_font_color())
                    fig.update_layout(
                        margin=dict(l=10, r=10, t=10, b=10),
                        paper_bgcolor="rgba(0,0,0,0)",
                        font_color=plotly_font_color(),
                        legend=dict(font=dict(color=plotly_font_color())),
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
                fig2 = px.area(daily, x="day", y="signed_amount", template=plotly_template())
                fig2.update_traces(line=dict(color=None))
                fig2.update_layout(
                    margin=dict(l=10, r=10, t=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    font_color=plotly_font_color(),
                    xaxis=dict(tickfont=dict(color=plotly_font_color()), title_font=dict(color=plotly_font_color())),
                    yaxis=dict(tickfont=dict(color=plotly_font_color()), title_font=dict(color=plotly_font_color())),
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
        with dlg, ui.card().classes("my-card p-5 w-[620px] max-w-[95vw]").style("max-height: 88vh; overflow-y: auto; padding-bottom: 18px;"):

            ui.label(f"Add: {entry_type}").classes("text-lg font-bold")

            d_date = ui.input("Date", value=today().isoformat()).props("type=date").classes("w-full")
            d_amount = ui.number("Amount", value=0.0, format="%.2f").classes("w-full")

            is_debit = entry_type.lower() == 'debit'
            is_income = entry_type.lower() in ('credit', 'income')
            is_invest = entry_type.lower() == 'investment'
            is_cc_repay = entry_type.lower() in ('cc repay', 'cc_repay', 'ccrepay', 'credit card repay', 'credit card repayment')

            # Per 5.14 UX rules:
            # - Income: Method fixed to Bank (no method dropdown)
            # - Investment: Method fixed to Bank, Account disabled, Category default Investment
            # - CC Repay: Method fixed to Card (no method dropdown)
            fixed_method = None
            hide_method = False
            disable_account = False
            fixed_category = preset_category

            if is_income:
                fixed_method = 'Bank'
                hide_method = True
                disable_account = True  # income goes to bank; avoid card/LOC accounts
            if is_invest:
                fixed_method = 'Bank'
                hide_method = True
                disable_account = True
                if not fixed_category:
                    fixed_category = 'Investment'
            if is_cc_repay:
                fixed_method = 'Card'
                hide_method = True

            default_method = ("Card" if is_debit else ("Bank" if (is_income or is_invest) else "Other"))

            # Presets override remembered defaults.
            method_default = (fixed_method or preset_method or (last_debit_method if (is_debit and last_debit_method in methods) else default_method))
            # Choose a sensible default account
            if disable_account:
                # Prefer a non-card/bank-like account for Investment (locked)
                def _is_card_account(name: str) -> bool:
                    n = (name or '').lower()
                    return any(x in n for x in ['mastercard', 'visa', 'card', 'ct ', 'canadiantire', 'credit'])
                bank_candidates = [a for a in accounts if a and (not _is_card_account(str(a)))]
                account_default = (preset_account or (bank_candidates[0] if bank_candidates else (accounts[0] if accounts else "")))
                if is_income:
                    accounts = bank_candidates  # hide card/LOC accounts for income
            else:
                account_default = (preset_account or (last_debit_account if (is_debit and last_debit_account in accounts) else (accounts[0] if accounts else "")))


            # Ensure defaults are valid options (NiceGUI select raises if value not in options)
            if method_default and method_default not in methods:
                methods = [method_default] + [m for m in methods if m != method_default]
            if account_default and account_default not in (accounts or []):
                accounts = [account_default] + [a for a in (accounts or []) if a != account_default]

            if hide_method:
                d_method = None
            else:
                d_method = ui.select(methods or [""], value=(method_default if method_default in (methods or []) else ""), label="Method").classes("w-full")

            d_account = ui.select(accounts or [""], value=(account_default if account_default in (accounts or []) else ""), label="Account").classes("w-full")
            d_account.props('popup-content-class="mf-menu-light"')
            if disable_account:
                d_account.props("disable")

            if hide_method and fixed_method:
                ui.label(f"Method: {fixed_method}").classes("text-xs").style("color: var(--mf-muted); margin-top:-6px;")
            d_category = ui.select(categories, value=(fixed_category or "Uncategorized"), label="Category").classes("w-full")
            d_notes = ui.textarea("Notes", value="").classes("w-full")
            d_rec = ui.checkbox("Mark as recurring (creates template for future cycles only)")

            # Receipt scan (Expense only): opens camera on mobile, runs free OCR in the browser (tesseract.js)
            if entry_type.lower() == 'debit':
                scan_state: Dict[str, Any] = {"data_url": None}

                scan_dlg = ui.dialog()
                parsed_state: Dict[str, Any] = {"parsed": None}
                with scan_dlg, ui.card().classes('my-card p-0 w-[720px] max-w-[95vw]').style('max-height: min(88vh, 80dvh); height: min(88vh, 80dvh); display:flex; flex-direction:column; overflow:hidden;'):
                    # Keep action buttons visible on mobile by making the content area scrollable.
                    with ui.column().classes('w-full').style('flex:1; overflow-y:auto; padding: 16px;'):
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
                                    try:
                                        if hasattr(obj, 'seek'):
                                            obj.seek(0)
                                    except Exception:
                                        pass
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

                        upload_receipt = ui.upload(auto_upload=True, label='Capture / Upload receipt').props("accept='image/*'").classes('w-full')
                        try:
                            upload_receipt.on_upload(_on_upload)
                        except Exception:
                            upload_receipt.on('upload', _on_upload)

                        async def _run_ocr() -> None:
                            if not scan_state.get('data_url'):
                                # 5.2.6 HF: some mobile browsers show upload as complete but the server event may not fire.
                                # Try to recover from the upload component's value before warning the user.
                                try:
                                    maybe_files = getattr(upload_receipt, 'value', None)
                                    if maybe_files:
                                        # Try to feed the first file-like object into the same handler.
                                        await _on_upload(maybe_files[0])
                                except Exception:
                                    pass
                                if not scan_state.get('data_url'):
                                    ui.notify('Please upload a receipt image first.', type='warning')
                                    return
                            ui.notify('Scanning…', type='info', timeout=1.2)
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
                                ui.notify('Low confidence TOTAL detected — verify amount before applying.', type='warning', timeout=2.0)
                            else:
                                ui.notify('Scan complete. Review and tap Apply.', type='positive', timeout=1.2)

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
                    with ui.row().classes('w-full items-center gap-2 sticky bottom-0').style('background: rgba(8,12,20,0.92); backdrop-filter: blur(8px); padding: 10px; border-top: 1px solid var(--mf-border); position: sticky; bottom: 0; background: var(--mf-menu-bg, var(--mf-surface-2)); backdrop-filter: blur(18px); -webkit-backdrop-filter: blur(18px); z-index: 20'):
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
            if preset_method is not None and d_method is not None:
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

                method = str(((d_method.value if d_method is not None else method_default) or "Other")).strip()

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


            # Sticky footer so Save/Cancel never get pushed off-screen on mobile
            with ui.row().classes("w-full justify-end gap-2 sticky bottom-0").style(
                "padding: 10px; background: var(--mf-card-top); backdrop-filter: blur(10px); border-top: 1px solid var(--mf-border);"
            ):
                ui.button("Cancel", on_click=dlg.close).props("flat")
                ui.button("Save", on_click=save).props("unelevated")

        ui.run_javascript('window.mfSetTheme(localStorage.getItem(\\"mf_theme\\")||\\"Midnight Blue\\");')
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
            if is_month_locked(f_month.value or mkey):
                ui.notify("This month is locked. Unlock it to edit.", type="warning")
                return
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

            # Month selector + lock (5.12)
            month_options = []
            try:
                if not tx.empty and "date_parsed" in tx.columns:
                    month_options = sorted({month_key(d) for d in tx["date_parsed"].dropna().tolist()}, reverse=True)
            except Exception:
                month_options = []
            if not month_options:
                month_options = [month_key(today())]

            mkey = (app.storage.user.get("tx_month") or month_key(today()))
            if mkey not in month_options:
                mkey = month_options[0]

            with ui.row().classes("w-full items-center gap-3 mt-2"):
                f_month = ui.select(month_options, value=mkey, label="Month").classes("w-full")
                lock_sw = ui.switch("Month Lock", value=is_month_locked(mkey)).classes("shrink-0")

            f_type = ui.select(["All"] + types, value="All", label="Type").classes("w-full")
            f_text = ui.input("Search notes/category/account").classes("w-full")
            try:
                q_prefill = (app.storage.user.get('tx_search_prefill') or '').strip()
                if q_prefill:
                    f_text.value = q_prefill
                    app.storage.user.pop('tx_search_prefill', None)
            except Exception:
                pass
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
            # Handlers for month + lock (5.12)
            def _on_month_changed(e=None):
                mk = f_month.value or mkey
                app.storage.user["tx_month"] = mk
                try:
                    lock_sw.value = is_month_locked(mk)
                except Exception:
                    pass
                refresh_table()

            f_month.on('update:model-value', lambda e: _on_month_changed())
            lock_sw.on('update:model-value', lambda e: (set_month_lock(f_month.value or mkey, bool(lock_sw.value)),
                                                        ui.notify(('Month locked' if lock_sw.value else 'Month unlocked'), type='positive'),
                                                        refresh_table()))
            def refresh_table():
                df = tx.copy()
                mk = (f_month.value or mkey)
                app.storage.user["tx_month"] = mk
                try:
                    lock_sw.value = is_month_locked(mk)
                except Exception:
                    pass
                if "date_parsed" in df.columns:
                    df = df[df["date_parsed"].apply(lambda d: month_key(d) if pd.notna(d) else "") == mk]
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
                    # search across common text fields (merchant/notes/category/account/method/etc.)
                    cols = [c for c in ["merchant", "payee", "description", "notes", "category", "account", "method", "card", "source"] if c in df.columns]
                    if cols:
                        hay = df[cols].astype(str).agg(" ".join, axis=1).str.lower()
                    else:
                        # fallback: stringify the row
                        hay = df.astype(str).agg(" ".join, axis=1).str.lower()

                    mask_text = hay.str.contains(q, na=False)

                    # numeric search: if user typed a number, also match amount closely (OR with text match)
                    mask_amt = False
                    try:
                        q_num = float(q.replace(",", "").replace("$", ""))
                    except Exception:
                        q_num = None
                    if q_num is not None and "amount" in df.columns:
                        amt = df["amount"].apply(to_float)
                        mask_amt = (amt - q_num).abs() < 0.01

                    df = df[mask_text | mask_amt]
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
                if is_month_locked(f_month.value or mkey):
                    ui.notify("This month is locked. Unlock it to edit.", type="warning")
                    return
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



@ui.page("/security")
def security_page() -> None:
    if not require_login():
        nav_to("/login")
        return

    def content() -> None:
        with ui.card().classes("my-card p-5"):
            ui.label("Passkeys / Face ID").classes("text-lg font-bold")
            ui.label("Register a passkey for quick biometric login (iPhone Face ID, Touch ID, etc.).").style("color: var(--mf-muted)")
            ui.separator().classes("my-3 opacity-30")

            default_user = os.environ.get('APP_USER') or os.environ.get('APP_USERNAME') or 'admin'
            u_in = ui.input("Username for passkey", value=default_user).classes("w-full").props("id=pk_user")

            def do_register():
                username = (u_in.value or "").strip()
                if not username:
                    ui.notify("Username required", type="warning")
                    return
                ui.notify("Opening Face ID / Passkey prompt…", type="info", timeout=1.5)
                js = """
                (async () => {{
                  try {{
                    if (!window.PublicKeyCredential) {{
                      throw new Error('Passkeys not supported on this browser/device');
                    }}
                    const u = {json.dumps("%%U%%")};
                    const optRes = await fetch(`/api/passkeys/options/register?username=${{encodeURIComponent(u)}}`);
                    if (!optRes.ok) {{
                      const t = await optRes.text();
                      throw new Error(t || "Failed to get registration options");
                    }}
                    const opts = await optRes.json();
                    const b64urlToBuf = (s) => { s=(s||'').replace(/-/g,'+').replace(/_/g,'/'); while(s.length%4) s+='='; return Uint8Array.from(atob(s), c=>c.charCodeAt(0)); };
                    const bufToB64url = (b) => btoa(String.fromCharCode(...new Uint8Array(b))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/g,'');
                    const pubKey = {{
                      challenge: b64urlToBuf(opts.challenge),
                      rp: opts.rp,
                      user: {{ id: b64urlToBuf(opts.user.id), name: opts.user.name, displayName: opts.user.displayName }},
                      pubKeyCredParams: opts.pubKeyCredParams,
                      timeout: opts.timeout,
                      attestation: opts.attestation,
                      ...(opts.authenticatorSelection ? { authenticatorSelection: opts.authenticatorSelection } : {}),
                    }};
                    if (opts.excludeCredentials) {{
                      pubKey.excludeCredentials = opts.excludeCredentials.map(c => ({
                        type: c.type,
                        id: b64urlToBuf(c.id),
                      }));
                    }}
                    const cred = await navigator.credentials.create({{ publicKey: pubKey }});
                    const data = {{
                      id: cred.id,
                      rawId: bufToB64url(cred.rawId),
                      type: cred.type,
                      response: {{
                        clientDataJSON: bufToB64url(cred.response.clientDataJSON),
                        attestationObject: bufToB64url(cred.response.attestationObject),
                      }}
                    }};
                    const vRes = await fetch(`/api/passkeys/verify/register`, {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify(data)}});
                    if (!vRes.ok) {{
                      const t = await vRes.text();
                      throw new Error(t || "Registration verify failed");
                    }}
                    document.getElementById('pk_status')?.replaceChildren('Passkey registered ✅');
                  }} catch (e) {{
                    alert(`Passkey registration failed: ${{e.message||e}}`);
                  }}
                }})(); 
                """;
                js = js.replace("%%U%%", username)
                ui.run_javascript(js)
            ui.button('Register Passkey on this device', on_click=do_register).props('unelevated').classes('w-full mt-2')
            ui.label('').classes('text-xs mt-2').style('color: var(--mf-muted);').props('id=pk_status')

            with ui.row().classes("items-center gap-2 mt-3"):
                ui.icon("info").style("opacity:0.8")
                ui.label("If you change domain, set MYFIN_RP_ID and MYFIN_ORIGIN in Render env.").classes("text-xs").style("color: var(--mf-muted)")

            ui.separator().classes("my-3 opacity-30")
            ui.label("Registered passkeys (server)").classes("text-sm font-semibold").style("color: var(--mf-muted)")
            store = _load_passkeys()
            if not store:
                ui.label("No passkeys registered yet.").style("color: var(--mf-muted)")
            else:
                for user, data in store.items():
                    with ui.card().classes("my-card p-3 w-full"):
                        ui.label(user).classes("font-semibold")
                        ui.label(f"Credential ID: {str(data.get('credential_id',''))[:18]}…").classes("text-xs").style("color: var(--mf-muted)")
                        def _mk_del(u=user):
                            def _del():
                                s=_load_passkeys()
                                if u in s:
                                    s.pop(u, None)
                                    _save_passkeys(s)
                                    ui.notify("Deleted passkey", type="positive")
                                    nav_to("/security")
                            return _del
                        ui.button("Delete", on_click=_mk_del()).props("outline").classes("mt-2")

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

        
        # --- Build card entries (computed from transactions) ---
        entries = []
        for i in range(len(df)):
            name = str(names[i]).strip() or 'Card'
            emoji = str(emojis[i]).strip()
            method = str(methods[i]).strip()
            bd = billing_days[i]
            try:
                lim = float(str(limits[i]).replace(',','').strip()) if str(limits[i]).strip() else 0.0
            except Exception:
                lim = 0.0

            # Sum spend (debit) for this card within current cycle (if available)
            util_used = 0.0
            util_paid = 0.0

            tx = cached_df('transactions')
            if not tx.empty:
                txu = tx.copy()
                if 'amount_num' not in txu.columns:
                    txu['amount_num'] = txu.get('amount','').apply(parse_amount)
                if 'type_norm' not in txu.columns:
                    txu['type_norm'] = txu.get('type','').astype(str).str.strip().str.lower()

                # Try infer cycle window from billing day (optional; keep fallback to all-time)
                cycle_mask = pd.Series([True]*len(txu))
                try:
                    bd_int = int(float(bd)) if str(bd).strip() else None
                except Exception:
                    bd_int = None
                if bd_int and 1 <= bd_int <= 31:
                    txu['date_parsed'] = txu.get('date','').apply(parse_date)
                    if txu['date_parsed'].notna().any():
                        today_d = today()
                        import calendar as _cal
                        # last statement date = most recent billing day
                        last_stmt_month = today_d.month
                        last_stmt_year = today_d.year
                        if today_d.day < bd_int:
                            # go previous month
                            if last_stmt_month == 1:
                                last_stmt_month = 12
                                last_stmt_year -= 1
                            else:
                                last_stmt_month -= 1
                        last_day = _cal.monthrange(last_stmt_year, last_stmt_month)[1]
                        stmt_day = min(bd_int, last_day)
                        last_stmt = dt.date(last_stmt_year, last_stmt_month, stmt_day)
                        cycle_start = last_stmt + dt.timedelta(days=1)
                        cycle_mask = (txu['date_parsed'] >= cycle_start) & (txu['date_parsed'] <= today_d)

                method_key = str(method).strip()
                card_key = str(name).strip()

                scope = txu[cycle_mask].copy()

                spend_mask = (scope['type_norm'].isin(['debit','expense','spend'])) & (scope.get('account','').astype(str).str.strip() == card_key)
                util_used = float(scope.loc[spend_mask, 'amount_num'].sum())

                repay_mask = (scope['type_norm'].isin(['credit card repay','cc repay','credit card repayment','cc repayment'])) & (scope.get('account','').astype(str).str.strip() == card_key)
                util_paid = float(scope.loc[repay_mask, 'amount_num'].sum())

            balance = max(0.0, util_used - util_paid)
            remaining = max(0.0, (lim - balance)) if lim else 0.0
            pct = (balance/lim) if lim else 0.0
            pct = max(0.0, min(1.0, pct))

            # Trend since last payoff (balance reset when repaid to ~0)
            spark_x: List[str] = []
            spark_y: List[float] = []
            try:
                hist = txu.copy()
                if 'date_parsed' not in hist.columns:
                    hist['date_parsed'] = hist.get('date','').apply(parse_date)

                hist = hist[hist.get('account','').astype(str).str.strip() == card_key].copy()
                hist = hist[hist['date_parsed'].notna()].sort_values('date_parsed')

                if not hist.empty:
                    spend_m = hist['type_norm'].isin(['debit','expense','spend'])
                    repay_m = hist['type_norm'].isin(['credit card repay','cc repay','credit card repayment','cc repayment'])
                    hist = hist[spend_m | repay_m].copy()

                    hist['signed'] = 0.0
                    hist.loc[spend_m, 'signed'] = hist.loc[spend_m, 'amount_num'].astype(float)
                    hist.loc[repay_m, 'signed'] = -hist.loc[repay_m, 'amount_num'].astype(float)

                    if not hist.empty:
                        hist['bal'] = hist['signed'].cumsum()

                        payoff_rows = hist.index[hist['bal'] <= 0.00001].tolist()
                        if payoff_rows:
                            last_payoff = payoff_rows[-1]
                            hist2 = hist.loc[hist.index > last_payoff].copy()
                        else:
                            hist2 = hist.copy()

                        if not hist2.empty:
                            daily = hist2.groupby('date_parsed', as_index=False)['signed'].sum()
                            daily = daily.sort_values('date_parsed')
                            daily['bal'] = daily['signed'].cumsum()

                            spark_x = [d.isoformat() for d in daily['date_parsed'].tolist()]
                            spark_y = [float(x) for x in daily['bal'].tolist()]
            except Exception:
                pass

            entries.append({
                'name': name,
                'emoji': emoji,
                'method': method,
                'billing_day': bd,
                'limit': lim,
                'balance': balance,
                'remaining': remaining,
                'pct': pct,
                'spark_x': spark_x,
                'spark_y': spark_y,
            })

        def _is_ct(c):
            n = c['name'].lower()
            return ('canadiantire' in n) or ('canadian tire' in n)

        def _is_loc(c):
            n = c['name'].lower()
            m = (c.get('method') or '').lower().strip()
            return ('line of credit' in n) or (m == 'loc') or (n.startswith('loc')) or (' loc' in n)

        def _is_rbc(c):
            return ('rbc' in c['name'].lower())

        # Desired grouping + order
        ct = [c for c in entries if _is_ct(c) and not _is_loc(c)]
        rbc = [c for c in entries if _is_rbc(c) and not _is_loc(c) and not _is_ct(c)]
        loc = [c for c in entries if _is_loc(c)]
        other = [c for c in entries if c not in ct + rbc + loc]

        def _order_ct(c):
            n = c['name'].lower()
            return (0 if 'grey' in n or 'gray' in n else 1, n)

        def _order_rbc(c):
            n = c['name'].lower()
            return (0 if 'visa' in n else 1 if 'master' in n else 2, n)

        ct.sort(key=_order_ct)
        rbc.sort(key=_order_rbc)

        def _tile(c, col='col-12 col-sm-6', emph=False):
            extra = ' mf-card-emph' if emph else ''
            issuer = ' mf-issuer-ct' if _is_ct(c) else (' mf-issuer-loc' if _is_loc(c) else (' mf-issuer-rbc' if _is_rbc(c) else ''))
            # card visual variant (CT black/grey, RBC)
            nlow = c['name'].lower()
            variant = ''
            if _is_ct(c):
                variant = ' mf-ct-black' if ('black' in nlow) else (' mf-ct-grey' if ('grey' in nlow or 'gray' in nlow) else '')
            elif _is_rbc(c):
                variant = ' mf-rbc-blue'
            # Use a plain div with Quasar grid classes so 2 cards can sit in a single row reliably (incl. mobile)
            with ui.element('div').classes(col):
                with ui.card().classes('my-card mf-card-widget' + extra + issuer + variant):
                    with ui.row().classes('items-center justify-between'):
                        ui.label(f"{c['emoji']} {c['name']}").classes('text-lg font-semibold').style('color: var(--mf-text);')
                        if c.get('method'):
                            ui.badge(c['method']).classes('q-pa-xs').style('background: rgba(120,160,255,0.18); color: var(--mf-text); border: 1px solid var(--mf-border);')

                    with ui.row().classes('w-full items-center justify-between mt-3'):
                        ui.label('Limit').classes('text-xs').style('color: var(--mf-muted);')
                        ui.label(currency(c['limit']) if c.get('limit') else '—').classes('text-sm font-semibold').style('color: var(--mf-text);')
                        ui.label('Billing day').classes('text-xs').style('color: var(--mf-muted);')
                        ui.label(str(c.get('billing_day') or '—')).classes('text-sm font-semibold').style('color: var(--mf-text);')

                    ui.separator().classes('my-3 opacity-30')

                    with ui.row().classes('w-full items-center justify-between'):
                        ui.label('Used').classes('text-xs').style('color: var(--mf-muted);')
                        ui.label(currency(c.get('balance', 0.0))).classes('text-sm font-semibold').style('color: var(--mf-text);')

                    with ui.element('div').classes('w-full mf-progress'):
                        ui.element('div').style(f"width: {float(c.get('pct', 0.0))*100:.1f}%;")

                    with ui.row().classes('w-full items-center justify-between mt-2'):
                        ui.label('Remaining').classes('text-xs').style('color: var(--mf-muted);')
                        ui.label(currency(c.get('remaining', 0.0)) if c.get('limit') else '—').classes('text-sm font-semibold').style('color: var(--mf-text);')

                    # Mini trend chart: balance since last payoff (resets after fully repaid)
                    try:
                        sx = c.get('spark_x') or []
                        sy = c.get('spark_y') or []
                        if len(sx) >= 2 and len(sy) == len(sx):
                            import plotly.graph_objects as go
                            fig = go.Figure()
                            fig.add_trace(go.Scatter(x=sx, y=sy, mode='lines', line=dict(width=2)))
                            fig.update_layout(
                                height=90,
                                margin=dict(l=0, r=0, t=6, b=0),
                                paper_bgcolor='rgba(0,0,0,0)',
                                plot_bgcolor='rgba(0,0,0,0)',
                                showlegend=False,
                                xaxis=dict(visible=False),
                                yaxis=dict(visible=False),
                            )
                            ui.plotly(fig).classes('w-full mt-2')
                        else:
                            ui.label('No recent activity').classes('text-xs mt-2').style('color: var(--mf-muted);')
                    except Exception:
                        ui.label('No recent activity').classes('text-xs mt-2').style('color: var(--mf-muted);')

        def _two_row(items):
            # Responsive grid (prevents large empty right space on wide desktops)
            with ui.element('div').classes('grid grid-cols-1 md:grid-cols-2 gap-4 w-full'):
                for c in items:
                    _tile(c, col='w-full')

        # --- Render: Canadian Tire (2 in a row)
        if ct:
            ui.label('Canadian Tire').classes('text-sm font-semibold mt-4').style('color: var(--mf-muted); letter-spacing:0.4px;')
            _two_row(ct)

        # --- Render: RBC Cards (2 in a row)
        if rbc:
            ui.label('RBC Cards').classes('text-sm font-semibold mt-6').style('color: var(--mf-muted); letter-spacing:0.4px;')
            _two_row(rbc)

        if other:
            ui.label('Other').classes('text-sm font-semibold mt-6').style('color: var(--mf-muted); letter-spacing:0.4px;')
            _two_row(other)

        # spacer + LOC single
        if loc:
            ui.element('div').style('height: 18px;')
            ui.label('Line of Credit').classes('text-sm font-semibold mt-6').style('color: var(--mf-muted); letter-spacing:0.4px;')
            with ui.row().classes('row w-full q-col-gutter-md justify-center'):
                for c in loc:
                    _tile(c, col='col-12', emph=True)

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
        if rdf is None or rdf.empty:
            rdf = (rdf if rdf is not None else pd.DataFrame(columns=["keyword", "category"])).copy()

        # Normalize columns
        if "keyword" not in rdf.columns:
            rdf["keyword"] = ""
        if "category" not in rdf.columns:
            rdf["category"] = ""

        rdf_view = rdf.copy()
        rdf_view["keyword"] = rdf_view["keyword"].astype(str).fillna("")
        rdf_view["category"] = rdf_view["category"].astype(str).fillna("")
        rdf_view["keyword_l"] = rdf_view["keyword"].str.lower()

        state = {"selected_kw": None}

        def parse_keywords(s: str) -> list[str]:
            parts = [p.strip() for p in (s or "").split(",")]
            parts = [p for p in parts if p]
            return sorted(list(dict.fromkeys([p.lower() for p in parts])))

        def keywords_to_string(keys: list[str]) -> str:
            return ", ".join(keys)

        def chips_preview(keys: list[str], max_chips: int = 3) -> str:
            if not keys:
                return ""
            shown = keys[:max_chips]
            tail = len(keys) - len(shown)
            return " • ".join(shown) + (f"  +{tail}" if tail > 0 else "")

        # UI
        ui.label("Rules").classes("text-2xl font-semibold").style("color: var(--mf-text);")
        ui.label("Keyword → category mapping used for Auto-category in Add.").classes("text-sm").style("color: var(--mf-muted);")

        with ui.row().classes("w-full gap-4 mt-4"):

            # ------------------------------
            # LEFT: Rule list (compact)
            # ------------------------------
            with ui.card().classes("my-card").style("width: 340px; max-width: 100%;"):
                with ui.row().classes("items-center justify-between"):
                    ui.label("Rule list").classes("text-sm font-semibold").style("color: var(--mf-text);")
                    ui.button("", icon="add").props("flat round").on("click", lambda e: clear_selection())
                ui.separator().classes("opacity-30 my-2")

                search = ui.input(placeholder="Search keyword/category").props("dense").classes("w-full")
                list_area = ui.column().classes("w-full gap-1").style("max-height: 62vh; overflow: auto;")

                def render_list():
                    list_area.clear()
                    q = (search.value or "").strip().lower()
                    view = rdf_view
                    if q:
                        view = view[
                            view["keyword_l"].str.contains(q, na=False)
                            | view["category"].str.lower().str.contains(q, na=False)
                        ].copy()

                    if view.empty:
                        ui.label("No rules match.").classes("text-sm").style("color: var(--mf-muted);").move(list_area)
                        return

                    view = view.sort_values(["category", "keyword_l"], kind="stable")

                    for _, row in view.iterrows():
                        kw_raw = str(row.get("keyword", "") or "")
                        cat = str(row.get("category", "") or "")
                        keys = parse_keywords(kw_raw)
                        active = (state["selected_kw"] == kw_raw)

                        item = ui.card().classes("q-pa-sm").style(
                            ""  # style set below
                        )
                        item.move(list_area)
                        item.style(
                            "border-radius: 14px; cursor:pointer; "
                            + ("border: 1px solid rgba(91,140,255,0.45); background: rgba(91,140,255,0.10);" if active
                               else "border: 1px solid var(--mf-border); background: rgba(255,255,255,0.04);")
                        )
                        with item:
                            ui.label(cat or "—").classes("text-sm font-semibold").style("color: var(--mf-text);")
                            ui.label(chips_preview(keys, 4) or "—").classes("text-xs").style("color: var(--mf-muted);")
                        item.on("click", lambda e, kw=kw_raw: (select_rule(kw), render_list()))

                search.on("input", lambda e: render_list())

            # ------------------------------
            # RIGHT: Editor
            # ------------------------------
            with ui.card().classes("my-card flex-1").style("min-width: 320px;"):
                with ui.row().classes("items-center justify-between"):
                    ui.label("Rule editor").classes("text-sm font-semibold").style("color: var(--mf-text);")
                    mode_badge = ui.badge("New").style(
                        "background: rgba(255,255,255,0.06); color: var(--mf-text); border: 1px solid var(--mf-border);"
                    )

                ui.separator().classes("opacity-30 my-2")

                ui.label("Keywords").classes("text-xs").style("color: var(--mf-muted);")
                kw_input = ui.input(placeholder="e.g. walmart, superstore, uber").classes("w-full")
                chips_row = ui.row().classes("w-full items-center gap-2").style("flex-wrap: wrap; margin-top: 10px;")
                hint_label = ui.label("Tip: Use multiple keywords separated by commas. Matching is case-insensitive.").classes("text-xs").style(
                    "color: var(--mf-muted); margin-top:6px;"
                )

                ui.separator().classes("opacity-30 my-3")

                ui.label("Category").classes("text-xs").style("color: var(--mf-muted);")
                cat_input = ui.input(placeholder="e.g. Groceries").classes("w-full")

                ui.separator().classes("opacity-30 my-3")

                with ui.row().classes("w-full justify-end gap-2"):
                    del_btn = ui.button("Delete", icon="delete").props("flat").style(
                        "border: 1px solid rgba(255,90,90,0.35); color: rgba(255,255,255,0.92);"
                    )
                    save_btn = ui.button("Save", icon="save").props("unelevated").style(
                        "background: var(--mf-accent); color: #071022; font-weight: 900;"
                    )

                # ------------------------------
                # Editor helpers
                # ------------------------------
                def refresh_chips() -> None:
                    chips_row.clear()
                    keys = parse_keywords(kw_input.value or "")
                    if not keys:
                        ui.label("No keywords yet").classes("text-xs").style("color: var(--mf-muted);").move(chips_row)
                        return
                    for k in keys[:14]:
                        ui.badge(k).classes("q-pa-sm").style(
                            "background: rgba(255,255,255,0.06); color: var(--mf-text); border: 1px solid var(--mf-border); border-radius: 999px;"
                        ).move(chips_row)
                    if len(keys) > 14:
                        ui.label(f"+{len(keys)-14} more").classes("text-xs").style("color: var(--mf-muted);").move(chips_row)

                def set_editor(keyword_raw: str, category: str) -> None:
                    kw_input.value = keyword_raw or ""
                    cat_input.value = category or ""
                    refresh_chips()
                    mode_badge.text = "Selected" if state["selected_kw"] else "New"

                def select_rule(kw: str) -> None:
                    state["selected_kw"] = kw
                    row = rdf_view.loc[rdf_view["keyword"] == kw]
                    if not row.empty:
                        r0 = row.iloc[0]
                        set_editor(str(r0.get("keyword", "")), str(r0.get("category", "")))
                    else:
                        set_editor("", "")
                    mode_badge.text = "Selected"

                def clear_selection() -> None:
                    state["selected_kw"] = None
                    set_editor("", "")
                    mode_badge.text = "New"
                    # list highlight refresh happens via render_list caller

                def save_rule() -> None:
                    keys = parse_keywords(kw_input.value or "")
                    keyword_str = keywords_to_string(keys)
                    category_str = (cat_input.value or "").strip()

                    if not keyword_str:
                        ui.notify("Enter at least one keyword", type="warning")
                        return
                    if not category_str:
                        ui.notify("Enter a category", type="warning")
                        return

                    old_kw = state["selected_kw"]
                    if old_kw:
                        if old_kw != keyword_str:
                            if delete_row_by_id("rules", "keyword", str(old_kw)):
                                append_row("rules", {"keyword": keyword_str, "category": category_str})
                            else:
                                ui.notify("Update failed (could not remove old rule)", type="negative")
                                return
                        else:
                            if not update_row_by_id("rules", "keyword", keyword_str, {"category": category_str}):
                                ui.notify("Update failed", type="negative")
                                return
                        invalidate("rules")
                        ui.notify("Rule saved", type="positive")
                        nav_to("/rules")
                    else:
                        append_row("rules", {"keyword": keyword_str, "category": category_str})
                        invalidate("rules")
                        ui.notify("Rule added", type="positive")
                        nav_to("/rules")

                def delete_rule() -> None:
                    old_kw = state["selected_kw"]
                    if not old_kw:
                        ui.notify("Select a rule on the left", type="warning")
                        return
                    if delete_row_by_id("rules", "keyword", str(old_kw)):
                        invalidate("rules")
                        ui.notify("Deleted", type="positive")
                        nav_to("/rules")
                    else:
                        ui.notify("Delete failed", type="negative")

                # Bind events
                kw_input.on("input", lambda e: refresh_chips())
                save_btn.on("click", lambda e: save_rule())
                del_btn.on("click", lambda e: delete_rule())

                # init
                refresh_chips()

            # After editor defined, we can define list events that call clear_selection/select_rule
            # (functions exist in scope now)

            # Now render list with functions available
            render_list()

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