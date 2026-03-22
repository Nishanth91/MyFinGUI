# -*- coding: utf-8 -*-
# ======================================
# FinTrackr App  Phase 4.6A (REAL FIX BUILD)
# Changes vs P4.5: Dashboard hero, Rules selection, OCR toast timeout, richer palette
# ======================================

# ==============================
# FinTrackr App  Phase 4.5 (P4.4 + P4.5 combined)
# Base: Myfin_NICEGUI_VF2_P4_2 (last stable)
# Changes: Budgets setup UX, Transactions table mobile UX, Rules edit, Cards utilization bars,
#          Dashboard pay-period view, Premium login styling
# ==============================

"""
FinTrackr  NiceGUI Stable
File: Myfin_NICEGUI_VF2_P4_2.py

Purpose
- A stable NiceGUI implementation that you can deploy on Render and use instead of Streamlit.
- Focus on correctness + usability + a consistent dark "banking style" UI.

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
APP_VERSION = '9.12'


def log(message: str) -> None:
    """Log a message to stdout and the configured logger."""
    try:
        _logger.info(message)
    except Exception:
        print(message)

# Simple in-memory cache for worksheet->DataFrame
_df_cache: dict[tuple[str, str], object] = {}
# UI bootstrapping state (Render can import/execute in different orders)
# These must exist before ensure_tabs() runs to avoid NameError during startup.
_ws = None  # websocket handle used by NiceGUI bootstrap (set later)
_tabs_ready: bool = False
_tabs_ready_at: float = 0.0
_gc = None  # gspread client cache
_ss = None  # spreadsheet cache
_header_cache = {}  # sheet headers cache
_migrated_tx_ids: bool = False  # migration guard for TxId->id backfill


import os
import json
import re
import math
import time
import calendar
import hashlib
import base64
from io import BytesIO
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
from nicegui import ui, app, run

# ---------------------------
# Minimal OCR API endpoint (Phase 6.5 HF5)
# ---------------------------
from fastapi import Body
@app.post('/api/ocr_server')
async def _api_ocr_server(payload: dict = Body(...)):
    try:
        data_url = str(payload.get('data_url') or '')
        text = server_ocr_from_data_url(data_url)
        return {'ok': True, 'text': text}
    except Exception as e:
        return {'ok': False, 'error': str(e)}

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
except Exception as e:
    log(f"[FinTrackr] Html sanitize patch skipped: {e}")
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse, Response

# ---------------------------
# Apple Touch Icon (iOS home screen bookmark)
# ---------------------------
_apple_touch_icon_cache: Optional[bytes] = None

@app.get('/apple-touch-icon.png')
@app.get('/apple-touch-icon-precomposed.png')
@app.get('/apple-touch-icon-180x180.png')
@app.get('/apple-touch-icon-152x152.png')
async def _apple_touch_icon():
    """Serve a 180x180 PNG for iOS home screen bookmarks.

    Matches the app header icon: dark-to-emerald gradient background
    with the Material 'insights' icon shape rendered in gold.
    Pure Python PNG generator, no PIL needed.
    """
    global _apple_touch_icon_cache
    if _apple_touch_icon_cache is not None:
        return Response(content=_apple_touch_icon_cache, media_type='image/png')

    import struct, zlib, math
    W = H = 180

    def _lerp(a, b, t):
        return a + (b - a) * t
    def _lerp3(c1, c2, t):
        return (int(_lerp(c1[0], c2[0], t)), int(_lerp(c1[1], c2[1], t)), int(_lerp(c1[2], c2[2], t)))
    def _blend(bg, fg, a):
        return (int(bg[0]*(1-a)+fg[0]*a), int(bg[1]*(1-a)+fg[1]*a), int(bg[2]*(1-a)+fg[2]*a))
    def _sdf_circle(px, py, ccx, ccy, rad):
        return math.sqrt((px-ccx)**2+(py-ccy)**2) - rad
    def _smoothstep(e0, e1, x):
        t = max(0.0, min(1.0, (x-e0)/(e1-e0)))
        return t*t*(3-2*t)
    def _dist_to_segment(px, py, x1, y1, x2, y2):
        dx, dy = x2-x1, y2-y1
        l2 = dx*dx+dy*dy
        if l2 == 0: return math.sqrt((px-x1)**2+(py-y1)**2)
        t = max(0, min(1, ((px-x1)*dx+(py-y1)*dy)/l2))
        return math.sqrt((px-x1-t*dx)**2+(py-y1-t*dy)**2)

    # "Insights" icon geometry: zigzag trend line with dots, plus a sparkle
    # All coords in 180x180 space, centered
    # Main zigzag line (the core of the Material insights icon)
    # 4 nodes going up-down-up pattern, scaled to fill nicely
    nodes = [
        (38, 130),   # bottom-left
        (72, 72),    # up
        (108, 108),  # down
        (142, 50),   # top-right
    ]
    dot_r = 9.0       # radius of the node dots
    line_w = 5.0       # line half-width
    # Sparkle star at top-right (like the insights icon has)
    star_cx, star_cy = 148, 40
    # Small sparkle bottom-left
    star2_cx, star2_cy = 30, 130

    GOLD = (251, 191, 36)
    GOLD_LT = (253, 224, 120)

    pixels = bytearray()
    for y in range(H):
        pixels.append(0)  # PNG filter byte
        for x in range(W):
            # --- 1. Background: dark-to-emerald diagonal gradient (matching header badge) ---
            t_diag = (x / W * 0.6 + y / H * 0.4)  # diagonal blend factor
            bg_dark = (15, 25, 35)      # #0F1923
            bg_emerald = (20, 90, 60)   # dark emerald
            bg = _lerp3(bg_dark, bg_emerald, t_diag)

            # Subtle radial highlight near center
            cd = math.sqrt((x-90)**2+(y-90)**2) / 130
            if cd < 1.0:
                bg = _blend(bg, (25, 50, 42), (1-cd)*0.15)

            r, g, b = bg

            # --- 2. Trend line segments (gold, anti-aliased) ---
            min_seg_d = 999.0
            for i in range(len(nodes)-1):
                d = _dist_to_segment(x, y, nodes[i][0], nodes[i][1], nodes[i+1][0], nodes[i+1][1])
                min_seg_d = min(min_seg_d, d)

            # Outer glow
            if min_seg_d < 16.0:
                ga = (1.0 - _smoothstep(line_w+1, 16.0, min_seg_d)) * 0.15
                r, g, b = _blend((r,g,b), GOLD, ga)
            # Core line
            if min_seg_d < line_w + 1.5:
                la = 1.0 - _smoothstep(line_w - 0.5, line_w + 1.5, min_seg_d)
                r, g, b = _blend((r,g,b), GOLD, la * 0.95)

            # --- 3. Node dots (filled gold circles with white center highlight) ---
            for nx, ny in nodes:
                dd = _sdf_circle(x, y, nx, ny, dot_r)
                # Outer glow ring
                if dd < 6.0 and dd > -dot_r:
                    rga = (1.0 - _smoothstep(0.0, 6.0, dd)) * 0.12
                    r, g, b = _blend((r,g,b), GOLD, rga)
                # Filled circle
                if dd < 1.5:
                    fa = 1.0 - _smoothstep(-0.5, 1.5, dd)
                    r, g, b = _blend((r,g,b), GOLD, fa * 0.95)
                # White center highlight
                inner_d = _sdf_circle(x, y, nx-1.5, ny-1.5, dot_r*0.35)
                if inner_d < 1.0:
                    ha = (1.0 - _smoothstep(-0.5, 1.0, inner_d)) * 0.35
                    r, g, b = _blend((r,g,b), (255,255,255), ha)

            # --- 4. Sparkle (4-point star) at top-right ---
            for scx, scy, sr in [(star_cx, star_cy, 11), (star2_cx, star2_cy, 6)]:
                sdx, sdy = abs(x-scx), abs(y-scy)
                # 4-point star: thin cross shape with falloff
                cross_d = min(sdx + sdy*3.5, sdx*3.5 + sdy)
                if cross_d < sr * 2.5:
                    sa = (1.0 - cross_d / (sr * 2.5)) ** 1.8
                    r, g, b = _blend((r,g,b), GOLD_LT, sa * 0.75)
                # Center bright dot
                cd2 = math.sqrt(sdx**2+sdy**2)
                if cd2 < sr*0.45:
                    ca = (1.0 - cd2 / (sr*0.45)) * 0.9
                    r, g, b = _blend((r,g,b), (255,255,255), ca)

            # Clamp
            r = max(0, min(255, int(r)))
            g = max(0, min(255, int(g)))
            b = max(0, min(255, int(b)))
            pixels.extend((r, g, b))

    def _make_png(width, height, raw):
        def _chunk(ctype, data):
            c = ctype + data
            return struct.pack('>I', len(data)) + c + struct.pack('>I', zlib.crc32(c) & 0xffffffff)
        sig = b'\x89PNG\r\n\x1a\n'
        ihdr = struct.pack('>IIBBBBB', width, height, 8, 2, 0, 0, 0)
        idat = zlib.compress(bytes(raw), 9)
        return sig + _chunk(b'IHDR', ihdr) + _chunk(b'IDAT', idat) + _chunk(b'IEND', b'')

    _apple_touch_icon_cache = _make_png(W, H, pixels)
    return Response(content=_apple_touch_icon_cache, media_type='image/png')


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
    except Exception as e:
        log(f"[FinTrackr] nav_to v2 failed: {e}")
    try:
        # Older style (if present)
        if hasattr(ui, 'open'):
            ui.open(path)  # type: ignore[attr-defined]
            return
    except Exception as e:
        log(f"[FinTrackr] nav_to open failed: {e}")
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
CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", "60"))  # seconds  reduced to 60s so sheet edits reflect faster

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
        "COSTCO GAS",
        "COSTCO",
        "GILL'S SUPERMARKET",
        "GILL'S",
        "BOMBAY SPICES",
        "DINO'S",
        "SUPERSTORE",
        "PETRO CANADA",
        "PETRO-CANADA",
        "SHELL",
        "CO-OP",
        "ESSO",
        "DOLLARAMA",
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
      - MM/DD/YYYY (e.g., Gill's 12/8/2025)
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

    Confidence is a heuristic score in [0..6] where:
      - 0 means nothing found
      - 2 means a weak guess
      - 4+ means strong enough to skip the "low confidence" warning
    """
    t = (text or "")
    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    if not lines:
        return None, 0.0, ""

    # helper
    def _to_num(s: str) -> Optional[float]:
        try:
            s = s.replace(",", "").strip()
            return float(s)
        except Exception:
            return None

    # patterns: allow optional currency, trailing tax-code letter (H/D/E/etc)
    num_pat = r"(?P<amt>\d{1,5}(?:[\.,]\d{2}))"
    amt_re = re.compile(num_pat)

    # We score candidates; later lines are generally more trustworthy for totals.
    best_amt: Optional[float] = None
    best_score: float = 0.0
    best_src: str = ""

    # Pre-compute how many times each amount appears (helps with receipts where TOTAL and TEND repeat)
    all_amounts = []
    for ln in lines:
        for m in amt_re.finditer(ln.replace("$", "")):
            v = _to_num(m.group("amt").replace(",", "").replace(" ", "").replace(".", "."))
            if v is not None:
                all_amounts.append(round(v, 2))
    freq = {}
    for v in all_amounts:
        freq[v] = freq.get(v, 0) + 1

    # keyword tiers
    strong_kw = (" total", "total ", "grand total", "amount due", "balance due")
    mid_kw = ("mcard tend", "visa tend", "debit tend", "tend", "paid", "purchase", "total purchase")
    weak_kw = ("subtotal", "sub total")

    for i, ln_raw in enumerate(lines):
        ln = ln_raw.lower()
        # Skip obvious noise-only header lines
        if len(ln) < 4:
            continue

        # Find the right-most amount on the line (often the relevant one)
        matches = list(amt_re.finditer(ln_raw.replace("$", "")))
        if not matches:
            continue
        m_last = matches[-1]
        amt_s = m_last.group("amt").replace(",", "").strip()
        val = _to_num(amt_s)
        if val is None:
            continue
        val = round(val, 2)

        score = 0.0

        # position weight: later lines => higher
        pos_w = (i + 1) / max(1, len(lines))
        score += 1.0 * pos_w  # 0..1

        # keyword weight
        if any(k in ln for k in strong_kw):
            score += 4.0
            src = "total"
        elif any(k in ln for k in mid_kw):
            score += 3.0
            src = "tender"
        elif any(k in ln for k in weak_kw):
            # subtotal is useful but weaker
            score += 1.0
            src = "subtotal"
        else:
            score += 0.5
            src = "number"

        # If the same amount repeats multiple times on the receipt, bump confidence.
        if freq.get(val, 0) >= 2:
            score += 1.0
        if freq.get(val, 0) >= 3:
            score += 0.5

        # Sanity: totals rarely are 0.00
        if val <= 0:
            score -= 2.0

        if score > best_score:
            best_score = score
            best_amt = val
            best_src = src

    if best_amt is None:
        return None, 0.0, ""

    # Clamp into [0..6]
    best_score = max(0.0, min(6.0, best_score))
    return best_amt, best_score, best_src

def _extract_card_last4(text: str) -> str:
    """Try to find last-4 digits of card, if printed."""
    # common formats: **** 1234, XXXX1234, x1234
    patterns = [
        r"(?:\*{2,}\s*){2,}(\d{4})",
        r"X{2,}\s*(\d{4})",
        r"(?:VISA|MASTERCARD|MASTER CARD|MC|DEBIT)\D{0,60}(\d{4})",
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



# ---------------------------
# Server-side OCR fallback (Phase 6.5 HF5)
# ---------------------------
def _get_google_vision_client():
    """Create (and cache) a Google Cloud Vision client from env JSON, if available."""
    global _gcv_client
    try:
        return _gcv_client  # type: ignore[name-defined]
    except Exception:
        pass
    try:
        import os, json
        from google.cloud import vision  # type: ignore
        from google.oauth2 import service_account  # type: ignore

        raw = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON', '') or ''
        if not raw.strip():
            _gcv_client = None
            return None
        info = json.loads(raw)
        creds = service_account.Credentials.from_service_account_info(info)
        _gcv_client = vision.ImageAnnotatorClient(credentials=creds)
        return _gcv_client
    except Exception:
        try:
            _gcv_client = None
        except Exception:
            pass
        return None


def _get_gcp_vision_sa_info() -> Optional[dict]:
    """Return service account info dict for Google Vision, if configured."""
    raw = (os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON') or
           os.getenv('GOOGLE_VISION_CREDENTIALS_JSON') or
           os.getenv('GOOGLE_VISION_CREDENTIALS') or
           os.getenv('GOOGLE_VISION_JSON') or
           os.getenv('SERVICE_ACCOUNT_JSON') or '')
    raw = (raw or '').strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        # Sometimes users paste with leading/trailing quotes or escaped newlines
        try:
            return json.loads(raw.encode('utf-8').decode('unicode_escape'))
        except Exception:
            return None


def _load_json_from_env(*keys: str):
    """Load a JSON object from the first non-empty env var in *keys*.

    Accepts:
      - raw JSON (starts with '{')
      - base64-encoded JSON
      - a filesystem path to a JSON file (last resort)

    Returns dict on success, or None.
    """
    import os, json, base64

    for k in keys:
        if not k:
            continue
        v = os.getenv(k, "")
        if not v:
            continue
        v = v.strip()
        # Raw JSON
        if v.startswith("{") and v.endswith("}"):
            try:
                return json.loads(v)
            except Exception:
                pass
        # Base64 JSON
        try:
            decoded = base64.b64decode(v, validate=True).decode("utf-8", "ignore").strip()
            if decoded.startswith("{") and decoded.endswith("}"):
                return json.loads(decoded)
        except Exception:
            pass
        # File path
        try:
            if os.path.exists(v) and os.path.isfile(v) and v.lower().endswith(".json"):
                with open(v, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
    return None


def _google_vision_rest_ocr(image_bytes: bytes) -> Tuple[str, str]:
    """Google Cloud Vision OCR via REST.

    Returns (text, debug_msg). On errors, text=="" and debug_msg contains details.
    """
    debug = ""
    try:
        import requests
        from google.oauth2 import service_account
        from google.auth.transport.requests import Request as GoogleAuthRequest

        sa_info = _load_json_from_env(
            "GOOGLE_VISION_CREDENTIALS_JSON",
            "GOOGLE_APPLICATION_CREDENTIALS_JSON",
            "SERVICE_ACCOUNT_JSON",
        )
        if not sa_info:
            return "", "Missing credentials env. Set GOOGLE_VISION_CREDENTIALS_JSON (or SERVICE_ACCOUNT_JSON)."

        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        creds = service_account.Credentials.from_service_account_info(sa_info, scopes=scopes)

        creds.refresh(GoogleAuthRequest())
        token = getattr(creds, "token", None)
        if not token:
            return "", "Could not obtain access token from service account."

        b64 = base64.b64encode(image_bytes).decode("utf-8")
        url = "https://vision.googleapis.com/v1/images:annotate"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"requests": [{"image": {"content": b64}, "features": [{"type": "TEXT_DETECTION"}]}]}

        resp = requests.post(url, headers=headers, json=payload, timeout=25)
        if resp.status_code != 200:
            detail = (getattr(resp, "text", "") or "")[:900]
            return "", f"Vision API HTTP {resp.status_code}: {detail}"

        data = resp.json()
        if "error" in data:
            return "", f"Vision API error: {json.dumps(data['error'])[:900]}"

        ann = (data.get("responses") or [{}])[0].get("fullTextAnnotation") or {}
        text = (ann.get("text") or "").strip()
        if not text:
            # Keep a tiny hint for debugging
            debug = "Vision returned empty text (no fullTextAnnotation.text)."
        return text, debug

    except Exception as e:
        return "", f"{type(e).__name__}: {e}"

def _decode_data_url_to_bytes(data_url: str) -> bytes:
    """Decode a browser data URL (data:<mime>;base64,....) into raw bytes.

    Accepts:
      - data URLs (base64 or urlencoded)
      - plain base64 strings (best-effort)
    """
    import base64
    import binascii
    from urllib.parse import unquote_to_bytes

    if not data_url:
        return b""

    s = data_url.strip()

    # Typical: data:image/jpeg;base64,/9j/4AAQSk...
    if s.startswith("data:"):
        try:
            header, payload = s.split(",", 1)
        except ValueError:
            return b""

        is_b64 = ";base64" in header.lower()
        if is_b64:
            # tolerate whitespace/newlines in base64
            payload = "".join(payload.split())
            try:
                return base64.b64decode(payload, validate=False)
            except (binascii.Error, ValueError):
                # last resort without validation
                try:
                    return base64.b64decode(payload)
                except Exception:
                    return b""
        else:
            # Non-base64 data URL payload is URL-encoded
            try:
                return unquote_to_bytes(payload)
            except Exception:
                return payload.encode("utf-8", errors="ignore")

    # If it's not a data URL, attempt base64 decode best-effort.
    # This covers cases where the UI gives only the base64 payload.
    try:
        s2 = "".join(s.split())
        return base64.b64decode(s2, validate=False)
    except Exception:
        return s.encode("utf-8", errors="ignore")

def server_ocr_from_data_url(data_url: str, *, return_debug: bool = False):
    """Server-side OCR entrypoint.

    Returns:
      - if return_debug=False (default): just the OCR text (str)
      - if return_debug=True: (text, debug_msg)
    """
    debug_msg = ""
    try:
        img_bytes = _decode_data_url_to_bytes(data_url)
        if not img_bytes:
            debug_msg = "No image bytes decoded from upload."
            return ("", debug_msg) if return_debug else ""

        # Enforce Google Cloud Vision to fix accuracy
        text, dbg = _google_vision_rest_ocr(img_bytes)
        debug_msg = dbg or debug_msg
        if not text:
            debug_msg += " (Google Cloud Vision OCR returned empty)"
        return (text, debug_msg) if return_debug else text

    except Exception as e:
        debug_msg = f"{type(e).__name__}: {e}"
        return ("", debug_msg) if return_debug else ""

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
    "cards": ["card_name", "owner", "billing_day", "max_limit", "method_name", "card_last4"],
    "recurring": ["recurring_id", "owner", "type", "amount", "method", "account", "category", "notes",
                  "day_of_month", "start_date", "active", "last_generated_month"],
    "rules": ["keyword", "category"],
    "locks": ["month", "locked"],
}


# -----------------------------
# Phase 6.5: OCR line-item intelligence
# -----------------------------

_PRICE_RE = re.compile(r"(?P<price>\d{1,6}\.\d{2})(?:\s*[A-Z])?\b")
# 9.8: Quantity pattern "2 @ 3.99" or "2 x 3.99" or "2 AT 3.99 ea" → captures qty and unit price
_QTY_PRICE_RE = re.compile(r"(?P<qty>\d+)\s*(?:@|x|X|AT|at)\s*\$?\s*(?P<unit>\d{1,6}\.\d{2})")

# 9.8: Walmart/Costco department code prefixes → category hints
_DEPT_CODE_MAP = {
    'gr': 'Groceries', 'gro': 'Groceries', 'gry': 'Groceries', 'fd': 'Groceries', 'food': 'Groceries',
    'hba': 'Health', 'rx': 'Health', 'phm': 'Health', 'phr': 'Health', 'hlth': 'Health',
    'hh': 'Household', 'hhold': 'Household', 'hm': 'Household', 'clng': 'Household',
    'gm': 'Shopping', 'ap': 'Shopping', 'el': 'Shopping', 'toy': 'Shopping',
}

def _is_noise_receipt_line(line: str) -> bool:
    l = (line or '').strip().lower()
    if not l:
        return True
    noise_words = [
        'subtotal', 'sub total', 'total', 'gst', 'pst', 'hst', 'tax', 'balance', 'change',
        'debit', 'credit', 'visa', 'mastercard', 'approval', 'approved', 'auth', 'aid',
        'terminal', 'term', 'tran', 'trans', 'transaction', 'ref', 'trace', 'invoice',
        'cash', 'tender', 'thank you', 'survey', 'items sold', 'reg', 'operator',
        'st#', 'tr#', 'store #', 'store:', 'pos', 'order', 'barcode',
        'rewards', 'loyalty', 'savings', 'you saved', 'member', 'coupon',
        'receipt', 'duplicate', 'copy', 'return policy',
    ]
    if any(w in l for w in noise_words):
        return True
    return False

def extract_receipt_line_items(text: str) -> List[Dict[str, Any]]:
    """Extract best-effort line items from OCR text.

    Returns list of dicts: {name:str, price:float, section_hint:str|None}

    Supports two patterns seen in receipts / OCR:
    1) "ITEM NAME ... $12.34" on the SAME line
    2) "ITEM NAME ..." on one line and the price on the NEXT line
       (common with Google Vision line breaks)

    We intentionally keep this lightweight and deterministic: no ML here.
    Section headers (like PHARMACY, GROCERY, etc.) are tracked so that items
    underneath a section inherit a category hint.
    """
    items: list[dict[str, Any]] = []
    if not text:
        return items

    lines = [ln.strip() for ln in str(text).splitlines() if ln and ln.strip()]
    prev_candidate: str | None = None

    # Track section headers (Walmart/Costco receipts often have PHARMACY, GROCERY, etc.)
    _SECTION_MAP = {
        'pharmacy': 'Health', 'pharm': 'Health', 'rx': 'Health', 'drug': 'Health',
        'health': 'Health', 'otc': 'Health', 'wellness': 'Health',
        'household': 'Household', 'hhold': 'Household', 'home': 'Household',
        'cleaning': 'Household', 'hba': 'Health',
        'grocery': 'Groceries', 'groceries': 'Groceries', 'produce': 'Groceries',
        'dairy': 'Groceries', 'bakery': 'Groceries', 'deli': 'Groceries', 'meat': 'Groceries',
        'frozen': 'Groceries', 'fresh': 'Groceries',
        'apparel': 'Shopping', 'clothing': 'Shopping', 'electronics': 'Shopping',
        'toys': 'Shopping', 'garden': 'Shopping',
    }
    current_section: str | None = None

    def _clean_name(s: str) -> str:
        s = s.replace('CAD', '').replace('$', '').strip(" -:|")
        # Remove long numeric codes (SKU/UPC) but keep short quantities (e.g., 2 AT 1 FOR)
        s = re.sub(r"\b\d{6,}\b", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        # Remove common trailing one-letter tax markers (E/D/H/C etc.)
        s = re.sub(r"\s+[A-Z]\b", "", s).strip()
        # 9.8: Strip Walmart/Costco dept code prefixes (e.g., "GR BANANAS" → "BANANAS")
        s_upper = s.strip()
        for _dc in _DEPT_CODE_MAP:
            _dc_up = _dc.upper()
            if s_upper.startswith(_dc_up + ' ') and len(s_upper) > len(_dc_up) + 2:
                s = s_upper[len(_dc_up):].strip()
                break
        return s

    def _extract_dept_hint(ln: str) -> str | None:
        """9.8: Check if line starts with a department code prefix and return category hint."""
        words = ln.strip().split()
        if words and len(words) >= 2:
            code = words[0].lower().rstrip(':.-')
            if code in _DEPT_CODE_MAP:
                return _DEPT_CODE_MAP[code]
        return None

    def _looks_like_price_only(ln: str) -> bool:
        ln2 = ln.replace('CAD', '').replace('$', '').strip()
        # A price-only line is typically short and mostly numeric punctuation.
        return bool(_PRICE_RE.fullmatch(ln2)) or (bool(_PRICE_RE.search(ln2)) and sum(ch.isalpha() for ch in ln2) == 0 and len(ln2) <= 12)

    for ln in lines:
        if _is_noise_receipt_line(ln):
            continue

        ln2 = ln.replace('CAD', '').strip()

        # Detect section headers (e.g., "** PHARMACY **", "PHARMACY", "--- GROCERY ---")
        header_clean = re.sub(r'[*\-_=\[\]{}()|#]+', ' ', ln2).strip().lower()
        header_clean = re.sub(r'\s+', ' ', header_clean).strip()
        if header_clean and not _PRICE_RE.search(ln2.replace('$', '')):
            for sec_kw, sec_cat in _SECTION_MAP.items():
                if sec_kw in header_clean and len(header_clean) < 30:
                    current_section = sec_cat
                    break

        # 9.8: Check for department code prefix → update section hint for this item
        _dept_hint = _extract_dept_hint(ln2)
        _item_section = _dept_hint or current_section

        # 9.8: Handle quantity patterns (e.g., "2 @ 3.99" → total = 7.98)
        qm = _QTY_PRICE_RE.search(ln2)
        if qm:
            try:
                qty = int(qm.group('qty'))
                unit_price = float(qm.group('unit'))
                price = round(qty * unit_price, 2)
                raw_name_part = ln2[:qm.start()]
                raw_name = _clean_name(raw_name_part)
                if (not raw_name) and prev_candidate:
                    raw_name = _clean_name(prev_candidate)
                if raw_name and len(raw_name) >= 2 and not _is_noise_receipt_line(raw_name):
                    items.append({'name': raw_name, 'price': price, 'section_hint': _item_section})
                    prev_candidate = None
                    continue
            except Exception:
                pass

        m = _PRICE_RE.search(ln2.replace('$', ''))
        if m:
            try:
                price = float(m.group('price'))
            except Exception:
                price = None

            if price is not None:
                raw_name_part = ln2[:m.start('price')]
                raw_name = _clean_name(raw_name_part)
                # If the line is basically just a price, attach to previous candidate if possible
                if (not raw_name or _looks_like_price_only(ln2)) and prev_candidate:
                    name = _clean_name(prev_candidate)
                else:
                    name = raw_name

                # Filter out obviously non-item lines
                if not name or len(name) < 2:
                    prev_candidate = None
                    continue
                if _is_noise_receipt_line(name):
                    prev_candidate = None
                    continue
                if sum(ch.isdigit() for ch in name) > max(8, int(0.7 * len(name))):
                    prev_candidate = None
                    continue

                items.append({'name': name, 'price': price, 'section_hint': _item_section})
                prev_candidate = None
                continue

        # Not a priced line: keep as candidate for "next-line price" pairing
        # But ignore headers/totals/etc.
        cand = _clean_name(ln2)
        if cand and not _is_noise_receipt_line(cand):
            prev_candidate = cand

    return items

def _build_rules_index(rules: List[Tuple[str, str]]) -> Dict[str, List[str]]:
    """Build a category -> [keywords] index from the rules list.

    Each rule is (keyword, category). We group keywords by their lowercase category name
    so classify_receipt_items can look up keywords per category efficiently.
    """
    idx: Dict[str, List[str]] = {}
    for kw, cat in (rules or []):
        key = (cat or '').strip().lower()
        if not key:
            continue
        kw_clean = (kw or '').strip().lower()
        if not kw_clean:
            continue
        if key not in idx:
            idx[key] = []
        idx[key].append(kw_clean)
    return idx


def classify_receipt_items(items: List[Dict[str, Any]], rules: List[Tuple[str, str]]) -> Dict[str, Any]:
    """Classify receipt line items into Groceries/Household/Shopping/Health using the rules sheet.

    Notes:
      - We intentionally IGNORE merchant/store keywords (e.g., walmart/costco/superstore) for line-item classification,
        because they would otherwise force everything into Groceries.
      - We add a small fallback keyword list for Walmart-style abbreviations & common non-food signals (clothing, toys, RX).
    """
    idx = _build_rules_index(rules)

    def _norm(s: str) -> str:
        s = (s or "").lower()
        # keep alnum and spaces; normalize whitespace
        s = re.sub(r"[^a-z0-9]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    # Merchant/store words to ignore when classifying line items
    IGNORE_ITEM_KEYWORDS = {
        "walmart", "costco", "superstore", "no frills", "nofrills", "save on", "saveon", "freshco",
        "safeway", "gills", "gill", "dinos", "dino", "bombayspices", "bombay spices",
        "grocery", "groceries",
    }

    def _filter_keywords(words: List[str]) -> List[str]:
        out: List[str] = []
        for w in words or []:
            w2 = _norm(w)
            if not w2:
                continue
            if w2 in IGNORE_ITEM_KEYWORDS:
                continue
            # avoid ultra-short noisy tokens
            if len(w2) < 3:
                continue
            out.append(w2)
        # unique preserving order
        seen = set()
        uniq: List[str] = []
        for w2 in out:
            if w2 not in seen:
                seen.add(w2)
                uniq.append(w2)
        return uniq

    kw_groc = _filter_keywords(idx.get('groceries', []) + idx.get('grocery', []))
    kw_house = _filter_keywords(idx.get('household', []) + idx.get('house hold', []))
    kw_shop = _filter_keywords(idx.get('shopping', []) + idx.get('shop', []))
    kw_health = _filter_keywords(idx.get('health', []) + idx.get('medical', []))

    # Hard fallback signals (for receipts with abbreviated items)
    fb_shop = _filter_keywords([
        'shirt', 'tshirt', 't-shirt', 'jeans', 'pant', 'pants', 'dress', 'top', 'bra', 'brief', 'sock', 'socks', 'shoe', 'shoes',
        'tank', 'tank top', 'tanktop', 'cami', 'camisole', 'tunic', 'romper', 'jumpsuit', 'bodysuit',
        'women', 'womens', "women's", 'mens', "men's", 'boys', 'girls', 'kids', 'infant', 'toddler',
        'legging', 'leggings', 'tights', 'jogger', 'joggers', 'sweatpant', 'sweatpants', 'trackpant',
        'polo', 'henley', 'cardigan', 'vest', 'blazer', 'suit', 'tie', 'scarf', 'glove', 'gloves', 'hat', 'cap', 'beanie',
        'sandal', 'sandals', 'boot', 'boots', 'sneaker', 'sneakers', 'slipper', 'slippers', 'flip flop',
        'toy', 'toys', 'lego', 'doll', 'stroller', 'electronics', 'headphone', 'headphones', 'earbuds', 'charger',
        'game', 'switch', 'ps5', 'xbox', 'playstation', 'nintendo', 'beauty', 'lotion', 'makeup', 'cosmetic',
        'jewelry', 'necklace', 'bracelet', 'watch', 'handbag', 'purse', 'backpack', 'luggage', 'suitcase',
        'jacket', 'coat', 'hoodie', 'sweater', 'blouse', 'skirt', 'shorts', 'underwear', 'apparel', 'clothing',
        'fabric', 'curtain', 'curtains', 'bedding', 'pillow', 'duvet', 'comforter', 'blanket',
        'laptop', 'tablet', 'phone case', 'cable', 'adapter', 'keyboard', 'mouse', 'speaker',
        'candle', 'decor', 'decoration', 'frame', 'picture frame', 'vase', 'plant pot',
        'swimwear', 'bikini', 'trunks', 'robe', 'pajama', 'pyjama', 'nightgown', 'lingerie',
    ])
    fb_house = _filter_keywords([
        'detergent', 'laundry', 'bleach', 'dish', 'soap', 'shampoo', 'conditioner', 'toothpaste', 'toothbrush',
        'paper towel', 'towel', 'towels', 'toilet paper', 'tissue', 'tissues', 'napkin', 'napkins',
        'trash', 'garbage', 'bag', 'bags', 'cleaner', 'disinfect', 'disinfectant', 'floor', 'scrub', 'softener',
        'sponge', 'sponges', 'wipes', 'wipe', 'mop', 'broom', 'dustpan', 'vacuum',
        'batteries', 'battery', 'light bulb', 'bulb', 'lightbulb', 'hanger', 'hangers',
        'foil', 'aluminum foil', 'cling wrap', 'plastic wrap', 'ziploc', 'ziplock',
        'laundry basket', 'hamper', 'dish soap', 'dishwasher', 'rinse aid',
        'air freshener', 'freshener', 'deodorizer', 'febreze',
        'lysol', 'clorox', 'windex', 'pledge', 'swiffer', 'drano', 'ajax', 'comet',
        'bin liner', 'bin liners', 'garbage bag', 'trash bag',
        'pet food', 'dog food', 'cat food', 'cat litter', 'litter',
        # Extended household signals (Walmart abbreviations & common items)
        'household', 'hhold', 'hh ', 'home care', 'cleaning', 'clean supply',
        'tide', 'gain', 'downy', 'bounce', 'oxiclean', 'resolve', 'shout', 'spray nine',
        'dawn', 'palmolive', 'cascade', 'finish', 'fairy', 'method cleaner',
        'glad', 'hefty', 'glad bag', 'hefty bag', 'glad wrap', 'saran',
        'bounty', 'charmin', 'cottonelle', 'scott', 'viva', 'royale', 'purex',
        'glade', 'renuzit', 'air wick', 'airwick', 'plug in', 'candle warmer',
        'toilet bowl', 'bowl cleaner', 'toilet cleaner', 'drain cleaner',
        'furniture polish', 'wood cleaner', 'glass cleaner', 'stainless steel cleaner',
        'rubber gloves', 'latex gloves', 'cleaning gloves', 'dust cloth', 'microfiber',
        'shelf liner', 'drawer liner', 'contact paper', 'storage bin', 'storage box',
        'clothespin', 'clothespins', 'ironing', 'starch', 'fabric spray',
        'pest control', 'mouse trap', 'ant trap', 'roach', 'raid', 'off spray',
        'door mat', 'bath mat', 'shower curtain', 'curtain rod', 'curtain ring',
        'command hook', 'command strip', 'adhesive hook', 'wall hook',
    ])
    fb_health = _filter_keywords([
        'pharmacy', 'pharm', 'pharma', 'advil', 'tylenol', 'vitamin', 'vitamins', 'bandage', 'bandages', 'ointment',
        'clinic', 'dental', 'dentist', 'doctor', 'hospital', 'chiro', 'chiropractor',
        'medicine', 'medication', 'prescription', 'ibuprofen', 'acetaminophen', 'aspirin',
        'antibiotic', 'antacid', 'allergy', 'benadryl', 'claritin', 'zyrtec', 'reactine',
        'first aid', 'thermometer', 'blood pressure', 'glucometer', 'test strip',
        'sunscreen', 'spf', 'insect repellent', 'bug spray',
        'hand sanitizer', 'sanitizer', 'rubbing alcohol', 'hydrogen peroxide', 'isopropyl',
        'floss', 'dental floss', 'mouthwash', 'listerine',
        'cough', 'cold medicine', 'flu', 'sinus', 'nasal spray', 'throat lozenge',
        'eye drops', 'contact lens', 'contact solution',
        'heating pad', 'ice pack', 'knee brace', 'tensor', 'bandaid', 'band aid',
        'melatonin', 'probiotic', 'supplement', 'supplements', 'omega', 'fish oil', 'multivitamin',
        'diaper', 'diapers', 'baby wipes', 'formula', 'baby formula',
        # Walmart pharmacy / OTC signals
        'otc', 'drug', 'dispens', 'health', 'wellness', 'rx item', 'rx sale',
        'polysporin', 'neosporin', 'pepto', 'gravol', 'tums', 'gaviscon', 'robitussin',
        'mucinex', 'dayquil', 'nyquil', 'vicks', 'halls', 'buckley', 'dimetapp',
        'motrin', 'aleve', 'midol', 'excedrin', 'robax', 'voltaren',
        'calamine', 'hydrocortisone', 'cortisone', 'orajel', 'anbesol',
        'pepcid', 'zantac', 'imodium', 'metamucil', 'dulcolax', 'senokot',
        'sudafed', 'aerius', 'allegra', 'xyzal', 'cetirizine', 'loratadine',
        'pedialyte', 'cepacol', 'strepsils', 'chloraseptic',
        'prenatal', 'folic acid', 'iron supplement', 'calcium supplement', 'vitamin d',
        'vitamin c', 'zinc supplement', 'biotin', 'collagen',
    ])
    fb_groc = _filter_keywords([
        'banana', 'bananas', 'apple', 'apples', 'orange', 'oranges', 'grape', 'grapes', 'mango', 'mangoes',
        'strawberry', 'blueberry', 'raspberry', 'cherry', 'peach', 'pear', 'pears', 'plum', 'kiwi',
        'watermelon', 'cantaloupe', 'pineapple', 'avocado', 'lemon', 'lime', 'coconut', 'fig',
        'milk', 'bread', 'butter', 'cheese', 'cream', 'yogurt', 'yoghurt', 'egg', 'eggs',
        'chicken', 'beef', 'pork', 'lamb', 'turkey', 'salmon', 'fish', 'shrimp', 'tuna', 'steak',
        'rice', 'pasta', 'noodle', 'noodles', 'flour', 'sugar', 'salt', 'pepper', 'spice',
        'cereal', 'oatmeal', 'granola', 'pancake', 'waffle', 'syrup',
        'tofu', 'spinach', 'lettuce', 'kale', 'broccoli', 'cauliflower', 'carrot', 'carrots',
        'potato', 'potatoes', 'onion', 'onions', 'garlic', 'ginger', 'tomato', 'tomatoes',
        'cucumber', 'celery', 'zucchini', 'squash', 'corn', 'beans', 'lentils', 'chickpeas',
        'juice', 'coffee', 'tea', 'water', 'soda', 'pop', 'beverage',
        'chip', 'chips', 'cracker', 'crackers', 'cookie', 'cookies', 'snack', 'snacks',
        'sauce', 'ketchup', 'mustard', 'mayo', 'mayonnaise', 'salsa', 'dressing',
        'jam', 'jelly', 'peanut butter', 'honey', 'nutella',
        'frozen', 'ice cream', 'pizza', 'fries', 'nuggets',
        'deli', 'ham', 'salami', 'bacon', 'sausage',
        'olive oil', 'vegetable oil', 'canola oil', 'cooking oil',
        'bakery', 'baguette', 'croissant', 'muffin', 'donut', 'bagel',
        'organic', 'produce', 'fresh', 'meat', 'seafood', 'poultry',
        # 9.8: Walmart/Costco abbreviated item names & common grocery signals
        'bnls', 'boneless', 'skinless', 'ground', 'lean', 'roast', 'chop', 'fillet', 'filet',
        'hummus', 'guacamole', 'tortilla', 'wrap', 'pita', 'naan', 'roti', 'paratha',
        'canned', 'soup', 'broth', 'stock', 'bouillon', 'ramen', 'instant noodle',
        'nut', 'nuts', 'almond', 'almonds', 'cashew', 'cashews', 'walnut', 'walnuts', 'pecan',
        'dried fruit', 'raisin', 'raisins', 'trail mix', 'protein bar', 'granola bar',
        'condensed milk', 'evaporated milk', 'coconut milk', 'oat milk', 'almond milk', 'soy milk',
        'whip cream', 'sour cream', 'cream cheese', 'cottage cheese', 'cheddar', 'mozzarella',
        'margarine', 'spread', 'ghee', 'cooking spray', 'vinegar', 'soy sauce',
        'masala', 'turmeric', 'cumin', 'cinnamon', 'paprika', 'chili powder', 'curry',
        'baking soda', 'baking powder', 'yeast', 'cornstarch', 'cocoa',
        'chocolate', 'candy', 'gum', 'mint', 'popcorn',
    ])

    # Words that are too short/ambiguous for plain substring matching and need word-boundary checks
    _SHORT_AMBIGUOUS = {'bag', 'bags', 'top', 'bra', 'mop', 'gel', 'oil', 'bar', 'pad', 'tea', 'jam', 'dip', 'ham', 'rub'}

    def _has_any(text_s: str, keywords: List[str]) -> bool:
        words_set = set(text_s.split())
        for k in keywords:
            if not k:
                continue
            if k in _SHORT_AMBIGUOUS:
                # Use word-boundary match for short/ambiguous keywords
                if k in words_set:
                    return True
            else:
                if k in text_s:
                    return True
        return False

    def infer_item_category(item_name: str) -> str:
        t = _norm(item_name)
        if not t:
            return 'Uncategorized'
        # priority: Health -> Shopping -> Household -> Groceries -> Uncategorized
        if _has_any(t, kw_health) or _has_any(t, fb_health):
            return 'Health'
        if _has_any(t, kw_shop) or _has_any(t, fb_shop):
            return 'Shopping'
        if _has_any(t, kw_house) or _has_any(t, fb_house):
            return 'Household'
        if _has_any(t, kw_groc) or _has_any(t, fb_groc):
            return 'Groceries'
        return 'Uncategorized'

    cat_amounts = {c: 0.0 for c in ['Groceries', 'Household', 'Shopping', 'Health', 'Uncategorized']}
    per_item = []

    for it in items or []:
        name = (it.get('name') or '').strip()
        price = float(it.get('price') or 0.0)
        cat = infer_item_category(name)
        # If the item couldn't be categorized by name but we have a section hint
        # from the receipt (e.g., items under "PHARMACY" header), use that hint
        if cat == 'Uncategorized' and it.get('section_hint'):
            cat = str(it['section_hint'])
        cat_amounts[cat] = cat_amounts.get(cat, 0.0) + price
        per_item.append({'name': name, 'price': price, 'cat': cat})

    total = round(sum(cat_amounts.values()), 2)
    # normalize tiny negative/positive noise
    for k in list(cat_amounts.keys()):
        if abs(cat_amounts[k]) < 0.005:
            cat_amounts[k] = 0.0
        cat_amounts[k] = round(cat_amounts[k], 2)

    # If nothing meaningful was classified (e.g., OCR didn't yield line items), leave everything unassigned
    if total <= 0.0:
        cat_amounts = {c: 0.0 for c in ['Groceries', 'Household', 'Shopping', 'Health', 'Uncategorized']}

    # Remove Uncategorized from output if empty (keep output clean for split dialog)
    if cat_amounts.get('Uncategorized', 0.0) < 0.005:
        cat_amounts.pop('Uncategorized', None)

    return {
        'detected_amounts': cat_amounts,
        'detected_total': total,
        'items': per_item,
    }
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
    except Exception as e:
        _logger.warning("Could not list worksheets for diagnostics: %s", e)
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
            except Exception as e:
                _logger.warning("Could not read header row for tab '%s': %s", tab, e)
                cur = []
            if not cur:
                try:
                    w.append_row(headers)
                except Exception as e:
                    _logger.error("Failed to write header row for tab '%s': %s", tab, e)
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
    except Exception as e:
        _logger.warning("Could not read headers for tab '%s': %s", tab, e)
        headers = []

    if not headers:
        headers = TABS[tab]
        try:
            w.append_row(headers)
        except Exception as e:
            _logger.error("Failed to write default headers for tab '%s': %s", tab, e)
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
    except Exception as e:
        log(f"[FinTrackr] read_df_optional({sheet_title}) failed: {e}")
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
    # build batch updates
    batch = []
    for k, v in updates.items():
        key = str(k).lower()
        if key not in lower_map:
            continue
        col_i, _ = lower_map[key]
        cell = gspread.utils.rowcol_to_a1(row_idx, col_i + 1)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            vv = str(v)
        elif isinstance(v, dt.date):
            vv = v.isoformat()
        else:
            vv = '' if v is None else str(v)
        batch.append({'range': cell, 'values': [[vv]]})
    if batch:
        gs_retry(lambda: w.batch_update(batch, value_input_option='USER_ENTERED'))
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
            except Exception as e2:
                log(f"[FinTrackr] ui.notify failed: {e2}")
            return _cache[tab][1].copy()
        try:
            ui.notify(f'Google Sheets read failed for {tab}: {e}', type='negative')
        except Exception as e2:
            log(f"[FinTrackr] ui.notify failed: {e2}")
        df = pd.DataFrame(columns=TABS.get(tab, []))

    _cache[tab] = (now, df.copy())
    return df

def invalidate(*tabs: str) -> None:

    for t in tabs:
        _cache.pop(t, None)

def soft_invalidate(*tabs: str) -> None:
    """9.8.1: Mark cache as stale but keep serving old data for 5 seconds.
    This prevents a thundering-herd re-read when doing rapid back-to-back saves."""
    now = time.time()
    for t in tabs:
        if t in _cache:
            # Set timestamp to 5 seconds before expiry so next read within 5s still uses cache
            _cache[t] = (now - CACHE_TTL + 5, _cache[t][1])


# -----------------------------
# Rules + Category inference
# -----------------------------
def load_rules(force: bool = False) -> List[Tuple[str, str]]:
    """Load category rules from the **rules** sheet only.

    Phase 6.5 change:
    - The legacy/admin `rules_text` source is deprecated and ignored to avoid conflicts.

    Robustness:
    - Some sheets have both "Keywords" and "keyword" columns; one may be mostly empty.
      We auto-pick the keyword column that actually has data (or merge if multiple have data).
    """
    df = cached_df('rules', force=force)

    rules: list[tuple[str, str]] = []
    if df is None or getattr(df, 'empty', True):
        return rules

    cols = list(df.columns)
    # map normalized -> actual col
    lmap = {str(c).strip().lower(): c for c in cols}

    # Category column
    cat_candidates = []
    for c in cols:
        cl = str(c).strip().lower()
        if cl in ('category', 'cat') or 'category' in cl:
            cat_candidates.append(c)
    cat_col = cat_candidates[0] if cat_candidates else (cols[1] if len(cols) >= 2 else (cols[0] if cols else None))

    # Keyword columns (could be more than one)
    kw_candidates = []
    for c in cols:
        cl = str(c).strip().lower()
        if cl in ('keyword', 'keywords', 'key', 'match', 'contains') or 'keyword' in cl:
            kw_candidates.append(c)
    if not kw_candidates and cols:
        kw_candidates = [cols[0]]

    # Pick keyword columns that actually have values
    def _non_empty_count(colname: Any) -> int:
        try:
            s = df[colname]
            return int(s.astype(str).str.strip().replace('nan', '').replace('None', '').ne('').sum())
        except Exception:
            return 0

    kw_candidates = sorted(kw_candidates, key=_non_empty_count, reverse=True)
    best_kw = kw_candidates[0] if kw_candidates else None
    # Also include other keyword columns if they have meaningful content (e.g. >20% of best)
    best_ct = _non_empty_count(best_kw) if best_kw is not None else 0
    use_kw_cols = []
    for c in kw_candidates:
        ct = _non_empty_count(c)
        if ct <= 0:
            continue
        if best_ct <= 0 or ct >= max(1, int(best_ct * 0.2)):
            use_kw_cols.append(c)
    if not use_kw_cols and best_kw is not None:
        use_kw_cols = [best_kw]

    for _, r in df.iterrows():
        cat = str(r.get(cat_col, '')).strip() if cat_col is not None else ''
        if not cat or cat.lower() == 'nan':
            continue

        # merge keywords from all selected keyword columns
        merged = []
        for kc in use_kw_cols:
            v = str(r.get(kc, '')).strip()
            if not v or v.lower() == 'nan':
                continue
            merged.append(v)

        if not merged:
            continue

        key = ','.join(merged)
        parts = [p.strip() for p in re.split(r'[,;\n]+', key) if p.strip()]
        for p in parts:
            rules.append((p.lower(), cat))

    return rules




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
    _header_cache.pop('recurring', None)
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
    # Force-refresh header cache for recurring to avoid stale data
    _header_cache.pop('recurring', None)
    try:
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
    except Exception as e:
        _logger.error("Failed to append recurring template: %s", e)
        raise
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
    except Exception as e:
        _logger.error("Failed to save passkeys data: %s", e)

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
            .replace(/\\+/g,'-').replace(/\\//g,'_').replace(/=+$/g,'');

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
  background: radial-gradient(ellipse 1400px 800px at 10% 8%, var(--mf-g1), transparent 55%),
              radial-gradient(ellipse 1000px 650px at 85% 15%, var(--mf-g2), transparent 52%),
              radial-gradient(ellipse 800px 500px at 50% 95%, rgba(168,85,247,0.06), transparent 50%),
              var(--mf-bg) !important;
  color: var(--mf-text) !important;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

.my-card {
  background: var(--mf-card-bg, linear-gradient(165deg, var(--mf-card-top), var(--mf-card-bottom))) !important;
  border: 1px solid var(--mf-card-border) !important;
  border-radius: 20px !important;
  box-shadow:
    var(--mf-card-shadow, 0 6px 24px rgba(0,0,0,0.22)),
    inset 0 1px 0 rgba(255,255,255,0.08);
  overflow: hidden;
  position: relative;
  transition: transform 0.1s ease, box-shadow 0.1s ease;
}
.my-card::before{
  content:"";
  position:absolute; inset:-1px;
  background:
    radial-gradient(500px 200px at 15% 0%, rgba(255,255,255,0.05), transparent 50%);
  pointer-events:none;
  opacity:0.5;
  border-radius: 20px;
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
      rgba(0,49,104,0.50) 0%,
      rgba(0,91,170,0.22) 30%,
      rgba(0,0,0,0.00) 64%),
    linear-gradient(180deg, var(--mf-card-top), var(--mf-card-bottom)) !important;
  border-color: rgba(0,81,165,0.30) !important;
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
    radial-gradient(520px 240px at 18% 0%, rgba(255,255,255,0.12), transparent 62%),
    radial-gradient(520px 260px at 82% 18%, rgba(0,81,165,0.25), transparent 68%),
    radial-gradient(520px 260px at 70% 92%, rgba(251,191,36,0.10), transparent 72%);
}
.my-card.mf-issuer-loc::before{
  background:
    radial-gradient(520px 240px at 18% 0%, rgba(255,255,255,0.18), transparent 62%),
    radial-gradient(520px 260px at 82% 18%, rgba(99,102,241,0.18), transparent 68%),
    radial-gradient(520px 260px at 70% 92%, rgba(16,185,129,0.10), transparent 72%);
}
.my-card > * { position: relative; }
.my-card:hover{
  transform: translateY(-2px);
  box-shadow:
    0 12px 40px rgba(0,0,0,0.32),
    inset 0 1px 0 rgba(255,255,255,0.12);
}
/* Premium smooth transitions on interactive elements */
* { transition-timing-function: cubic-bezier(0.22, 1, 0.36, 1); }


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
  color: var(--mf-accent) !important;
  background: transparent !important;
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
  color: var(--mf-accent) !important;
  background: transparent !important;
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
  background: rgba(255,255,255,0.03) !important;
  border: 1px solid var(--mf-border) !important;
  border-radius: 16px !important;
  overflow: hidden;
}
.q-table__top, .q-table__bottom {
  background: transparent !important;
  color: var(--mf-text) !important;
}
.q-table thead tr th {
  color: var(--mf-muted) !important;
  background: rgba(255,255,255,0.04) !important;
  font-size: 0.72rem !important;
  font-weight: 600 !important;
  letter-spacing: 0.06em !important;
  text-transform: uppercase !important;
  border-bottom: 1px solid var(--mf-border) !important;
}
.q-table tbody td {
  color: var(--mf-text) !important;
  font-size: 0.88rem !important;
  padding: 10px 12px !important;
}
.q-table tbody tr {
  transition: background 0.15s ease;
}
.q-table tbody tr:nth-child(odd) {
  background: rgba(255,255,255,0.015) !important;
}
.q-table tbody tr:hover {
  background: rgba(var(--mf-accent-rgb, 91,140,255),0.08) !important;
}
.q-table tbody tr.selected {
  background: rgba(var(--mf-accent-rgb, 91,140,255),0.14) !important;
}
.q-btn {
  text-transform: none !important;
  border-radius: 10px !important;
  font-weight: 500 !important;
  letter-spacing: 0.01em !important;
}
.q-btn--unelevated {
  box-shadow: 0 2px 8px rgba(0,0,0,0.12) !important;
}

.mf-top-menu { display: none; }
@media (max-width: 899px) {
  .mf-top-menu { display: inline-flex; }
}

.mf-bottom-nav {
  position: fixed;
  bottom: 12px;
  left: 12px;
  right: 12px;
  z-index: 1000;
  border-radius: 20px;
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
}
@media (min-width: 900px) {
  .mf-bottom-nav { display: none; }
}

.tile {
  cursor: pointer;
  transition: transform .22s cubic-bezier(0.22, 1, 0.36, 1), box-shadow .22s ease, border-color .22s ease;
}
.tile:hover {
  transform: translateY(-4px) scale(1.01);
  box-shadow: 0 12px 30px rgba(0,0,0,0.22);
}
.tile:active {
  transform: translateY(-1px) scale(0.97);
}
/* Global touch optimization  eliminate 300ms tap delay on all interactive elements */
.tile, .my-card[style*="cursor"], .mf-tab, .mf-tab-add, .mf-navbtn, button {
  touch-action: manipulation;
  -webkit-tap-highlight-color: transparent;
}

/* B6: Dialog form fields  slightly larger for touch */
.mf-add-dialog .q-field {
  min-height: 48px !important;
}
.mf-add-dialog .q-field__native,
.mf-add-dialog .q-field__input {
  font-size: 15px !important;
  padding: 6px 2px !important;
}
.mf-add-dialog .q-field__label {
  font-size: 13px !important;
}
/* 8.2.2: Hide labels only on Date & Amount fields (section header suffices) */
.mf-add-dialog .mf-no-label .q-field__label { display: none !important; }

/*  Task 5: ALL dialogs/popups use SOLID opaque backgrounds  */
/* No transparency, no bleed-through, fully readable text */
.q-dialog__backdrop {
  background: rgba(0,0,0,0.55) !important;
  -webkit-backdrop-filter: none !important;
  backdrop-filter: none !important;
}
.q-dialog__inner {
  padding-bottom: env(safe-area-inset-bottom, 0px) !important;
}
.q-dialog__inner > div {
  background: var(--mf-bg) !important;
  border: 1px solid rgba(255,255,255,0.10) !important;
  box-shadow: 0 16px 48px rgba(0,0,0,0.40) !important;
  border-radius: 22px !important;
  animation: mf-dialogIn 0.12s cubic-bezier(0.2,0.9,0.3,1) !important;
  will-change: transform, opacity;
}
@keyframes mf-dialogIn {
  from { opacity: 0; transform: translate3d(0, 10px, 0) scale(0.97); }
  to   { opacity: 1; transform: translate3d(0, 0, 0) scale(1); }
}
html.mf-light .q-dialog__backdrop {
  background: rgba(100,100,120,0.38) !important;
}
html.mf-light .q-dialog__inner > div {
  background: #ffffff !important;
  border: 1px solid rgba(17,24,39,0.10) !important;
  box-shadow: 0 16px 48px rgba(0,0,0,0.12) !important;
}
/* Force ALL .q-card and .my-card inside dialogs to be fully opaque */
.q-dialog__inner > div .q-card,
.q-dialog__inner > div .my-card,
.q-dialog .q-card,
.q-dialog .my-card {
  background: var(--mf-bg) !important;
  border: none !important;
  box-shadow: none !important;
}
html.mf-light .q-dialog__inner > div .q-card,
html.mf-light .q-dialog__inner > div .my-card,
html.mf-light .q-dialog .q-card,
html.mf-light .q-dialog .my-card {
  background: #ffffff !important;
}
/* Kill black bar at bottom when dialog opens (iOS safe-area) */
body.q-body--dialog { overflow: hidden !important; }
.q-dialog { padding-bottom: env(safe-area-inset-bottom, 0px) !important; }

/* Nicer KPI blocks */
.kpi {
  background: linear-gradient(165deg, rgba(255,255,255,0.07), rgba(255,255,255,0.02)) !important;
  border: 1px solid rgba(255,255,255,0.10) !important;
  border-radius: 16px !important;
}
.kpi .kpi-value { letter-spacing: -0.02em; font-feature-settings: 'tnum'; }

/* Budget progress bar */
.mf-progress {
  height: 8px;
  border-radius: 999px;
  background: rgba(255,255,255,0.08);
  overflow: hidden;
}
.mf-progress > div {
  height: 100%;
  background: linear-gradient(90deg, var(--mf-accent), var(--mf-accent2));
  border-radius: 999px;
  transition: width 0.6s cubic-bezier(0.22, 1, 0.36, 1);
}
html.mf-light .mf-progress {
  background: rgba(17,24,39,0.06);
}


/* ================================
   Phase 8.0 Shell Layout (premium banking)
   Desktop: persistent left rail, no hamburger needed
   Mobile: bottom tab bar + hamburger for full nav
   ================================ */
.mf-shell { display: flex; min-height: 100vh; width: 100%; }

/*  Nav Rail  */
/* Desktop: persistent left rail.  Mobile: slides in from RIGHT for easy thumb access (B2). */
.mf-rail {
  width: 86px;
  position: fixed;
  right: 14px;           /* B2: anchor to right edge on mobile */
  top: 14px;
  height: calc(100vh - 28px);
  padding: 0;
  z-index: 50;
  transform: translateX(200%);  /* B2: hidden off-screen to the right */
  transition: transform 160ms cubic-bezier(0.2,0.9,0.3,1);
}
.mf-nav-open .mf-rail { transform: translateX(0); }

.mf-backdrop{
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.40);
  -webkit-backdrop-filter: none;
  backdrop-filter: none;
  z-index: 40;
  display: none;
}
.mf-nav-open .mf-backdrop{ display:block; }

.mf-rail-card{
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: 4px;
  border: 1px solid var(--mf-border);
  background: var(--mf-bg);
  border-radius: 16px;
  box-shadow: 0 8px 28px rgba(0,0,0,0.18);
  padding: 10px 8px;
  overflow-y: auto;
}
.mf-brand{
  height: 38px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 10px;
  border: 1px solid var(--mf-border);
  background: rgba(255,255,255,0.04);
  font-weight: 800;
  font-size: 11px;
  letter-spacing: 0.5px;
  user-select: none;
}
.mf-navbtn .q-btn__content{ flex-direction: column !important; gap: 3px; }
.mf-navbtn{
  width: 100%;
  min-height: 50px;
  border-radius: 12px !important;
  border: 1px solid transparent !important;
  text-transform: none !important;
  transition: none;               /* instant response */
  touch-action: manipulation;
  -webkit-tap-highlight-color: transparent;
  user-select: none;
  -webkit-user-select: none;
}
.mf-navbtn:active {
  transform: scale(0.92);
  background: rgba(var(--mf-accent-rgb, 91,140,255), 0.10) !important;
}
.mf-navbtn.is-active{
  background: rgba(var(--mf-accent-rgb, 91,140,255), 0.14) !important;
  border-color: rgba(var(--mf-accent-rgb, 91,140,255), 0.22) !important;
}
.mf-navbtn .q-btn__content span { font-size: 10px; opacity: 0.7; font-weight: 600; }

/*  Premium Floating Navigation Pill (Mobile)  */
.mf-bottombar {
  position: fixed;
  bottom: 24px; left: 50%; transform: translateX(-50%);
  width: calc(100% - 48px); max-width: 400px;
  z-index: 55;
  display: none;
  align-items: center;
  justify-content: space-around;
  height: 72px;
  -webkit-tap-highlight-color: transparent !important;
  background: rgba(15, 23, 42, 0.85); /* Darker, richer slate */
  backdrop-filter: blur(32px);
  -webkit-backdrop-filter: blur(32px);
  border: 1px solid rgba(255, 255, 255, 0.08); /* More subtle border */
  border-radius: 40px;
  box-shadow: 0 20px 48px rgba(0,0,0,0.6), inset 0 1px 1px rgba(255,255,255,0.15); /* Softer, deeper shadow */
  will-change: transform;
  -webkit-backface-visibility: hidden;
  contain: layout style;
  padding: 0 8px; /* Inner padding */
}
html.mf-light .mf-bottombar {
  background: rgba(255, 255, 255, 0.85);
  border: 1px solid rgba(0, 0, 0, 0.1);
  box-shadow: 0 16px 40px rgba(0,0,0,0.15), inset 0 1px 1px rgba(255,255,255,0.8);
}
.mf-bottombar .mf-tab {
  flex: 1;
  display: flex; align-items: center; justify-content: center;
  cursor: pointer;
  color: var(--mf-muted);
  -webkit-tap-highlight-color: transparent;
  touch-action: manipulation;
  user-select: none; -webkit-user-select: none;
  padding: 8px 0; border: none; background: none; margin: 0 4px; border-radius: 20px;
  transition: all 0.2s cubic-bezier(0.2, 0.8, 0.2, 1);
  position: relative;
  will-change: transform; transform: translateZ(0);
}
.mf-bottombar .mf-tab .q-icon { font-size: 26px; transition: all 0.2s; }
.mf-bottombar a.mf-tab, .mf-bottombar a.mf-tab-add { text-decoration: none !important; color: var(--mf-muted); }
.mf-bottombar a.mf-tab:visited { color: var(--mf-muted); }
.mf-bottombar a.mf-tab.is-active, .mf-bottombar a.mf-tab.is-active:visited { color: var(--mf-accent); }
/* More button is inside the bottom bar nav  inherits bar styling */
.mf-bottombar .mf-more-btn {
  background: none; border: none;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  touch-action: manipulation;
  user-select: none; -webkit-user-select: none;
}
.mf-bottombar .mf-tab.is-active { color: var(--mf-accent); }
.mf-bottombar .mf-tab.is-active .q-icon {
  color: var(--mf-accent);
  background: rgba(var(--mf-accent-rgb, 91,140,255), 0.15); /* Softer background */
  border-radius: 20px; /* Pill shape for active state */
  padding: 8px 16px; /* Wider padding for pill */
  box-shadow: 0 4px 16px rgba(var(--mf-accent-rgb, 91,140,255), 0.25);
  transform: translateY(-2px); /* Slight lift */
}
.mf-bottombar .mf-tab.is-active::after {
  content: '';
  position: absolute;
  bottom: -6px;
  left: 50%;
  transform: translateX(-50%);
  width: 4px; height: 4px;
  border-radius: 50%;
  background-color: var(--mf-accent);
}
.mf-bottombar .mf-tab:active { transform: scale(0.85); }
/* Floating Add Button (Center Pop) - More Dramatic */
.mf-bottombar .mf-tab-add {
  position: relative; top: -24px; /* Higher pop */
  width: 64px; height: 64px; border-radius: 50%;
  background: linear-gradient(135deg, #0EA5E9, #10B981 70%, #059669); /* Richer gradient */
  color: #fff !important;
  display: flex; align-items: center; justify-content: center;
  flex: 0 0 64px; margin: 0 12px;
  box-shadow: 0 16px 32px rgba(16, 185, 129, 0.5), inset 0 2px 8px rgba(255,255,255,0.4); /* Stronger glow */
  cursor: pointer; border: none; -webkit-tap-highlight-color: transparent; touch-action: manipulation; transition: all 0.3s cubic-bezier(0.34, 1.56, 0.64, 1); /* Bouncy transition */
}
.mf-bottombar .mf-tab-add:active { transform: scale(0.85) translateY(4px); box-shadow: 0 4px 12px rgba(16, 185, 129, 0.4); }
.mf-bottombar .mf-tab-add .q-icon { font-size: 36px; color: #fff; text-shadow: 0 2px 4px rgba(0,0,0,0.2); }

/*  8.2.1 Task 3: Compact "More" popup (floats above hamburger icon)  */
.mf-more-popup {
  position: fixed;
  bottom: calc(66px + env(safe-area-inset-bottom, 0px));
  right: 8px;
  z-index: 60;
  display: none;
  flex-direction: column;
  gap: 2px;
  background: var(--mf-bg);
  border: 1px solid var(--mf-border);
  border-radius: 16px;
  box-shadow: 0 8px 32px rgba(0,0,0,0.25);
  padding: 8px;
  min-width: 170px;
  animation: mf-popupIn 0.1s ease-out;
}
@keyframes mf-popupIn {
  from { opacity: 0; transform: translateY(8px) scale(0.95); }
  to   { opacity: 1; transform: translateY(0) scale(1); }
}
html.mf-light .mf-more-popup {
  background: #fff;
  box-shadow: 0 8px 32px rgba(0,0,0,0.10);
}
.mf-more-open .mf-more-popup { display: flex; }
.mf-more-item {
  width: 100%;
  border-radius: 12px !important;
  text-transform: none !important;
  justify-content: flex-start !important;
  min-height: 52px;
  font-size: 15px !important;
  font-weight: 600 !important;
  padding: 8px 16px !important;
  touch-action: manipulation;
  -webkit-tap-highlight-color: transparent;
}
.mf-more-item .q-icon { font-size: 22px !important; }
/* <a> based more-items need link color reset + flex layout */
a.mf-more-item {
  text-decoration: none !important;
  color: var(--mf-text) !important;
  display: flex !important;
  align-items: center !important;
}
a.mf-more-item:visited { color: var(--mf-text) !important; }
a.mf-more-item.is-active,
a.mf-more-item.is-active:visited {
  background: rgba(var(--mf-accent-rgb, 91,140,255), 0.12) !important;
  color: var(--mf-accent) !important;
}
.mf-more-item:active {
  background: rgba(var(--mf-accent-rgb, 91,140,255), 0.10) !important;
}
.mf-more-item.is-active {
  background: rgba(var(--mf-accent-rgb, 91,140,255), 0.12) !important;
  color: var(--mf-accent) !important;
}
.mf-more-backdrop {
  position: fixed; inset: 0; z-index: 58;
  background: transparent; display: none;
}
.mf-more-open .mf-more-backdrop { display: block; }
/* Hide more popup on desktop */
@media (min-width: 901px) {
  .mf-more-popup { display: none !important; }
  .mf-more-backdrop { display: none !important; }
}

/*  Main area  */
@keyframes pageFadeIn {
  0% { opacity: 0; transform: translateY(12px) scale(0.99); }
  100% { opacity: 1; transform: translateY(0) scale(1); }
}
.mf-main { flex: 1; padding: 26px 32px; animation: pageFadeIn 0.4s cubic-bezier(0.2, 0.8, 0.2, 1); }
.mf-header{
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 6px;
  max-width: 1180px;
  margin: 0 auto 16px auto;
  min-height: 56px;
}
.mf-title .t1 { font-size: 18px; font-weight: 900; }
.mf-title .t2 { font-size: 12px; color: var(--mf-muted); }
/* 8.2.2: Transaction table  proper row wrapping + full-row selection */
.mf-tx-table .q-table__container { overflow-x: hidden !important; }
.mf-tx-table td {
  white-space: normal !important;
  word-break: break-word !important;
  line-height: 1.45 !important;
  padding: 10px 8px !important;
}
.mf-tx-table th { padding: 8px !important; }
.mf-tx-table tbody tr {
  cursor: pointer !important;
  transition: background 0.08s ease;
}
.mf-tx-table tbody tr:active {
  background: rgba(var(--mf-accent-rgb, 91,140,255), 0.12) !important;
}
.mf-tx-table .q-table__bottom { padding: 6px 8px !important; }
/* Selection checkbox column smaller on mobile */
@media (max-width: 600px) {
  .mf-tx-table th:first-child, .mf-tx-table td:first-child {
    width: 32px !important; min-width: 32px !important; max-width: 32px !important;
    padding: 4px !important;
  }
}

/* 8.2.2: Header actions row  neat row of icon buttons under title */
.mf-header-actions {
  padding: 2px 0 0 0;
}
.mf-header-actions .q-btn {
  width: 44px !important; height: 44px !important;
  min-width: 44px !important; min-height: 44px !important;
}
.mf-header-actions .q-btn .q-icon {
  font-size: 24px !important;
}
.mf-canvas{
  max-width: 1440px;
  width: 100%;
  margin: 0 auto;
  display: flex;
  flex-direction: column;
  gap: 20px;
  padding-left: 24px;
  padding-right: 24px;
}

/* Premium Layout Helpers */
.mf-h-scroll {
  display: flex; flex-wrap: nowrap; overflow-x: auto;
  scroll-snap-type: x mandatory; -webkit-overflow-scrolling: touch;
  scrollbar-width: none; padding-bottom: 8px; gap: 12px;
}
.mf-h-scroll::-webkit-scrollbar { display: none; }
.mf-h-scroll > * { scroll-snap-align: center; flex: 0 0 auto; }

/*  Desktop (901px): persistent LEFT rail, no hamburger, no bottom bar  */
@media (min-width: 901px) {
  .mf-rail {
    left: 14px; right: auto;                 /* B2: desktop keeps rail on left */
    transform: translateX(0) !important;
  }
  .mf-backdrop { display: none !important; }
  .mf-main { margin-left: 100px; }
  .mf-hamburger { display: none !important; }
  .mf-bottombar { display: none !important; }
  /* 9.11.1: ensure nav buttons are consistently left-aligned */
  .mf-navbtn .q-btn__content { flex-direction: row !important; gap: 8px; justify-content: flex-start !important; }
  .mf-navbtn .q-btn__content .q-icon { font-size: 20px; }
  .mf-navbtn .q-btn__content span { font-size: 12px; opacity: 0.85; font-weight: 600; }
}

/*  Mobile (900px): bottom bar visible, rail is overlay only  */
@media (max-width: 900px) {
  .mf-rail { width: 80px; }
  /* Task 1: hide Home/Add/Tx/Cards from rail on mobile  they're on the bottom bar */
  .mf-rail-desktop-only { display: none !important; }
  .mf-main {
    padding: 10px 4px;
    padding-bottom: calc(110px + env(safe-area-inset-bottom, 0px)); /* Expanded for floating pill */
  }
  .mf-bottombar { display: flex !important; }
  .mf-navbtn .q-btn__content span { display: none; }
  .mf-navbtn { min-height: 40px; }

/* Premium Layout Helpers extracted to root */
.mf-timeline-row {
  display: flex; align-items: center; justify-content: space-between;
  padding: 12px 16px; border-radius: 16px; background: rgba(255,255,255,0.03);
  margin-bottom: 8px; transition: background 0.15s;
}
.mf-timeline-row:active { background: rgba(255,255,255,0.08); transform: scale(0.98); }
html.mf-light .mf-timeline-row { background: rgba(0,0,0,0.02); }
html.mf-light .mf-timeline-row:active { background: rgba(0,0,0,0.06); }

  /* 8.2.2: Mobile layout  prevent ALL clipping & overflow */
  .mf-main { overflow-x: hidden !important; }
  .mf-canvas {
    padding: 0 !important; width: 100% !important;
    max-width: 100% !important; overflow-x: hidden !important;
    box-sizing: border-box !important; margin: 0 !important;
  }
  .mf-canvas > * { max-width: 100% !important; box-sizing: border-box !important; width: 100% !important; }
  .mf-canvas .my-card {
    border-radius: 14px !important; max-width: 100% !important;
    width: 100% !important; margin-left: 0 !important; margin-right: 0 !important;
    box-sizing: border-box !important;
  }
  .mf-canvas .q-card {
    width: 100% !important; max-width: 100% !important;
    margin-left: 0 !important; margin-right: 0 !important;
    box-sizing: border-box !important;
  }
  /* Tx page: allow wrapping  NOT cramped single-line */
  .q-table th { font-size: 11px !important; padding: 6px 6px !important; white-space: nowrap; }
  .q-table td {
    font-size: 13px !important; padding: 10px 6px !important;
    word-break: break-word !important; white-space: normal !important;
    line-height: 1.4 !important;
  }
  .q-table { font-size: 13px !important; width: 100% !important; }
  .q-table__container { overflow-x: auto !important; -webkit-overflow-scrolling: touch; }
  /* Full-row selection highlight on mobile */
  .q-table tbody tr { cursor: pointer; }
  .q-table tbody tr:active { background: rgba(var(--mf-accent-rgb, 91,140,255), 0.10) !important; }
  .q-table tbody tr.selected { background: rgba(var(--mf-accent-rgb, 91,140,255), 0.14) !important; }
  /* All form fields constrained */
  .q-field { max-width: 100% !important; box-sizing: border-box !important; }
  .q-select { max-width: 100% !important; }
  /* Fixed-width elements must shrink on mobile */
  .w-40 { width: 100% !important; max-width: 48% !important; }
  .w-64 { width: 100% !important; max-width: 100% !important; }
  /* Expansion panels full width */
  .q-expansion-item { max-width: 100% !important; overflow: hidden !important; }

  /* Admin tiles stack nicely on small screens */
  .mf-canvas .q-card .tile { min-width: 0; }
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
  /* 8.2.2: compact header with action row below title */
  .mf-header { height: auto !important; padding-top: 8px !important; padding-bottom: 6px !important; }
  .mf-header-actions { padding: 0 !important; }
  .mf-header-actions .q-btn { width: 42px !important; height: 42px !important; min-width: 42px !important; min-height: 42px !important; }
  .mf-header-actions .q-btn .q-icon { font-size: 22px !important; }
}
.q-menu { z-index: 99999 !important; }

/* Mobile full-bleed adjustments (8.2.2  edge-to-edge cards) */
@media (max-width: 600px){
  .mf-header, .mf-canvas { max-width: none !important; width: 100% !important; margin: 0 !important; }
  .mf-main { padding-left: 4px !important; padding-right: 4px !important; }
  .mf-canvas { padding-left: 0 !important; padding-right: 0 !important; }
  .mf-header { padding-left: 10px !important; padding-right: 10px !important; }
  /* Force ALL cards truly full-width on small screens */
  .mf-canvas .my-card,
  .mf-canvas .q-card,
  .mf-canvas > * {
    width: 100% !important; max-width: 100% !important;
    margin-left: 0 !important; margin-right: 0 !important;
    box-sizing: border-box !important;
  }
  .mf-canvas .grid { width: 100% !important; max-width: 100% !important; }
}

/* Stronger issuer tint + variants */
.my-card.mf-issuer-ct { border-color: rgba(251,191,36,0.35) !important; }
.my-card.mf-issuer-rbc { border-color: rgba(0,81,165,0.35) !important; }
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
  background: radial-gradient(circle at 35% 35%, rgba(0,81,165,0.25), transparent 60%),
              radial-gradient(circle at 70% 70%, rgba(251,191,36,0.12), transparent 65%);
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
  color: var(--mf-accent) !important;
  background: transparent !important;
}
.q-item:hover, .q-item.q-manual-focusable--focused {
  background: rgba(120,160,255,0.14) !important;
}

/* Dialog cards must follow theme surface (fix light theme dark dialog) */
/* Task 5: Dialog cards  fully solid, NO semi-transparent gradients */
.q-dialog .my-card, .q-dialog .q-card.my-card {
  background: var(--mf-bg) !important;
  border: none !important;
  color: var(--mf-text) !important;
}
html.mf-light .q-dialog .my-card,
html.mf-light .q-dialog .q-card.my-card {
  background: #ffffff !important;
}

/* Remove any numeric label rendered inside progress bars */
.q-linear-progress__label { display: none !important; }

/* ========================================
   Phase 7.0: Premium UI Overhaul
   ======================================== */

/* Premium form inputs */
.q-field--outlined .q-field__control,
.q-field--filled .q-field__control {
  border-radius: 12px !important;
  transition: border-color 0.2s ease, box-shadow 0.2s ease;
}
.q-field--outlined .q-field__control:focus-within,
.q-field--filled .q-field__control:focus-within {
  border-color: var(--mf-accent) !important;
  box-shadow: 0 0 0 3px rgba(91,140,255,0.12) !important;
}
html.mf-light .q-field--outlined .q-field__control:focus-within,
html.mf-light .q-field--filled .q-field__control:focus-within {
  box-shadow: 0 0 0 3px rgba(29,78,216,0.10) !important;
}

/* Premium badges */
.q-badge {
  border-radius: 8px !important;
  font-weight: 600 !important;
  letter-spacing: 0.02em !important;
  padding: 3px 10px !important;
}

/* Premium separator */
.q-separator {
  background: var(--mf-border) !important;
}

/* Premium scrollbar */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.12); border-radius: 99px; }
::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.22); }
html.mf-light ::-webkit-scrollbar-thumb { background: rgba(17,24,39,0.12); }
html.mf-light ::-webkit-scrollbar-thumb:hover { background: rgba(17,24,39,0.22); }

/* Premium login page */
.mf-login-hero {
  position: fixed; top: 0; left: 0; right: 0; bottom: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
  background: radial-gradient(ellipse 1200px 700px at 30% 20%, var(--mf-g1), transparent 50%),
              radial-gradient(ellipse 800px 500px at 70% 80%, var(--mf-g2), transparent 50%),
              var(--mf-bg);
  z-index: 9999;
  overflow: auto;
}
/* Login: show left brand panel + hide mobile logo on desktop (>768px) */
@media (min-width: 769px) {
  .mf-login-left { display: flex !important; }
  .mf-login-mobile-logo { display: none !important; }
}
/* Login: on small screens, remove outer shadow/rounding so it fills nicely */
@media (max-width: 768px) {
  .mf-login-hero { padding: 0; }
  .mf-login-hero > div { border-radius: 0 !important; box-shadow: none !important; min-height: 100dvh; }
}

/* Premium card accent strip */
.mf-accent-strip {
  height: 3px;
  border-radius: 0 0 2px 2px;
  background: linear-gradient(90deg, var(--mf-accent), var(--mf-accent2));
  opacity: 0.7;
}

/* Premium stat value */
.mf-stat-value {
  font-size: 2rem;
  font-weight: 800;
  letter-spacing: -0.03em;
  line-height: 1.1;
  font-feature-settings: 'tnum';
}
.mf-stat-label {
  font-size: 0.7rem;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--mf-muted);
}

/* Premium header */
.mf-header {
  backdrop-filter: blur(20px) !important;
  -webkit-backdrop-filter: blur(20px) !important;
}

/* Premium chip / tag */
.mf-tag {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 4px 10px;
  border-radius: 8px;
  font-size: 0.75rem;
  font-weight: 600;
  background: rgba(255,255,255,0.06);
  border: 1px solid var(--mf-border);
  color: var(--mf-text);
}
html.mf-light .mf-tag { background: rgba(17,24,39,0.04); }

/* Premium card utilization bar */
.mf-util-bar {
  height: 6px;
  border-radius: 999px;
  background: rgba(255,255,255,0.06);
  overflow: hidden;
  position: relative;
}
.mf-util-bar > div {
  height: 100%;
  border-radius: 999px;
  transition: width 0.8s cubic-bezier(0.22, 1, 0.36, 1);
}
.mf-util-green { background: linear-gradient(90deg, #22c55e, #4ade80); }
.mf-util-yellow { background: linear-gradient(90deg, #eab308, #fbbf24); }
.mf-util-red { background: linear-gradient(90deg, #ef4444, #f87171); }

/* Premium section headers */
.mf-section-title {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--mf-muted);
  padding-bottom: 8px;
  border-bottom: 1px solid var(--mf-border);
  margin-bottom: 12px;
}

/* Premium icon containers */
.mf-icon-box {
  width: 40px;
  height: 40px;
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
}

/* Premium nav buttons */
.mf-navbtn {
  border-radius: 12px !important;
  transition: background 0.2s ease, transform 0.15s ease !important;
}
.mf-navbtn:hover {
  background: rgba(255,255,255,0.08) !important;
}
.mf-navbtn.is-active {
  background: rgba(var(--mf-accent-rgb, 91,140,255), 0.14) !important;
}

/* Smooth page transitions — optimized for speed */
.mf-canvas {
  animation: mf-fadein 0.12s ease-out;
  will-change: opacity;
}
@keyframes mf-fadein {
  from { opacity: 0; }
  to { opacity: 1; }
}

/* Premium empty states */
.mf-empty-state {
  text-align: center;
  padding: 40px 20px;
  color: var(--mf-muted);
}
.mf-empty-state .q-icon { font-size: 48px; opacity: 0.3; margin-bottom: 12px; }

"""
ui.add_head_html("<style>" + BANK_CSS + """
/* Budget progress: hide numeric overlay label */
.mf-budget .q-linear-progress__label{display:none !important;}
/* Light theme: force dropdown menus to render light */
html.mf-light .q-menu, html.mf-light .q-menu.q-dark{background: var(--mf-menu-bg) !important; color: var(--mf-text) !important;}
html.mf-light .q-menu .q-list{background: var(--mf-menu-bg) !important; color: var(--mf-text) !important;}
html.mf-light .q-menu .q-item__label{color: var(--mf-text) !important;}
html.mf-light .q-item:hover{background: rgba(120,160,255,0.14) !important;}

/*  Add Dialog: Premium form styling (E2)  */
.mf-add-dialog input,
.mf-add-dialog textarea,
.mf-add-dialog .q-field__native,
.mf-add-dialog .q-field__input {
  font-size: 16px !important;  /* prevent iOS zoom */
}
.mf-add-dialog .q-dialog__inner > div { max-width: min(680px, 95vw); }
.mf-add-dialog .q-card { box-sizing: border-box; overflow-x: hidden; }
/* 8.2.2: Force dialog sections and form fields to stretch full width */
.mf-add-dialog .q-card > * { width: 100% !important; box-sizing: border-box !important; }
.mf-add-dialog .column { align-items: stretch !important; width: 100% !important; }
.mf-add-dialog .row { width: 100% !important; }
.mf-add-dialog .q-field { width: 100% !important; box-sizing: border-box !important; }
.mf-add-dialog .q-select { width: 100% !important; box-sizing: border-box !important; }
/* Force every NiceGUI wrapper div inside dialog to be full-width */
.mf-add-dialog div[class*="nicegui"] { width: 100% !important; }
.mf-add-dialog > div > div { width: 100% !important; }
/* 8.7: Custom chip-select  replaces Quasar q-select entirely */
.mf-chip-row {
  display: flex; flex-wrap: wrap; gap: 8px;
}
.mf-chip-row.mf-chip-scroll {
  max-height: 140px; overflow-y: auto; padding-right: 4px;
}
.mf-chip {
  border-radius: 20px; padding: 7px 16px; font-size: 13px;
  font-weight: 400; cursor: pointer; white-space: nowrap;
  user-select: none; transition: all 0.12s ease;
  border: 1.5px solid var(--mf-border);
  background: var(--mf-surface);
  color: var(--mf-text);
}
.mf-chip:hover { border-color: var(--mf-accent); }
.mf-chip.active {
  border-color: var(--mf-accent) !important;
  background: color-mix(in srgb, var(--mf-accent) 14%, transparent);
  color: var(--mf-accent); font-weight: 600;
}
.mf-chip.disabled {
  opacity: 0.45; cursor: not-allowed;
}
.mf-chip.disabled:hover { border-color: var(--mf-border); }
/* scrollbar for chip grids */
.mf-chip-scroll::-webkit-scrollbar { width: 4px; }
.mf-chip-scroll::-webkit-scrollbar-thumb { background: var(--mf-border); border-radius: 4px; }
.mf-hide-scrollbar::-webkit-scrollbar { display: none; }
.mf-hide-scrollbar { -ms-overflow-style: none; scrollbar-width: none; }
/* 9.9: Custom dropdown for high-cardinality fields (Category) — full-width, large touch targets */
.mf-dd-trigger {
  display: flex; align-items: center; gap: 8px;
  padding: 14px 18px; border-radius: 14px;
  border: 1.5px solid var(--mf-border);
  background: var(--mf-surface); color: var(--mf-text);
  font-size: 16px; font-weight: 600; cursor: pointer;
  transition: border-color 0.15s;
  user-select: none; width: 100%; box-sizing: border-box;
}
.mf-dd-trigger:hover { border-color: var(--mf-accent); }
.mf-dd-trigger.open { border-color: var(--mf-accent); }
.mf-dd-trigger.open .mf-dd-arrow { transform: rotate(180deg); }
.mf-dd-trigger.disabled { opacity: 0.45; cursor: not-allowed; pointer-events: none; }
.mf-dd-panel {
  max-height: 260px; overflow-y: auto;
  border: 1px solid var(--mf-border); border-radius: 14px;
  background: var(--mf-surface); margin-top: 4px;
  padding: 6px; width: 100%; box-sizing: border-box;
}
.mf-dd-panel::-webkit-scrollbar { width: 4px; }
.mf-dd-panel::-webkit-scrollbar-thumb { background: var(--mf-border); border-radius: 4px; }
.mf-dd-item {
  padding: 13px 18px; border-radius: 10px; font-size: 15px; font-weight: 500;
  color: var(--mf-text); cursor: pointer; transition: background 0.1s;
  width: 100%; box-sizing: border-box;
}
.mf-dd-item:hover { background: color-mix(in srgb, var(--mf-accent) 10%, transparent); }
.mf-dd-item.active {
  background: color-mix(in srgb, var(--mf-accent) 14%, transparent);
  color: var(--mf-accent); font-weight: 700;
}

/* Upload bar theming */
.mf-add-dialog .q-uploader__header,
.q-uploader__header {
  background: var(--mf-surface-2, var(--mf-bg-2)) !important;
  color: var(--mf-text) !important;
  border-bottom: 1px solid var(--mf-border);
}
.mf-add-dialog .q-uploader,
.q-uploader {
  background: var(--mf-surface, var(--mf-bg)) !important;
  border: 1px solid var(--mf-border) !important;
  border-radius: 14px !important;
  overflow: hidden;
}
.mf-add-dialog .q-uploader__header .q-btn { color: var(--mf-accent) !important; }

/* Form fields  rounder, cleaner, themed */
.mf-add-dialog .q-field--outlined .q-field__control {
  border-radius: 12px !important;
  transition: border-color 0.15s ease;
}
.mf-add-dialog .q-field--outlined .q-field__control::before {
  border-color: var(--mf-border) !important;
}
.mf-add-dialog .q-field--outlined.q-field--focused .q-field__control::before {
  border-color: var(--mf-accent) !important;
  border-width: 2px !important;
}
.mf-add-dialog .q-field__label { color: var(--mf-muted) !important; }
.mf-add-dialog .q-checkbox__label { color: var(--mf-muted) !important; font-size: 13px; }

/* Select dropdown: match app theme */
.mf-add-dialog .q-field--outlined .q-field__append .q-icon {
  color: var(--mf-muted) !important;
}
/* Section headers in dialog */
.mf-add-dialog .mf-dlg-section {
  font-size: 11px; font-weight: 700; text-transform: uppercase;
  letter-spacing: 0.08em; color: var(--mf-muted);
}
/* Save button glow */
.mf-add-dialog .mf-save-btn:hover {
  box-shadow: 0 6px 20px rgba(0,0,0,0.20) !important;
  transform: translateY(-1px);
}

/* Split slider polish */
.mf-split-card .q-slider__track-container { height: 6px; }
.mf-split-card .q-slider__thumb { transform: scale(1.05); }
.mf-split-pill { border-radius: 999px; padding: 6px 10px; border: 1px solid var(--mf-border); background: rgba(255,255,255,0.06); }
html.mf-light .mf-split-pill { background: rgba(0,0,0,0.03); }

/*  About Page Responsive  */
.mf-about-wrap { max-width: 640px; margin: 0 auto; }
.mf-about-features {
  display: grid; grid-template-columns: 1fr; gap: 14px; width: 100%; margin-top: 8px;
}
.mf-about-author {
  display: flex; gap: 24px; align-items: flex-start; flex-direction: column;
}
@media (min-width: 768px) {
  .mf-about-wrap { max-width: 900px; }
  .mf-about-features { grid-template-columns: repeat(2, 1fr); }
  .mf-about-author { flex-direction: row; }
}
@media (min-width: 1024px) {
  .mf-about-features { grid-template-columns: repeat(3, 1fr); }
}

/*  Home Page Dashboard Responsive Grid  */
.mf-dash-grid {
  display: grid; grid-template-columns: 1fr; gap: 18px; width: 100%;
}
.mf-dash-grid > * { min-width: 0; }
@media (min-width: 901px) {
  .mf-dash-grid { grid-template-columns: 1fr; gap: 20px; }
  .mf-dash-grid > .mf-dash-full,
  .mf-dash-grid > :has(> .mf-dash-full) { grid-column: 1 / -1; }
}
/* 9.8.1: Desktop home sections stretch full-width to match hero */
@media (min-width: 901px) {
  .mf-home-section { width: 100% !important; box-sizing: border-box; }
}
/* 9.8.4: Desktop side-by-side for budgets + spending — equal 50/50 */
.mf-home-2col { display: flex; flex-direction: column; gap: 16px; width: 100%; }
@media (min-width: 901px) {
  .mf-home-2col { flex-direction: row; gap: 20px; }
  .mf-home-2col > * { flex: 1 1 0%; min-width: 0; max-width: 50%; }
}
/* 9.8.4: Force equal-height cards inside 2col */
.mf-home-2col .my-card { height: 100%; }
/* 8.2.2: Ensure canvas children stretch full width  always */
.mf-canvas > * { width: 100% !important; min-width: 0; box-sizing: border-box !important; margin-left: 0 !important; margin-right: 0 !important; }
.mf-canvas .my-card { width: 100% !important; box-sizing: border-box !important; margin-left: 0 !important; margin-right: 0 !important; }
.mf-canvas .q-card { width: 100% !important; box-sizing: border-box !important; }
/* ROOT FIX: NiceGUI's ui.column() adds Quasar .column { align-items: flex-start } 
   this prevents children from stretching. Force stretch inside cards so content fills width. */
.my-card .column,
.my-card .q-column,
.my-card > .column,
.mf-canvas .column {
  align-items: stretch !important;
}
.my-card .column > *,
.mf-canvas .my-card .column > * {
  width: 100% !important;
  min-width: 0 !important;
  box-sizing: border-box !important;
}
/* 9.11.1: Merchant grid — override width:100% on grid items so they fill columns properly */
.mf-merchant-grid {
  display: grid !important;
  grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)) !important;
  gap: 14px !important;
  width: 100% !important;
}
.mf-merchant-grid > * {
  width: auto !important;
  min-width: 0 !important;
}
/* KPI tiles: lighter, cleaner look (E5) */
.kpi { border-radius: 16px !important; }
html.mf-light .kpi {
  background: rgba(255,255,255,0.65) !important;
  border-color: rgba(0,0,0,0.06) !important;
  box-shadow: 0 2px 8px rgba(0,0,0,0.04) !important;
}
/* Section titles: clean spacing */
.mf-section-title {
  font-size: 13px; font-weight: 700; text-transform: uppercase;
  letter-spacing: 0.06em; color: var(--mf-muted); margin-bottom: 10px;
}
/* Admin grid responsive */
@media (max-width: 600px) {
  .mf-admin-grid { grid-template-columns: repeat(2, 1fr) !important; }
}

</style>""", shared=True)

ui.add_head_html('<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap">', shared=True)

ui.add_head_html(r'''
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="default">
<meta name="apple-mobile-web-app-title" content="FinTrackr">
<meta name="theme-color" content="#0F1923">
<meta name="viewport" content="width=device-width, initial-scale=1, minimum-scale=1, viewport-fit=cover">
<link rel="manifest" href="/manifest.json">
<link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png">
<link rel="apple-touch-icon-precomposed" sizes="180x180" href="/apple-touch-icon.png">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="preload" href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" as="style">
<!-- iOS startup images: dark background to eliminate white flash during load -->
<link rel="apple-touch-startup-image" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='1170' height='2532' viewBox='0 0 1170 2532'%3E%3Crect fill='%230F1923' width='1170' height='2532'/%3E%3Ctext x='585' y='1220' text-anchor='middle' fill='%23FBBF24' font-size='80' font-family='system-ui' font-weight='800'%3EFinTrackr%3C/text%3E%3Ccircle cx='585' cy='1100' r='45' fill='%2322C55E' opacity='0.7'/%3E%3C/svg%3E" media="(device-width: 390px) and (device-height: 844px) and (-webkit-device-pixel-ratio: 3)">
<link rel="apple-touch-startup-image" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='1284' height='2778' viewBox='0 0 1284 2778'%3E%3Crect fill='%230F1923' width='1284' height='2778'/%3E%3Ctext x='642' y='1340' text-anchor='middle' fill='%23FBBF24' font-size='80' font-family='system-ui' font-weight='800'%3EFinTrackr%3C/text%3E%3Ccircle cx='642' cy='1210' r='45' fill='%2322C55E' opacity='0.7'/%3E%3C/svg%3E" media="(device-width: 428px) and (device-height: 926px) and (-webkit-device-pixel-ratio: 3)">
<link rel="apple-touch-startup-image" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='1179' height='2556' viewBox='0 0 1179 2556'%3E%3Crect fill='%230F1923' width='1179' height='2556'/%3E%3Ctext x='590' y='1230' text-anchor='middle' fill='%23FBBF24' font-size='80' font-family='system-ui' font-weight='800'%3EFinTrackr%3C/text%3E%3Ccircle cx='590' cy='1110' r='45' fill='%2322C55E' opacity='0.7'/%3E%3C/svg%3E" media="(device-width: 393px) and (device-height: 852px) and (-webkit-device-pixel-ratio: 3)">
<style>
/* CRITICAL: Paint dark background IMMEDIATELY  prevents iOS white flash + black bar */
html,body{
  background:#0F1923 !important;
  min-height: 100vh;
  min-height: -webkit-fill-available;
  /* Task 7: extend background into safe areas to prevent black bars */
  padding-bottom: env(safe-area-inset-bottom, 0px);
  overscroll-behavior-y: none;
}
html.mf-light, html.mf-light body {
  background: #f8f9fa !important;
}
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
/* 8.5: Hide NiceGUI reconnect flash when switching between apps */
.q-loading-bar { display: none !important; }
.nicegui-reconnecting { display: none !important; }
div[class*="nicegui"][class*="reconnect"] { display: none !important; }
.q-dialog--seamless { display: none !important; }

/* iOS launch splash overlay  covers blank page while NiceGUI hydrates */
#mf-splash{
  position:fixed; inset:0; z-index:999999;
  background: #0F1923;
  display:flex; flex-direction:column; align-items:center; justify-content:center; gap:16px;
  transition: opacity 0.4s ease;
}
#mf-splash.mf-splash-hide{ opacity:0; pointer-events:none; }
#mf-splash .mf-sp-icon{
  width:64px; height:64px; border-radius:16px;
  background: linear-gradient(135deg, #0F1923, #22C55E);
  display:flex; align-items:center; justify-content:center;
  box-shadow: 0 6px 24px rgba(0,0,0,0.4);
  animation: mf-sp-pulse 1.6s ease-in-out infinite;
}
@keyframes mf-sp-pulse{
  0%,100%{ transform: scale(1); }
  50%{ transform: scale(1.05); opacity:0.85; }
}
#mf-splash .mf-sp-title{
  font-family: Inter, system-ui, sans-serif; font-weight:800; font-size:26px;
  color: rgba(255,255,255,0.92); letter-spacing:-0.04em; margin-top:4px;
}
#mf-splash .mf-sp-dots{ display:flex; gap:5px; margin-top:10px; }
#mf-splash .mf-sp-dot{
  width:5px; height:5px; border-radius:50%; background:#22C55E; opacity:0.3;
  animation: mf-sp-blink 1.2s ease-in-out infinite;
}
#mf-splash .mf-sp-dot:nth-child(2){ animation-delay:0.2s; }
#mf-splash .mf-sp-dot:nth-child(3){ animation-delay:0.4s; }
@keyframes mf-sp-blink{ 0%,100%{ opacity:0.25; } 50%{ opacity:1; } }
</style>
<script>
// Inject splash overlay ASAP  matches the app header icon (insights icon in emerald+gold badge)
(function(){
  if(document.getElementById('mf-splash')) return;
  var s=document.createElement('div'); s.id='mf-splash';
  // The icon SVG replicates the Material "insights" icon in gold (#FBBF24)
  s.innerHTML='<div class="mf-sp-icon"><svg xmlns="http://www.w3.org/2000/svg" width="34" height="34" viewBox="0 0 24 24" fill="#FBBF24"><path d="M21 8c-1.45 0-2.26 1.44-1.93 2.51l-3.55 3.56c-.3-.09-.74-.09-1.04 0l-2.55-2.55C12.27 10.45 11.46 9 10 9c-1.45 0-2.27 1.44-1.93 2.52l-4.56 4.55C2.44 15.74 1 16.55 1 18c0 1.1.9 2 2 2 1.45 0 2.26-1.44 1.93-2.51l4.55-4.56c.3.09.74.09 1.04 0l2.55 2.55C12.73 16.55 13.54 18 15 18c1.45 0 2.27-1.44 1.93-2.52l3.56-3.55C21.56 12.26 23 11.45 23 10c0-1.1-.9-2-2-2z"/><path d="M15 9l.94-2.07L18 6l-2.06-.93L15 3l-.92 2.07L12 6l2.08.93z"/><path d="M3.5 11L4 9l2-.5L4 8l-.5-2L3 8l-2 .5L3 9z"/></svg></div><div class="mf-sp-title">FinTrackr</div><div class="mf-sp-dots"><div class="mf-sp-dot"></div><div class="mf-sp-dot"></div><div class="mf-sp-dot"></div></div>';
  document.body.prepend(s);
  // Auto-hide once NiceGUI content appears (or 4s timeout)
  var hide=function(){ var el=document.getElementById('mf-splash'); if(el){el.classList.add('mf-splash-hide'); setTimeout(function(){try{el.remove();}catch(e){}},400);} };
  var ob=new MutationObserver(function(){
    if(document.querySelector('.mf-shell,.mf-login-hero,.nicegui-content')){ob.disconnect(); setTimeout(hide,80);}
  });
  ob.observe(document.body,{childList:true,subtree:true});
  setTimeout(hide,4000);
  // Register service worker for caching fonts + static assets
  if('serviceWorker' in navigator){ navigator.serviceWorker.register('/sw.js',{scope:'/'}).catch(function(){}); }
})();
</script>
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
          .replace(/\\+/g, '-').replace(/\\//g, '_').replace(/=+$/g, '');
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
        toast('Passkey registered ');
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
      const LIGHT_THEMES = new Set(["Frost", "Sand Gold", "Slate Light", "Mint Light", "Rose Light", "Arctic Light"]);
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

      // 8.7: Dropdowns replaced with custom chip-select  no q-select JS hacks needed.
    }catch(e){}
  };

  // Apply saved theme ASAP
  try{
    const saved = localStorage.getItem("mf_theme");
    if(saved){ window.mfSetTheme(saved); }
    else {
      // Default to system preference: Dark/Night -> Graphite Rose, Light/Day -> Mint Light
      try{
        const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
        // Also check time of day: 6 AM6 PM = day, otherwise night
        const hour = new Date().getHours();
        const isNight = (hour < 6 || hour >= 18);
        const useDark = prefersDark || isNight;
        window.mfSetTheme(useDark ? "Graphite Rose" : "Mint Light");
      }catch(e){
        window.mfSetTheme("Graphite Rose");
      }
    }
    try{ setTimeout(()=>{ window.mfFixPlotlyText && window.mfFixPlotlyText(); }, 120);}catch(e){}
    // finish booting
    window.__mfBooting = false;
    // If user never picked a theme manually, follow system preference changes
    try{
      if(!(localStorage.getItem("mf_theme_user")==="1") && window.matchMedia){
        const mq = window.matchMedia('(prefers-color-scheme: dark)');
        const handler = (e)=>{ try{ window.mfSetTheme(e.matches ? "Graphite Rose" : "Mint Light"); }catch(_e){} };
        if(mq && mq.addEventListener){ mq.addEventListener('change', handler); }
        else if(mq && mq.addListener){ mq.addListener(handler); }
      }
    }catch(e){}

  }catch(e){
    try{ window.mfSetTheme("Graphite Rose"); }catch(_){}
  }

  // 8.2.2: Instant touch feedback on bottom bar (touchstart fires before click)
  document.addEventListener('touchstart', function(e) {
    var tab = e.target.closest('.mf-tab, .mf-tab-add');
    if (tab) {
      tab.style.transform = 'scale(0.88)';
      var icon = tab.querySelector('.q-icon');
      if (icon) {
        icon.style.color = 'var(--mf-accent)';
        icon.style.background = 'rgba(91,140,255,0.18)';
        icon.style.borderRadius = '12px';
        icon.style.padding = '6px 14px';
      }
    }
  }, {passive: true});
  document.addEventListener('touchend', function(e) {
    var tab = e.target.closest('.mf-tab, .mf-tab-add');
    if (tab) {
      tab.style.transform = '';
      var icon = tab.querySelector('.q-icon');
      if (icon && !tab.classList.contains('is-active')) {
        icon.style.color = '';
        icon.style.background = '';
        icon.style.borderRadius = '';
        icon.style.padding = '';
      }
    }
  }, {passive: true});
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
        return str(app.storage.user.get("theme") or "Graphite Rose")
    except Exception:
        return "Graphite Rose"

def is_light_theme_name(name: str) -> bool:
    n = (name or "").lower()
    return ("light" in n) or (n in ("arctic light", "slate light", "sand gold", "frost", "pearl mint", "mint light", "rose light"))

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
                except Exception as e:
                    _logger.debug("Could not set search prefill: %s", e)
                d.close()
                nav_to("/tx")
            ui.button("Search", icon="search", on_click=_go).props("unelevated")
    d.open()

def topbar():
    with ui.row().classes("w-full items-center justify-between px-3 py-2"):
        with ui.row().classes("items-center gap-3"):
            with ui.element('div').style(
                'width: 36px; height: 36px; border-radius: 10px; display: flex; align-items: center; justify-content: center;'
                'background: linear-gradient(135deg, #0F1923, #22C55E);'
                'box-shadow: 0 2px 8px rgba(34,197,94,0.25);'
            ):
                ui.icon('insights').style('font-size: 20px; color: #FBBF24;')
            with ui.column().classes("gap-0"):
                ui.label(APP_TITLE).classes("text-lg font-bold")
                ui.label(APP_SUBTITLE).classes("text-xs").style("color: var(--mf-muted)")
        with ui.row().classes("items-center gap-2"):
            ui.button("Refresh", on_click=lambda: refresh_all()).props("outline icon=refresh").classes("text-sm")
            ui.button("Logout", on_click=logout).props("outline icon=logout").classes("text-sm")

def nav_button(label: str, icon: str, path: str):
    ui.button(label, on_click=lambda: nav_to(path)).props(f"flat icon={icon}").classes("w-full")

def shell(content_fn, *, active_path: str = ""):
    """Phase 8.0 shell: persistent desktop rail + mobile bottom bar + header.
    """
    # 8.7: Tell Quasar whether we're in dark or light mode at page-render time.
    # Without this, NiceGUI defaults to light mode and Quasar renders dark text
    # on dark backgrounds  making select/dropdown text invisible.
    _LIGHT_THEMES = {'Frost', 'Sand Gold', 'Slate Light', 'Mint Light', 'Rose Light', 'Arctic Light'}
    _user_theme = str(app.storage.user.get('theme', '') or '').strip()
    _is_light = (_user_theme in _LIGHT_THEMES) or ('light' in _user_theme.lower())
    ui.dark_mode(not _is_light)

    # Active path detection (best-effort)
    try:
        if not active_path:
            active_path = ui.context.client.page.path  # type: ignore[attr-defined]
    except Exception:
        pass

    def nav_btn(label: str, icon: str, href: str) -> None:
        cls = "mf-navbtn" + (" is-active" if href == active_path else "")
        def go(_evt=None) -> None:
            try:
                nav_to(href)
            except Exception:
                pass
            ui.run_javascript("document.documentElement.classList.remove('mf-nav-open')")
        ui.button(label, icon=icon).props("flat").classes(cls).on("click", go)

    with ui.element("div").classes("mf-shell"):
        # Backdrop overlay (tap to close on mobile)
        ui.element("div").classes("mf-backdrop").on("click", lambda: ui.run_javascript("document.documentElement.classList.remove(\'mf-nav-open\')"))

        # Nav rail  on mobile only shows items NOT in bottom bar (Rules, Admin, About)
        # On desktop shows all items as a persistent sidebar
        with ui.element("div").classes("mf-rail"):
            with ui.element("div").classes("mf-rail-card"):
                with ui.row().classes('items-center gap-2 mb-1'):
                    with ui.element('div').style(
                        'width: 26px; height: 26px; border-radius: 7px; display: flex; align-items: center; justify-content: center;'
                        'background: linear-gradient(135deg, #0F1923, #22C55E); flex-shrink: 0;'
                    ):
                        ui.icon('insights').style('font-size: 14px; color: #FBBF24;')
                    ui.label("FinTrackr").classes("mf-brand")
                ui.separator().props("dark").classes("opacity-20 my-1")

                # Desktop-only nav buttons (hidden on mobile via CSS class)
                with ui.element("div").classes("mf-rail-desktop-only"):
                    nav_btn("Home", "dashboard", "/")
                    nav_btn("Add", "add_circle", "/add")
                    nav_btn("Merchants", "storefront", "/merchants")
                    nav_btn("Cards", "credit_card", "/cards")
                # Always visible (these are what "More" reveals on mobile)
                nav_btn("Ledger", "receipt_long", "/tx")
                nav_btn("Rules", "rule", "/rules")
                nav_btn("Admin", "settings", "/admin")
                nav_btn("About", "info", "/about")

                ui.element('div').style('flex: 1;')  # push version to bottom
                ui.label(f"v{APP_VERSION}").classes("text-xs").style("color: var(--mf-muted); text-align:center; opacity: 0.5;")
                # 8.8: Desktop logout button (visible only on desktop via mf-rail-desktop-only parent)
                with ui.element("div").classes("mf-rail-desktop-only").style("margin-top: 8px;"):
                    ui.button("Logout", icon="logout", on_click=do_logout).props("flat dense").style(
                        "color: #ef4444; font-size: 12px; font-weight: 600; width: 100%; border-radius: 10px; text-transform: none;"
                    )

        # Bottom tab bar (mobile only  CSS hides on 901px)
        # 8.2.2: ALL 5 icons inside one <nav> for consistent look.
        # Navigation tabs use <a href> = instant (zero server hop).
        # More button rendered as <button> with id; click handler attached via JS after render
        # (Vue v-html strips onclick, so we use addEventListener instead).
        _bottom_tabs_html = []
        for _bl, _bi, _bh in [
            ("Cards", "account_balance_wallet", "/cards"),
            ("Merchants", "storefront", "/merchants"),
            ("Add", "add", "/add"),
            ("Home", "space_dashboard", "/"),
            ("More", "apps", None),
        ]:
            if _bl == "Add":
                _bottom_tabs_html.append(
                    f'<a href="/add" class="mf-tab-add" style="text-decoration:none;">'
                    f'<i class="q-icon notranslate material-icons" aria-hidden="true" role="img">add_circle</i></a>'
                )
            elif _bh is None:
                # More button  id used to attach JS listener after render
                _bottom_tabs_html.append(
                    f'<button class="mf-tab mf-more-btn" id="mf-more-toggle-btn" type="button">'
                    f'<i class="q-icon notranslate material-icons" aria-hidden="true" role="img">menu</i></button>'
                )
            else:
                _act = " is-active" if _bh == active_path else ""
                _bottom_tabs_html.append(
                    f'<a href="{_bh}" class="mf-tab{_act}" style="text-decoration:none;">'
                    f'<i class="q-icon notranslate material-icons" aria-hidden="true" role="img">{_bi}</i></a>'
                )
        ui.html(f'<nav class="mf-bottombar">{"".join(_bottom_tabs_html)}</nav>')
        # Attach More button and instant tab visual feedback JS
        ui.run_javascript("""
        (function(){
            var btn = document.getElementById('mf-more-toggle-btn');
            if(btn) btn.addEventListener('click', function(e){
                e.preventDefault();
                document.documentElement.classList.toggle('mf-more-open');
            });
            var tabs = document.querySelectorAll('.mf-bottombar a');
            tabs.forEach(function(t) {
                t.addEventListener('click', function(e) {
                    // Instant visual feedback before new page loads
                    tabs.forEach(function(x) { x.classList.remove('is-active'); });
                    if (this.classList.contains('mf-tab')) {
                        this.classList.add('is-active');
                    }
                    this.style.transform = 'scale(0.9)';
                    setTimeout(() => { this.style.transform = ''; }, 150);
                });
            });
        })();
        """)

        # 8.2.2: Compact "More" popup  <a> tags for Rules/Admin/About (instant nav via href)
        _more_items = []
        for _ml, _mi, _mh in [("Ledger", "receipt_long", "/tx"), ("Rules", "rule", "/rules"), ("Admin", "settings", "/admin"), ("About", "info", "/about")]:
            _act = " is-active" if _mh == active_path else ""
            _more_items.append(
                f'<a href="{_mh}" class="mf-more-item{_act}">'
                f'<i class="q-icon notranslate material-icons" style="font-size:22px; margin-right:10px;">{_mi}</i>{_ml}</a>'
            )
        _more_items.append('<div style="height:1px; background:var(--mf-border); margin:4px 6px;"></div>')
        _more_html = '<div class="mf-more-popup">' + ''.join(_more_items) + '</div>'
        _more_html += '<div class="mf-more-backdrop" id="mf-more-backdrop-el"></div>'
        ui.html(_more_html)
        # Backdrop: attach click listener via JS (Vue strips onclick from v-html)
        ui.run_javascript("""
        (function(){
            var bd = document.getElementById('mf-more-backdrop-el');
            if(bd) bd.addEventListener('click', function(){ document.documentElement.classList.remove('mf-more-open'); });
            // Inject Logout button into popup
            var popup = document.querySelector('.mf-more-popup');
            if(!popup) return;
            var btn = document.createElement('button');
            btn.className = 'mf-more-item';
            btn.id = 'mf-logout-btn';
            btn.style.cssText = 'color:#ef4444 !important; display:flex; align-items:center; width:100%; border:none; background:none; cursor:pointer; font-size:15px; font-weight:600; padding:8px 16px; min-height:52px; border-radius:12px; touch-action:manipulation;';
            btn.innerHTML = '<i class="q-icon notranslate material-icons" style="font-size:22px; margin-right:10px;">logout</i>Logout';
            popup.appendChild(btn);
            // Wire logout button click to hidden NiceGUI trigger
            document.addEventListener('click', function(e){
                if(e.target && (e.target.id === 'mf-logout-btn' || e.target.closest('#mf-logout-btn'))){
                    document.documentElement.classList.remove('mf-more-open');
                    var t = document.getElementById('mf-logout-trigger');
                    if(t) t.click();
                }
            });
        })();
        """)
        # Hidden NiceGUI button  Python handler for session logout
        ui.button("").props("flat").style("display:none;").props('id=mf-logout-trigger').on("click", do_logout)

        # Main content area
        with ui.element("main").classes("mf-main"):
            with ui.element("div").classes("mf-header"):
                # 8.2.2: Clean two-row header  title on top, actions below
                with ui.column().classes("w-full gap-1"):
                    # Row 1: FinTrackr logo + title
                    with ui.row().classes("items-center justify-between w-full"):
                        with ui.row().classes("items-center gap-3"):
                            with ui.element("div").classes("mf-title"):
                                with ui.row().classes('items-center gap-2'):
                                    with ui.element('div').style(
                                        'width: 30px; height: 30px; border-radius: 8px; display: flex; align-items: center; justify-content: center;'
                                        'background: linear-gradient(135deg, #0F1923, #22C55E);'
                                    ):
                                        ui.icon('insights').style('font-size: 17px; color: #FBBF24;')
                                    ui.link("FinTrackr", "/").classes("t1 text-xl md:text-2xl font-extrabold").style("color: inherit; text-decoration: none; letter-spacing: -0.03em;")

                        # Desktop theme select (hidden on mobile)
                        def _open_theme_dialog():
                            with ui.dialog() as td, ui.card().classes("my-card p-0 w-full max-w-sm").style("overflow: hidden; border-radius: 24px;"):
                                ui.element('div').classes('mf-accent-strip')
                                with ui.column().classes("p-5 gap-3"):
                                    with ui.row().classes("items-center gap-2"):
                                        ui.icon("palette").style("color: var(--mf-accent); font-size: 20px;")
                                        ui.label("Theme").classes("text-base font-bold")

                                    _theme_swatches = {
                                        'Midnight Blue': ('#5B8CFF', '#46E6A6'), 'Emerald Gold': ('#22C55E', '#FBBF24'),
                                        'Graphite Rose': ('#F472B6', '#A78BFA'), 'Arctic Light': ('#1D4ED8', '#0EA5E9'),
                                        'Mint Light': ('#059669', '#10B981'), 'Rose Light': ('#DB2777', '#F43F5E'),
                                        'Slate Light': ('#334155', '#2563EB'), 'Sand Gold': ('#B45309', '#D97706'),
                                    }

                                    cur = app.storage.user.get('theme')
                                    if not cur:
                                        try:
                                            h = now().hour
                                        except Exception:
                                            h = datetime.datetime.now().hour
                                        cur = 'Midnight Blue' if (h >= 19 or h < 7) else 'Arctic Light'
                                        app.storage.user['theme'] = cur
                                    else:
                                        cur = str(cur)

                                    ui.label('Dark').classes('mf-stat-label mt-1')
                                    with ui.column().classes("w-full gap-1"):
                                        for tname in ['Midnight Blue', 'Emerald Gold', 'Graphite Rose']:
                                            is_cur = (tname == cur)
                                            c1, c2 = _theme_swatches.get(tname, ('#5B8CFF', '#46E6A6'))
                                            with ui.button(
                                                on_click=lambda tn=tname: (
                                                    app.storage.user.__setitem__('theme', tn),
                                                    ui.run_javascript(f"window.mfSetTheme({tn!r})"),
                                                    td.close(),
                                                ),
                                            ).classes("w-full justify-start").props("unelevated" if is_cur else "flat").style(
                                                f"border-radius: 12px; padding: 8px 12px;"
                                                f"{'border: 2px solid var(--mf-accent);' if is_cur else 'border: 1px solid var(--mf-border);'}"
                                            ):
                                                with ui.row().classes("items-center gap-3 w-full"):
                                                    with ui.element("div").style(f"width: 28px; height: 28px; border-radius: 8px; background: linear-gradient(135deg, {c1}, {c2}); flex-shrink: 0;"):
                                                        pass
                                                    ui.label(tname).classes("text-sm font-medium")
                                                    if is_cur:
                                                        ui.icon("check_circle").style("color: var(--mf-accent); margin-left: auto; font-size: 18px;")

                                    ui.label('Light').classes('mf-stat-label mt-2')
                                    with ui.column().classes("w-full gap-1"):
                                        for tname in ['Arctic Light', 'Mint Light', 'Rose Light', 'Slate Light', 'Sand Gold']:
                                            is_cur = (tname == cur)
                                            c1, c2 = _theme_swatches.get(tname, ('#1D4ED8', '#0EA5E9'))
                                            with ui.button(
                                                on_click=lambda tn=tname: (
                                                    app.storage.user.__setitem__('theme', tn),
                                                    ui.run_javascript(f"window.mfSetTheme({tn!r})"),
                                                    td.close(),
                                                ),
                                            ).classes("w-full justify-start").props("unelevated" if is_cur else "flat").style(
                                                f"border-radius: 12px; padding: 8px 12px;"
                                                f"{'border: 2px solid var(--mf-accent);' if is_cur else 'border: 1px solid var(--mf-border);'}"
                                            ):
                                                with ui.row().classes("items-center gap-3 w-full"):
                                                    with ui.element("div").style(f"width: 28px; height: 28px; border-radius: 8px; background: linear-gradient(135deg, {c1}, {c2}); flex-shrink: 0;"):
                                                        pass
                                                    ui.label(tname).classes("text-sm font-medium")
                                                    if is_cur:
                                                        ui.icon("check_circle").style("color: var(--mf-accent); margin-left: auto; font-size: 18px;")

                                    with ui.row().classes("justify-end w-full mt-2"):
                                        ui.button("Close").props("flat").style("border-radius: 10px;").on("click", td.close)
                            td.open()

                        _theme_names = ['Midnight Blue', 'Emerald Gold', 'Graphite Rose', 'Arctic Light', 'Mint Light', 'Rose Light', 'Slate Light', 'Sand Gold']
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

                    # Row 2: Action buttons  theme (mobile), search, refresh  bigger for accessibility
                    with ui.row().classes("items-center gap-3 mf-header-actions"):
                        ui.button("", icon="palette").props("flat round").classes("mf-show-mobile").style(
                            "border: 1px solid var(--mf-border); background: var(--mf-surface); border-radius: 12px;"
                        ).on("click", _open_theme_dialog)
                        ui.run_javascript('window.mfSetTheme(localStorage.getItem(\"mf_theme\") || \"Midnight Blue\")')
                        ui.button("", icon="search").props("flat round").style(
                            "border: 1px solid var(--mf-border); background: var(--mf-surface); border-radius: 12px;"
                        ).on("click", lambda: open_search_dialog())
                        ui.button("", icon="refresh").props("flat round").style(
                            "border: 1px solid var(--mf-border); background: var(--mf-surface); border-radius: 12px;"
                        ).on("click", lambda: ui.navigate.to(ui.context.client.page.path))
                        # 8.2.2: Logout moved to More popup  desktop keeps it in rail

            with ui.element("div").classes("mf-canvas"):
                content_fn()



# -----------------------------
# Shared actions
# -----------------------------
def refresh_all():
    """Force-clear all caches so next read fetches fresh data from Google Sheets."""
    invalidate("transactions", "cards", "recurring", "rules", "budgets", "locks")
    ui.notify("Refreshed  data reloaded from Google Sheets", type="positive")


def owners_list() -> List[str]:
    # Phase 2+: treat everything as family-wide (no per-person owner split)
    return ["Family"]


def accounts_list() -> List[str]:
    """Return the canonical set of accounts.
    8.4: Cleaned up to only valid accounts  no redundant entries."""
    # Canonical accounts  the ONLY valid accounts in the system
    VALID_ACCOUNTS = [
        "Bank",
        "CT Mastercard - Black",
        "CT Mastercard - Grey",
        "RBC VISA",
        "RBC Mastercard",
        "RBC Line of Credit",
    ]
    return VALID_ACCOUNTS


def categories_list() -> List[str]:
    tx = cached_df("transactions")
    cats = set()
    if not tx.empty:
        cats |= set(tx["category"].astype(str).tolist())
    cats = {c.strip() for c in cats if c and c.strip()}
    base = ["Uncategorized", "Groceries", "Rent", "Utilities", "Subscriptions", "Dining", "Fuel", "Shopping", "Household", "Travel", "Health", "Salary", "Transfer"]
    return sorted(set(base) | cats)


def methods_list() -> List[str]:
    cards = cached_df("cards")
    methods = set(["Debit", "Card", "Bank"])
    if not cards.empty and "method_name" in cards.columns:
        methods |= set(cards["method_name"].astype(str).tolist())
    return sorted({m.strip() for m in methods if m and m.strip()})


# -----------------------------
# Pages
# -----------------------------
@ui.page("/login")
def login_page():
    ui.dark_mode(True)  # login page always uses dark theme
    # Premium login - responsive: side-by-side on desktop, stacked on mobile
    with ui.element('div').classes('mf-login-hero'):
        with ui.element('div').style(
            'display: flex; align-items: stretch; width: 100%; max-width: 960px;'
            'border-radius: 28px; overflow: hidden;'
            'box-shadow: 0 20px 60px rgba(0,0,0,0.18);'
        ):
            # Left panel - branding (hidden on mobile, visible on desktop)
            with ui.element('div').style(
                'flex: 1; display: none; flex-direction: column; align-items: center; justify-content: center;'
                'background: linear-gradient(135deg, #0F1923 0%, #1A2332 40%, #0D3320 100%);'
                'padding: 48px 40px; gap: 24px; min-height: 520px;'
            ).classes('mf-login-left'):
                with ui.element('div').style(
                    'width: 80px; height: 80px; border-radius: 22px; display: flex; align-items: center; justify-content: center;'
                    'background: rgba(255,255,255,0.18); backdrop-filter: blur(8px);'
                    'box-shadow: 0 8px 32px rgba(0,0,0,0.12);'
                ):
                    ui.icon('insights').style('font-size: 42px; color: #FBBF24;')
                ui.label('FinTrackr').style('font-size: 34px; font-weight: 800; color: #FBBF24; letter-spacing: -0.04em;')
                ui.label('Your premium personal finance dashboard').style(
                    'color: rgba(255,255,255,0.85); font-size: 15px; text-align: center; max-width: 260px; line-height: 1.6;'
                )
                # Feature highlights
                for feat_icon, feat_text in [
                    ('insights', 'Smart spending insights'),
                    ('document_scanner', 'AI receipt scanning'),
                    ('palette', '8 premium themes'),
                    ('security', 'Passkey authentication'),
                ]:
                    with ui.row().style(
                        'align-items: center; gap: 10px; background: rgba(255,255,255,0.12);'
                        'border-radius: 10px; padding: 8px 16px; width: 100%; max-width: 240px;'
                    ):
                        ui.icon(feat_icon).style('font-size: 18px; color: rgba(255,255,255,0.9);')
                        ui.label(feat_text).style('font-size: 13px; color: rgba(255,255,255,0.9); font-weight: 500;')

            # Right panel - sign-in form
            with ui.element('div').style(
                'flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center;'
                'padding: 48px 40px; background: var(--mf-bg);'
                'min-width: 0;'
            ):
                # Mobile-only logo (hidden on desktop where left panel shows it)
                with ui.column().classes('items-center gap-1 mb-6 mf-login-mobile-logo'):
                    with ui.element('div').style(
                        'width: 60px; height: 60px; border-radius: 18px; display: flex; align-items: center; justify-content: center;'
                        'background: linear-gradient(135deg, #0F1923, #22C55E);'
                        'box-shadow: 0 8px 24px rgba(34,197,94,0.30);'
                    ):
                        ui.icon('insights').style('font-size: 30px; color: #FBBF24;')
                    ui.label('FinTrackr').classes('text-xl font-extrabold mt-2').style('letter-spacing: -0.03em; background: linear-gradient(135deg, #4F46E5, #06B6D4); -webkit-background-clip: text; -webkit-text-fill-color: transparent;')

                with ui.column().classes('w-full gap-0').style('max-width: 380px;'):
                    ui.label('Welcome back').classes('text-2xl font-extrabold').style('letter-spacing: -0.02em;')
                    ui.label('Sign in to manage your finances').classes('text-sm mb-6').style('color: var(--mf-muted)')

                    u_in = ui.input("Username").classes("w-full").props("outlined dense")
                    u_in.style("margin-bottom: 12px;")
                    p_in = ui.input("Password", password=True, password_toggle_button=True).classes("w-full").props("outlined dense")

                    def attempt():
                        if check_login(u_in.value or "", p_in.value or ""):
                            app.storage.user["logged_in"] = True
                            ui.notify("Welcome", type="positive")
                            nav_to("/")
                        else:
                            ui.notify("Invalid login", type="negative")

                    ui.button("Sign in", on_click=attempt).classes("w-full mt-5").props("unelevated").style(
                        "background: linear-gradient(135deg, var(--mf-accent), var(--mf-accent2)) !important;"
                        "color: #fff !important; font-weight: 700; border-radius: 12px; padding: 14px 0;"
                        "box-shadow: 0 4px 14px rgba(91,140,255,0.25); font-size: 15px;"
                    )

                    with ui.row().classes('w-full justify-center mt-5 gap-2'):
                        ui.icon('lock').style('font-size: 14px; color: var(--mf-muted);')
                        ui.label('256-bit encrypted').classes('text-xs').style('color: var(--mf-muted);')

                    with ui.row().classes('w-full justify-center mt-4'):
                        ui.label(f'v{APP_VERSION}').classes('text-xs').style('color: var(--mf-muted); opacity: 0.5;')


@ui.page("/")
def dashboard_page():
    if not require_login():
        nav_to("/login")
        return

    def content():
        # Safe: run recurring generation for today once per page load
        # FIX: Defer heavy synchronous work so the page renders instantly.
        def _deferred_recurring():
            try:
                created = generate_recurring_for_date(today())
                if created:
                    ui.notify(f"Auto-added {created} recurring entries for {today().isoformat()}", type="positive")
            except Exception as e:
                _logger.error("Failed to generate recurring transactions: %s", e)
        ui.timer(0.1, _deferred_recurring, once=True)

        tx = cached_df("transactions")
        #  B2 fix: robust empty-data handling (no KeyError on cleaned sheets) 
        if tx is None or tx.empty or len(tx) == 0:
            with ui.card().classes("my-card p-6"):
                with ui.column().classes("items-center gap-3 w-full").style("padding: 30px 0;"):
                    ui.icon("account_balance_wallet").style("font-size: 48px; color: var(--mf-muted); opacity: 0.3;")
                    ui.label("No transactions yet").classes("text-lg font-bold")
                    ui.label("Tap the + button or go to Add to create your first entry.").style("color: var(--mf-muted); text-align: center;")
            return

        # --- normalize expected columns (robust to sheet header variations) ---
        def _first_col(df, candidates):
            for c in candidates:
                if c in df.columns:
                    return c
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
        for _col, _def in [("date", ""), ("amount", 0), ("type", ""), ("account", ""), ("category", ""), ("notes", ""), ("owner", "Family")]:
            if _col not in tx.columns:
                tx[_col] = _def

        tx["date_parsed"] = tx["date"].apply(parse_date)
        tx = tx[tx["date_parsed"].notna()].copy()
        if tx.empty:
            with ui.card().classes("my-card p-6"):
                with ui.column().classes("items-center gap-3 w-full").style("padding: 30px 0;"):
                    ui.icon("event_busy").style("font-size: 48px; color: var(--mf-muted); opacity: 0.3;")
                    ui.label("No valid transactions found").classes("text-lg font-bold")
                    ui.label("Check that your transactions sheet has a 'date' column with valid dates.").style("color: var(--mf-muted); text-align: center;")
            return
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
        intl = amt[typ.isin(['international', 'international transfer', 'intl'])].sum()

        loc_draw = amt[typ.isin(['loc draw', 'loc withdrawal', 'loc_draw', 'loc_withdrawal'])].sum()
        loc_repay = amt[typ.isin(['loc repay', 'loc repayment', 'loc_repay', 'loc_repayment'])].sum()
        transfer_out = amt[typ.isin(['transfer', 'transfer out', 'transfer_out'])].sum()

        net = income + loc_draw - expense - invest - intl - loc_repay - transfer_out


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
            # Determine whose payday
            _pay_owner = ''
            try:
                _abhi_dates = set()
                _y, _m = next_pay.year, next_pay.month
                for _offset in range(-1, 2):
                    _ym = _m + _offset
                    _yy = _y
                    if _ym < 1: _ym = 12; _yy -= 1
                    elif _ym > 12: _ym = 1; _yy += 1
                    _abhi_dates.update(abhi_pay_dates_for_month(_yy, _ym))
                _indhu_dates = set(wife_pay_dates_between(next_pay - dt.timedelta(days=3), next_pay + dt.timedelta(days=3)))
                _is_abhi = next_pay in _abhi_dates
                _is_indhu = next_pay in _indhu_dates
                if _is_abhi and _is_indhu: _pay_owner = 'Both'
                elif _is_abhi: _pay_owner = 'Abhi'
                elif _is_indhu: _pay_owner = 'Indhu'
            except Exception:
                pass

            pp_start = prev_pay
            pp_end = next_pay

            ptx = tx[(tx['date_parsed'] >= pp_start) & (tx['date_parsed'] < pp_end)].copy()
            ptyp = ptx['type_l']
            pamt = ptx['amount_num']
            # broaden type matching
            income_pp = pamt[ptyp.isin(['credit','income'])].sum()
            expense_pp = pamt[ptyp.isin(['debit','expense'])].sum()
            invest_pp = pamt[ptyp.isin(['investment'])].sum()
            intl_pp = pamt[ptyp.isin(['international', 'international transfer', 'intl'])].sum()
            
            loc_draw_pp = pamt[ptyp.isin(['loc draw', 'loc withdrawal', 'loc_draw', 'loc_withdrawal'])].sum()
            loc_repay_pp = pamt[ptyp.isin(['loc repay', 'loc repayment', 'loc_repay', 'loc_repayment'])].sum()
            transfer_out_pp = pamt[ptyp.isin(['transfer', 'transfer out', 'transfer_out'])].sum()

            net_pp = income_pp + loc_draw_pp - expense_pp - invest_pp - intl_pp - loc_repay_pp - transfer_out_pp
        except Exception:
            pp_start = today() - dt.timedelta(days=14)
            pp_end = today() + dt.timedelta(days=14)
            income_pp = expense_pp = invest_pp = intl_pp = net_pp = 0.0

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

        # Time-based greeting
        _hour = datetime.datetime.now().hour
        if _hour < 12: _greeting = 'Good morning'
        elif _hour < 17: _greeting = 'Good afternoon'
        else: _greeting = 'Good evening'

        # Daily average spending this month
        _daily_avg = round(expense / max(today().day, 1), 2) if expense > 0 else 0.0

        # Cashflow Rings Hero
        _ring_income = min(1.0, income / (income + expense + 1)) if income > 0 else 0
        _ring_expense = min(1.0, expense / (income + expense + 1)) if expense > 0 else 0
        _income_dash = 2 * 3.14159 * 76 * _ring_income
        _expense_dash = 2 * 3.14159 * 60 * _ring_expense
        
        with ui.element('div').classes('w-full flex-col items-center justify-center p-8 mt-2 relative').style(
            'background: linear-gradient(145deg, rgba(15,23,42,0.9), rgba(30,41,59,0.95));'
            'border-radius: 40px;'
            'border: 1px solid rgba(255,255,255,0.08);'
            'box-shadow: 0 32px 64px -12px rgba(0,0,0,0.5), inset 0 2px 4px rgba(255,255,255,0.1);'
            'backdrop-filter: blur(24px);'
            'overflow: hidden;'
        ):
            # Dynamic background mesh glow
            ui.html(f'''
                <div style="position: absolute; top: -50px; left: -50px; width: 200px; height: 200px; background: radial-gradient(circle, rgba(14,165,233,0.15) 0%, transparent 70%); border-radius: 50%; pointer-events: none;"></div>
                <div style="position: absolute; bottom: -50px; right: -50px; width: 200px; height: 200px; background: radial-gradient(circle, rgba(16,185,129,0.1) 0%, transparent 70%); border-radius: 50%; pointer-events: none;"></div>
            ''')
            
            # The Massive Rings SVG
            ui.html(f'''
                <div style="position: relative; width: 190px; height: 190px; margin: 0 auto; filter: drop-shadow(0 8px 16px rgba(0,0,0,0.4));">
                    <svg viewBox="0 0 170 170" style="transform: rotate(-90deg); width: 100%; height: 100%;">
                        <!-- Income Ring Track -->
                        <circle cx="85" cy="85" r="76" fill="none" stroke="rgba(255,255,255,0.05)" stroke-width="12" stroke-linecap="round"/>
                        <!-- Income Ring Fill -->
                        <circle cx="85" cy="85" r="76" fill="none" stroke="url(#incGrad)" stroke-width="12"
                            stroke-dasharray="{_income_dash:.1f} 477.5" stroke-linecap="round"
                            style="transition: stroke-dasharray 1.5s cubic-bezier(0.22,1,0.36,1);"/>
                            
                        <!-- Expense Ring Track -->
                        <circle cx="85" cy="85" r="60" fill="none" stroke="rgba(255,255,255,0.05)" stroke-width="12" stroke-linecap="round"/>
                        <!-- Expense Ring Fill -->
                        <circle cx="85" cy="85" r="60" fill="none" stroke="url(#expGrad)" stroke-width="12"
                            stroke-dasharray="{_expense_dash:.1f} 377" stroke-linecap="round"
                            style="transition: stroke-dasharray 1.5s cubic-bezier(0.22,1,0.36,1) 0.2s;"/>
                            
                        <defs>
                            <linearGradient id="incGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                                <stop offset="0%" stop-color="#34D399"/>
                                <stop offset="100%" stop-color="#059669"/>
                            </linearGradient>
                            <linearGradient id="expGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                                <stop offset="0%" stop-color="#F472B6"/>
                                <stop offset="100%" stop-color="#E11D48"/>
                            </linearGradient>
                        </defs>
                    </svg>
                    <div style="position: absolute; inset: 0; display: flex; flex-direction: column; align-items: center; justify-content: center; pointer-events: none;">
                        <span style="font-size: 13px; font-weight: 700; color: rgba(255,255,255,0.6); text-transform: uppercase; letter-spacing: 0.15em; margin-bottom: 2px;">Net</span>
                        <span style="font-size: 30px; font-weight: 900; color: #fff; letter-spacing: -0.04em; font-feature-settings: 'tnum'; text-shadow: 0 4px 12px rgba(0,0,0,0.5);">{currency(net)}</span>
                    </div>
                </div>
            ''')
            # Badges below rings
            with ui.row().classes('w-full justify-center gap-4 mt-6 position-relative z-10'):
                with ui.element('div').classes('flex items-center gap-1.5').style('background: rgba(34, 197, 94, 0.15); padding: 6px 16px; border-radius: 24px; border: 1px solid rgba(34, 197, 94, 0.3); backdrop-filter: blur(8px); box-shadow: 0 4px 12px rgba(34,197,94,0.1);'):
                    ui.icon('arrow_upward').style('font-size: 16px; color: #34D399;')
                    ui.label(f"{currency(income)}").classes('text-sm font-extrabold').style('color: #34D399; font-feature-settings: "tnum";')
                with ui.element('div').classes('flex items-center gap-1.5').style('background: rgba(225, 29, 72, 0.15); padding: 6px 16px; border-radius: 24px; border: 1px solid rgba(225, 29, 72, 0.3); backdrop-filter: blur(8px); box-shadow: 0 4px 12px rgba(225,29,72,0.1);'):
                    ui.icon('arrow_downward').style('font-size: 16px; color: #FB7185;')
                    ui.label(f"{currency(expense)}").classes('text-sm font-extrabold').style('color: #FB7185; font-feature-settings: "tnum";')

        # Hero tiles (responsive)
        _tile_w = 'min(220px, 30vw)'
        _tile_h = '120px'
        _tile_base = f'min-width: 140px; width: {_tile_w}; height: {_tile_h}; border-radius: 24px; padding: 18px; display: flex; flex-direction: column; justify-content: space-between; flex-shrink: 0; scroll-snap-align: start;'
        with ui.element('div').style(
            'display: flex; gap: 14px; overflow-x: auto; scroll-snap-type: x mandatory;'
            '-webkit-overflow-scrolling: touch; scrollbar-width: none; width: 100%; margin-top: 24px; padding: 0 2px 8px 2px;'
        ).classes('mf-hide-scrollbar'):

            # Tile 1: Floating Add Action (Gradient Burst)
            def _goto_add(mode):
                app.storage.user['add_auto_open'] = mode
                nav_to('/add')
            with ui.element('div').style(f'{_tile_base} background: linear-gradient(135deg, #3B82F6, #8B5CF6); box-shadow: 0 12px 24px rgba(99,102,241,0.3); border: 1px solid rgba(255,255,255,0.2); cursor: pointer;').on('click', lambda: _goto_add('expense')):
                with ui.element('div').style('width: 36px; height: 36px; border-radius: 14px; background: rgba(255,255,255,0.2); display: flex; align-items: center; justify-content: center; backdrop-filter: blur(8px);'):
                    ui.icon('add').style('font-size: 22px; color: #fff; font-weight: 900;')
                with ui.column().classes('gap-0'):
                    ui.label('Add').classes('text-base font-extrabold text-white').style('letter-spacing: -0.02em;')
                    ui.label('Expense').classes('text-sm text-white opacity-80 font-medium')

            # Tile 2: Next Payday (Glass Tile)
            with ui.element('div').style(f'{_tile_base} background: var(--mf-card-top); border: 1px solid var(--mf-border); cursor: pointer; box-shadow: 0 4px 12px rgba(0,0,0,0.05);'):
                with ui.row().classes('justify-between items-start w-full'):
                    with ui.element('div').style('width: 32px; height: 32px; border-radius: 12px; background: rgba(16,185,129,0.15); display: flex; align-items: center; justify-content: center;'):
                        ui.icon('event').style('font-size: 18px; color: #10B981;')
                    if days_to_next is not None:
                        ui.label(f"{days_to_next}d").classes('text-sm font-bold').style('color: #10B981; margin-top: 4px;')
                with ui.column().classes('gap-0'):
                    _po_label = f" ({_pay_owner})" if _pay_owner else ""
                    ui.label(f'Next Payday{_po_label}').classes('text-xs font-semibold').style('color: var(--mf-muted); white-space: nowrap;')
                    if next_pay:
                        ui.label(next_pay.strftime('%b %d')).classes('text-lg font-extrabold').style('color: var(--mf-text); letter-spacing: -0.02em;')
                    else:
                        ui.label('Unknown').classes('text-lg font-extrabold').style('color: var(--mf-text);')

            # Tile 3: Daily Avg (Glass Tile)
            with ui.element('div').style(f'{_tile_base} background: var(--mf-card-top); border: 1px solid var(--mf-border); box-shadow: 0 4px 12px rgba(0,0,0,0.05);'):
                with ui.row().classes('justify-between items-start w-full'):
                    with ui.element('div').style('width: 32px; height: 32px; border-radius: 12px; background: rgba(244,63,94,0.15); display: flex; align-items: center; justify-content: center;'):
                        ui.icon('local_fire_department').style('font-size: 18px; color: #F43F5E;')
                with ui.column().classes('gap-0'):
                    ui.label('Daily Pacing').classes('text-xs font-semibold').style('color: var(--mf-muted);')
                    ui.label(currency(_daily_avg)).classes('text-lg font-extrabold').style('color: var(--mf-text); letter-spacing: -0.02em; font-feature-settings: "tnum";')


        # 9.8: Spending Breakdown data (biggest expense + daily spend sparkline)
        _si_data = None
        try:
            if not tx.empty and 'amount_num' in tx.columns:
                _ins_spend = tx[tx['type_l'].isin(['debit', 'expense'])].copy()
                if not _ins_spend.empty and 'category' in _ins_spend.columns:
                    _si_total = float(_ins_spend['amount_num'].sum())
                    # Biggest expense excluding LOC/intl categories
                    _excl_cats = {'loc utilization', 'repayment', 'cc repay', 'international', 'international transfer'}
                    _real_spend = _ins_spend[~_ins_spend['category'].str.strip().str.lower().isin(_excl_cats)]
                    _si_biggest = 0.0
                    _si_big_note = ''
                    _si_big_cat = ''
                    _si_big_date = ''
                    if not _real_spend.empty:
                        _max_row = _real_spend.loc[_real_spend['amount_num'].idxmax()]
                        _si_biggest = float(_max_row['amount_num'])
                        _si_big_note = str(_max_row.get('notes', '') or '')[:30]
                        _si_big_cat = str(_max_row.get('category', ''))[:20]
                        try:
                            _si_big_date = _max_row['date_parsed'].strftime('%b %d') if hasattr(_max_row.get('date_parsed', None), 'strftime') else ''
                        except Exception:
                            _si_big_date = ''

                    # Daily spend for last 7 days (sparkline data)
                    _daily_amounts = []
                    _daily_labels = []
                    try:
                        import datetime as _dt_mod
                        _today = _dt_mod.date.today()
                        _real_with_date = _real_spend[_real_spend['date_parsed'].notna()].copy()
                        for _di in range(6, -1, -1):
                            _day = _today - _dt_mod.timedelta(days=_di)
                            _day_total = 0.0
                            try:
                                _day_mask = _real_with_date['date_parsed'].dt.date == _day
                                _day_total = float(_real_with_date.loc[_day_mask, 'amount_num'].sum())
                            except Exception:
                                pass
                            _daily_amounts.append(_day_total)
                            _daily_labels.append(_day.strftime('%a'))
                    except Exception:
                        _daily_amounts = []
                        _daily_labels = []

                    _si_data = {
                        'total': _si_total, 'biggest': _si_biggest,
                        'big_note': _si_big_note, 'big_cat': _si_big_cat, 'big_date': _si_big_date,
                        'daily_amounts': _daily_amounts, 'daily_labels': _daily_labels,
                    }
        except Exception:
            pass

        def _render_spending_insights():
            if not _si_data:
                return
            # 9.8.2: User-configurable accent color for spending breakdown (stored in admin Color Matrix)
            _sb_color = app.storage.user.get('spending_breakdown_color', '#3B82F6')
            # Compute a secondary color by shifting hue slightly for gradient
            def _hex_to_rgb(h):
                h = h.lstrip('#')
                return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
            def _rgb_to_hex(r, g, b):
                return f'#{int(r):02x}{int(g):02x}{int(b):02x}'
            _sb_r, _sb_g, _sb_b = _hex_to_rgb(_sb_color)
            _sb_color2 = _rgb_to_hex(max(0, _sb_r - 40), max(0, _sb_g - 20), min(255, _sb_b + 30))

            with ui.card().classes('my-card p-0').style('overflow: hidden; width: 100%;'):
                ui.element('div').style(f'height: 3px; background: linear-gradient(90deg, {_sb_color}, {_sb_color2}); border-radius: 0;')
                with ui.column().classes('p-5 gap-4'):
                    with ui.row().classes('items-center gap-2'):
                        with ui.element('div').style(f'width: 32px; height: 32px; border-radius: 10px; background: {_sb_color}1A; display: flex; align-items: center; justify-content: center;'):
                            ui.icon('insights').style(f'font-size: 18px; color: {_sb_color};')
                        ui.label('Spending Breakdown').classes('text-base font-extrabold').style('letter-spacing: -0.02em;')

                    # 7-day spend sparkline (SVG)
                    _amounts = _si_data.get('daily_amounts', [])
                    _labels = _si_data.get('daily_labels', [])
                    if _amounts and len(_amounts) >= 2:
                        _max_a = max(_amounts) if max(_amounts) > 0 else 1
                        _spark_w, _spark_h = 280, 60
                        _pad_x, _pad_y = 8, 6
                        _usable_w = _spark_w - 2 * _pad_x
                        _usable_h = _spark_h - 2 * _pad_y
                        _pts = []
                        for _si_i, _a in enumerate(_amounts):
                            _sx = _pad_x + (_si_i / (len(_amounts) - 1)) * _usable_w
                            _sy = _pad_y + (1 - (_a / _max_a)) * _usable_h
                            _pts.append(f'{_sx:.1f},{_sy:.1f}')
                        _polyline = ' '.join(_pts)
                        # Fill area under the line
                        _fill_pts = _polyline + f' {_pad_x + _usable_w:.1f},{_pad_y + _usable_h:.1f} {_pad_x:.1f},{_pad_y + _usable_h:.1f}'
                        _spark_svg = f'''<svg viewBox="0 0 {_spark_w} {_spark_h}" style="width: 100%; height: 60px;">
                          <defs><linearGradient id="spkFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="{_sb_color}" stop-opacity="0.18"/><stop offset="100%" stop-color="{_sb_color}" stop-opacity="0.02"/></linearGradient></defs>
                          <polygon points="{_fill_pts}" fill="url(#spkFill)"/>
                          <polyline points="{_polyline}" fill="none" stroke="{_sb_color}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>'''
                        # Dot on last point
                        _last_x = _pad_x + _usable_w
                        _last_y = _pad_y + (1 - (_amounts[-1] / _max_a)) * _usable_h
                        _spark_svg += f'<circle cx="{_last_x:.1f}" cy="{_last_y:.1f}" r="3.5" fill="{_sb_color}"/>'
                        _spark_svg += '</svg>'

                        with ui.element('div').style('display: flex; flex-direction: column; gap: 4px;'):
                            ui.label('Last 7 Days').classes('text-[10px] font-semibold').style('color: var(--mf-muted); text-transform: uppercase; letter-spacing: 0.06em;')
                            ui.html(_spark_svg)
                            # Day labels below
                            with ui.element('div').style(f'display: flex; justify-content: space-between; padding: 0 {_pad_x}px;'):
                                for _dl in _labels:
                                    ui.label(_dl).classes('text-[9px]').style('color: var(--mf-muted);')

                    # Biggest expense highlight
                    if _si_data.get('biggest', 0) > 0:
                        ui.element('div').style('height: 1px; background: linear-gradient(90deg, transparent, var(--mf-border), transparent);')
                        with ui.row().classes('items-center justify-between w-full'):
                            with ui.column().classes('gap-0'):
                                ui.label('Biggest Expense').classes('text-[10px] font-semibold').style('color: var(--mf-muted); text-transform: uppercase; letter-spacing: 0.06em;')
                                _big_desc = _si_data.get('big_note') or _si_data.get('big_cat', '')
                                if _big_desc:
                                    _big_sub = _big_desc
                                    if _si_data.get('big_date'):
                                        _big_sub += f'  ·  {_si_data["big_date"]}'
                                    ui.label(_big_sub).classes('text-xs').style('color: var(--mf-muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 200px;')
                            ui.label(currency(_si_data['biggest'])).classes('text-xl font-extrabold').style(f'color: {_sb_color}; font-feature-settings: "tnum"; letter-spacing: -0.02em;')

        #  Smart Alert Banners (slim, at top  v9.0)
        # Alert logic kept but rendered as slim banners instead of a full card
        _alert_banners_data: list[tuple[str, str, str, str]] = []
        try:
            if not spend.empty and 'category' in spend.columns:
                _uncat = spend[spend['category'].astype(str).str.strip().isin(['', 'Uncategorized', 'nan'])]
                if len(_uncat) > 0:
                    _alert_banners_data.append(('label_off', f'{len(_uncat)} uncategorized transaction{"s" if len(_uncat) != 1 else ""} this month', 'warning', '/tx'))
            if not spend.empty:
                _avg_txn = float(spend['amount_num'].mean())
                _large = spend[spend['amount_num'] > (_avg_txn * 3)]
                if len(_large) > 0:
                    _top = _large.sort_values('amount_num', ascending=False).iloc[0]
                    _alert_banners_data.append(('priority_high', f'Large transaction: {currency(float(_top["amount_num"]))} on {_top.get("date","")}', 'info', '/tx'))
            try:
                _today_d = today()
                _day_of_month = _today_d.day
                _days_in_month = calendar.monthrange(_today_d.year, _today_d.month)[1]
                _projected = float(expense) / max(_day_of_month, 1) * _days_in_month if expense > 0 else 0
                _last_mk = month_key(_today_d.replace(day=1) - dt.timedelta(days=1)) if _today_d.month > 1 else month_key(dt.date(_today_d.year - 1, 12, 1))
                _last_spend = tx[tx['type_l'].isin(['debit', 'expense']) & (tx['month'] == _last_mk)]
                _last_total = float(_last_spend['amount_num'].sum()) if not _last_spend.empty else 0
                if _last_total > 0 and _projected > _last_total * 1.2:
                    _pct_over = int(round((_projected / _last_total - 1) * 100))
                    _alert_banners_data.append(('speed', f'Spending pace {_pct_over}% above last month (projected {currency(_projected)})', 'warning', '/reports'))
            except Exception:
                pass
            if income == 0:
                _alert_banners_data.append(('info', 'No income recorded this month yet', 'info', '/add'))
            if not spend.empty and 'notes' in spend.columns:
                _dup_keys = spend.apply(lambda r: f"{r.get('date','')}|{r['amount_num']}|{str(r.get('notes','')).strip()}", axis=1)
                _dup_count = int(_dup_keys.duplicated(keep=False).sum())
                if _dup_count >= 4:
                    _alert_banners_data.append(('difference', f'{_dup_count // 2}+ possible duplicate transactions', 'warning', '/tx'))
            try:
                cards_df = cached_df('cards')
                if not cards_df.empty:
                    for _, _cd in cards_df.iterrows():
                        _limit = parse_money(_cd.get('max_limit'), default=0)
                        _method = str(_cd.get('method_name', '')).strip()
                        if _limit > 0 and _method and not spend.empty:
                            _card_spend = float(spend[spend.get('method', pd.Series(dtype=str)).astype(str).str.strip() == _method]['amount_num'].sum())
                            if _card_spend >= _limit * 0.85:
                                _pct_used = int(round(_card_spend / _limit * 100))
                                _alert_banners_data.append(('credit_card', f'{_cd.get("card_name", _method)}: {_pct_used}% of limit used', 'warning' if _pct_used < 100 else 'error', '/cards'))
            except Exception:
                pass
            if _alert_banners_data:
                for _ab_icon, _ab_msg, _ab_sev, _ab_path in _alert_banners_data[:3]:
                    _sev_colors = {'error': '#ef4444', 'warning': '#f59e0b', 'info': '#3b82f6'}
                    _bc = _sev_colors.get(_ab_sev, '#3b82f6')
                    with ui.element('div').style(
                        f'display: flex; align-items: center; gap: 10px; padding: 8px 16px; border-radius: 10px;'
                        f'background: {_bc}0D; border: 1px solid {_bc}22; margin-bottom: 8px; cursor: pointer;'
                    ).on('click', lambda p=_ab_path: nav_to(p)):
                        ui.icon(_ab_icon).style(f'font-size: 16px; color: {_bc};')
                        ui.label(_ab_msg).classes('text-xs font-medium').style(f'color: {_bc};')
                        ui.icon('chevron_right').style(f'font-size: 14px; color: {_bc}; margin-left: auto; opacity: 0.5;')
        except Exception:
            pass

        # Budgets data
        _budget_rows = []
        budgets = read_df_optional('budgets')
        if budgets is not None and not budgets.empty and (not spend.empty) and "category" in spend.columns:
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
                    spend_by_cat = spend.groupby('category', as_index=False)['amount_num'].sum()
                    rows = []
                    for _, r in spend_by_cat.iterrows():
                        cat = str(r['category'])
                        if cat in bmap and bmap[cat] > 0:
                            rows.append((cat, float(r['amount_num']), float(bmap[cat])))
                    present = set([x[0] for x in rows])
                    for cat, bud in bmap.items():
                        if cat not in present and bud > 0:
                            rows.append((cat, 0.0, float(bud)))
                    rows.sort(key=lambda x: (x[1]/x[2]) if x[2] else 0.0, reverse=True)
                    _budget_rows = list(rows)  # 9.8.3: capture for deferred rendering

                    # In-app budget alerts (background notifications)
                    try:
                        alerts80 = [(c, s, b) for c, s, b in rows if b and (s/b) >= 0.80 and (s/b) < 1.0]
                        alerts100 = [(c, s, b) for c, s, b in rows if b and (s/b) >= 1.0]
                        if alerts100:
                            ui.notify(f'Over budget: {alerts100[0][0]} ({currency(alerts100[0][1])} / {currency(alerts100[0][2])})', type='negative')
                        elif alerts80:
                            ui.notify(f'Budget warning (80%+): {alerts80[0][0]} ({currency(alerts80[0][1])} / {currency(alerts80[0][2])})', type='warning')
                    except Exception:
                        pass

        # 9.11: Budget widget — light card with hero-style concentric rings
        def _render_budget_widget():
            if not _budget_rows:
                return
            _user_ring_colors = app.storage.user.get('budget_ring_colors', None)
            _ring_colors = _user_ring_colors if (isinstance(_user_ring_colors, list) and len(_user_ring_colors) >= 3) else ['#8B5CF6', '#3B82F6', '#F59E0B', '#10B981', '#EC4899', '#EF4444']

            _total_spent = sum(s for _, s, _ in _budget_rows)
            _total_budget = sum(b for _, _, b in _budget_rows)
            _overall_pct = min(1.0, _total_spent / _total_budget) if _total_budget > 0 else 0

            _c0 = _ring_colors[0]
            _c1 = _ring_colors[1] if len(_ring_colors) > 1 else _c0

            # 9.11.1: Dark gray glassmorphism card (like hero), centered rings
            with ui.element('div').classes('w-full').style(
                'overflow:hidden;width:100%;'
                'background:linear-gradient(145deg, rgba(28,28,38,0.93), rgba(40,40,52,0.96));'
                'border:1px solid rgba(255,255,255,0.08);'
                'border-radius:28px;'
                'box-shadow:0 20px 50px -10px rgba(0,0,0,0.45), inset 0 1px 3px rgba(255,255,255,0.06);'
                'backdrop-filter:blur(20px);'
                'position:relative;'
                'padding:0;'
            ):
                ui.element('div').style(f'height:3px;background:linear-gradient(90deg,{_c0},{_c1});')
                # Subtle radial glow accents
                ui.html(f'''
                    <div style="position:absolute;top:-40px;left:-40px;width:160px;height:160px;background:radial-gradient(circle,{_c0}18 0%,transparent 70%);border-radius:50%;pointer-events:none;"></div>
                    <div style="position:absolute;bottom:-40px;right:-40px;width:160px;height:160px;background:radial-gradient(circle,{_c1}12 0%,transparent 70%);border-radius:50%;pointer-events:none;"></div>
                ''')
                with ui.element('div').style(
                    'display:flex;flex-direction:column;align-items:center;padding:24px 20px;gap:16px;position:relative;z-index:1;'
                ):
                    with ui.row().classes('items-center gap-2'):
                        with ui.element('div').style(f'width:32px;height:32px;border-radius:10px;background:{_c0}25;display:flex;align-items:center;justify-content:center;'):
                            ui.icon('account_balance_wallet').style(f'font-size:18px;color:{_c0};')
                        ui.label('Budgets').classes('text-base font-extrabold').style('letter-spacing:-0.02em;color:#fff;')

                    # Build concentric rings
                    _ring_size = 180
                    _cx, _cy = _ring_size / 2, _ring_size / 2
                    _stroke_w = 12
                    _gap = 3
                    _outer_r = (_ring_size / 2) - 8
                    _ring_data = []
                    for _ri, (cat, spent_amt, bud_amt) in enumerate(_budget_rows):
                        pct = min(1.0, spent_amt / bud_amt) if bud_amt else 0.0
                        _rc = '#ef4444' if pct >= 1.0 else ('#f59e0b' if pct >= 0.8 else _ring_colors[_ri % len(_ring_colors)])
                        _r = _outer_r - _ri * (_stroke_w + _gap)
                        if _r < 15:
                            break
                        _circ = 2 * 3.14159265 * _r
                        _dash = pct * _circ
                        _ring_data.append((cat, spent_amt, bud_amt, pct, _rc, _r, _circ, _dash))

                    # SVG ring with gradient strokes — centered
                    _svg_parts = []
                    _svg_parts.append(f'<div style="position:relative;width:190px;height:190px;margin:0 auto;filter:drop-shadow(0 8px 16px rgba(0,0,0,0.4));">')
                    _svg_parts.append(f'<svg viewBox="0 0 {_ring_size} {_ring_size}" style="transform:rotate(-90deg);width:100%;height:100%;"><defs>')
                    for _ri, (cat, spent_amt, bud_amt, pct, _rc, _r, _circ, _dash) in enumerate(_ring_data):
                        _svg_parts.append(f'<linearGradient id="budG{_ri}" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" stop-color="{_rc}"/><stop offset="100%" stop-color="{_rc}BB"/></linearGradient>')
                    _svg_parts.append('</defs>')
                    for _ri, (cat, spent_amt, bud_amt, pct, _rc, _r, _circ, _dash) in enumerate(_ring_data):
                        _svg_parts.append(f'<circle cx="{_cx}" cy="{_cy}" r="{_r}" fill="none" stroke="rgba(255,255,255,0.06)" stroke-width="{_stroke_w}" stroke-linecap="round"/>')
                        _svg_parts.append(f'<circle cx="{_cx}" cy="{_cy}" r="{_r}" fill="none" stroke="url(#budG{_ri})" stroke-width="{_stroke_w}" stroke-dasharray="{_dash:.1f} {_circ:.1f}" stroke-linecap="round" style="transition:stroke-dasharray 1.5s cubic-bezier(0.22,1,0.36,1) {_ri*0.15}s;"/>')
                    _svg_parts.append('</svg>')
                    # Center text overlay — white on dark
                    _pct_color = '#ef4444' if _overall_pct >= 1.0 else ('#f59e0b' if _overall_pct >= 0.8 else '#ffffff')
                    _svg_parts.append(f'<div style="position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;pointer-events:none;">')
                    _svg_parts.append(f'<span style="font-size:11px;font-weight:700;color:rgba(255,255,255,0.55);text-transform:uppercase;letter-spacing:0.15em;">Budget</span>')
                    _svg_parts.append(f'<span style="font-size:26px;font-weight:900;color:{_pct_color};letter-spacing:-0.04em;font-feature-settings:\'tnum\';text-shadow:0 2px 8px rgba(0,0,0,0.4);">{int(_overall_pct*100)}%</span>')
                    _svg_parts.append(f'<span style="font-size:10px;font-weight:600;color:rgba(255,255,255,0.5);">{currency(_total_spent)}</span>')
                    _svg_parts.append('</div></div>')
                    ui.html('\n'.join(_svg_parts))

                    # Category badges — centered, dark-bg aware
                    with ui.element('div').style('display:flex;flex-wrap:wrap;gap:8px;justify-content:center;margin-top:4px;'):
                        for (cat, spent_amt, bud_amt, pct, _rc, _r, _circ, _dash) in _ring_data:
                            with ui.element('div').style(
                                f'background:{_rc}18;padding:5px 12px;border-radius:20px;'
                                f'border:1px solid {_rc}35;'
                                f'display:flex;align-items:center;gap:6px;'
                                f'backdrop-filter:blur(6px);'
                            ):
                                ui.element('div').style(f'width:8px;height:8px;border-radius:50%;background:{_rc};flex-shrink:0;')
                                ui.label(f'{cat} {int(pct*100)}%').style(f'font-size:11px;font-weight:700;color:{_rc};white-space:nowrap;')

        def _render_recent_tx():
            try:
                if not tx.empty and "date_parsed" in tx.columns:
                    _recent_tx = tx.sort_values("date_parsed", ascending=False).head(5)
                    if not _recent_tx.empty:
                        with ui.column().classes("w-full mt-6 px-2"):
                            with ui.row().classes("items-center justify-between w-full mb-3"):
                                ui.label("Recent Activity").classes("text-lg font-extrabold").style("letter-spacing: -0.02em; color: var(--mf-text);")
                                ui.button("See all", on_click=lambda: nav_to("/tx")).props("flat dense").style("border-radius: 8px; font-size: 13px; font-weight: 600; text-transform: none; color: #3b82f6;")

                            with ui.element('div').style('width: 100%; border-radius: 20px; background: var(--mf-card-top); border: 1px solid var(--mf-border); box-shadow: 0 4px 12px rgba(0,0,0,0.05); overflow: hidden;'):
                                _last_date_group = None
                                _total_cnt = len(_recent_tx)
                                for _idx, (_, _rtx) in enumerate(_recent_tx.iterrows()):
                                    _rt_type = str(_rtx.get("type", "") or "").strip().lower()
                                    _rt_is_income = _rt_type in ("credit", "income")
                                    _rt_amt = float(_rtx.get("amount_num", 0) or 0)
                                    _rt_note = str(_rtx.get("notes", "") or "")[:35] or str(_rtx.get("category", "") or "")
                                    _rt_cat = str(_rtx.get("category", "") or "")
                                    
                                    # Apple Wallet style row (compacted for crispness)
                                    _rt_color = "#10B981" if _rt_is_income else "var(--mf-text)"
                                    _rt_sign = "+" if _rt_is_income else ""
                                    # Context-aware icons
                                    _cl = _rt_cat.lower()
                                    _nl = _rt_note.lower()
                                    if _rt_is_income:
                                        _rt_icon, _icon_color = ("arrow_downward", "#10B981") if "refund" in _nl else ("attach_money", "#10B981")
                                    elif "grocer" in _cl or "walmart" in _nl or "costco" in _nl:
                                        _rt_icon, _icon_color = "shopping_cart", "#F59E0B"
                                    elif "rent" in _cl or "mortgage" in _cl:
                                        _rt_icon, _icon_color = "home", "#3B82F6"
                                    elif "utilit" in _cl or "hydro" in _nl or "water" in _nl:
                                        _rt_icon, _icon_color = "bolt", "#EAB308"
                                    elif "subscript" in _cl or "netflix" in _nl or "spotify" in _nl:
                                        _rt_icon, _icon_color = "play_circle", "#EC4899"
                                    elif "din" in _cl or "restaurant" in _cl or "food" in _cl:
                                        _rt_icon, _icon_color = "restaurant", "#F43F5E"
                                    elif "fuel" in _cl or "gas" in _cl or "shell" in _nl or "petro" in _nl:
                                        _rt_icon, _icon_color = "local_gas_station", "#64748B"
                                    elif "shop" in _cl or "amazon" in _nl:
                                        _rt_icon, _icon_color = "local_mall", "#8B5CF6"
                                    elif "house" in _cl:
                                        _rt_icon, _icon_color = "roofing", "#06B6D4"
                                    elif "travel" in _cl or "flight" in _cl or "hotel" in _cl:
                                        _rt_icon, _icon_color = "flight", "#0EA5E9"
                                    elif "health" in _cl or "pharmacy" in _cl or "drug" in _nl:
                                        _rt_icon, _icon_color = "favorite", "#EF4444"
                                    elif "transfer" in _cl or "e-transfer" in _nl:
                                        _rt_icon, _icon_color = "swap_horiz", "#6366F1"
                                    elif "car" in _cl or "auto" in _cl:
                                        _rt_icon, _icon_color = "directions_car", "#F97316"
                                    else:
                                        _rt_icon, _icon_color = "storefront", "#9CA3AF"
                                    _icon_bg = f"{_icon_color}26"
                                    
                                    _bb = "border-bottom: 1px solid var(--mf-border);" if _idx < _total_cnt - 1 else ""
                                    with ui.element("div").style(f"display: flex; align-items: center; justify-content: space-between; padding: 12px 16px; {_bb}"):
                                        with ui.row().classes("items-center gap-3 flex-1").style("min-width: 0;"):
                                            with ui.element("div").style(
                                                f"width: 36px; height: 36px; border-radius: 10px; display: flex; align-items: center; justify-content: center;"
                                                f"background: {_icon_bg}; flex-shrink: 0;"
                                            ):
                                                ui.icon(_rt_icon).style(f"font-size: 18px; color: {_icon_color};")
                                            
                                            with ui.column().classes("gap-0 flex-1").style("min-width: 0; overflow: hidden;"):
                                                ui.label(_rt_note).classes("text-sm font-bold").style("white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: var(--mf-text); letter-spacing: -0.01em;")
                                                ui.label(_rt_cat).classes("text-xs font-medium").style("color: var(--mf-muted);")
                                                
                                        ui.label(f"{_rt_sign}{currency(_rt_amt)}").classes("text-sm font-extrabold").style(
                                            f"color: {_rt_color}; font-feature-settings: 'tnum'; white-space: nowrap; letter-spacing: -0.02em;"
                                        )
            except Exception:
                pass

        # 9.8.3: Budgets + Spending Breakdown side by side on desktop, stacked on mobile
        with ui.element('div').classes('mf-home-2col mf-home-section').style('width: 100%; margin-top: 20px;'):
            with ui.element('div'):
                _render_budget_widget()
            with ui.element('div'):
                _render_spending_insights()

        with ui.element('div').classes('mf-home-section').style('width: 100%;'):
            with ui.element('div').classes('mf-dash-grid'):
                with ui.element('div'):
                    _render_recent_tx()


    shell(content)


@ui.page("/add")
def add_page():
    if not require_login():
        nav_to("/login")
        return

    #  8.7: Custom chip-select  completely replaces Quasar q-select 
    def _chip_select(options, value, label=None, hint=None, scrollable=False, disabled=False, max_chips=8, accent_color='#6366f1'):
        """Chip-based option picker.  For high-cardinality fields (>max_chips)
        renders a custom pure-HTML dropdown instead.
        Returns object with .value / .props() / .on() / .set_visibility()."""

        _use_dropdown = len(options) > max_chips

        #  CUSTOM DROPDOWN MODE (9+ options  e.g. Category) 
        if _use_dropdown:
            _dd_state = {'value': value, 'disabled': disabled, 'open': False}
            _dd_cbs: list = []
            _dd_items: dict = {}

            _dd_container = ui.column().classes('w-full gap-1')
            with _dd_container:
                if label:
                    ui.label(label).classes('text-xs font-medium').style(
                        'color: var(--mf-muted); text-transform: uppercase; letter-spacing: 0.06em;'
                    )

                # Trigger button  shows current value + arrow
                _trigger = ui.element('div').classes('mf-dd-trigger')
                with _trigger:
                    _dd_label = ui.label(str(value)).style('pointer-events:none; flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;')
                    ui.icon('expand_more').classes('mf-dd-arrow').style('pointer-events:none; font-size:20px; color:var(--mf-muted); transition:transform 0.15s;')

                # Options panel  hidden until clicked
                _panel = ui.element('div').classes('mf-dd-panel')
                _panel.style('display:none;')
                with _panel:
                    for opt in options:
                        _cls = 'mf-dd-item'
                        if opt == value:
                            _cls += ' active'
                        item = ui.element('div').classes(_cls)
                        with item:
                            ui.label(str(opt)).style('pointer-events:none;')
                        _dd_items[opt] = item

                if hint:
                    ui.label(hint).classes('text-xs').style('color: var(--mf-muted); opacity: 0.6;')

            def _dd_toggle():
                if _dd_state['disabled']:
                    return
                _dd_state['open'] = not _dd_state['open']
                if _dd_state['open']:
                    _panel.style('display:block;')
                    _trigger.classes(add='open')
                else:
                    _panel.style('display:none;')
                    _trigger.classes(remove='open')

            def _dd_pick(opt):
                if _dd_state['disabled'] or _dd_state['value'] == opt:
                    _dd_state['open'] = False
                    _panel.style('display:none;')
                    _trigger.classes(remove='open')
                    return
                old = _dd_state['value']
                _dd_state['value'] = opt
                _dd_label.text = str(opt)
                if old in _dd_items:
                    _dd_items[old].classes(remove='active')
                if opt in _dd_items:
                    _dd_items[opt].classes(add='active')
                _dd_state['open'] = False
                _panel.style('display:none;')
                _trigger.classes(remove='open')
                for cb in _dd_cbs:
                    try:
                        cb(opt)
                    except Exception:
                        pass

            _trigger.on('click', lambda _: _dd_toggle())
            for opt in options:
                _dd_items[opt].on('click', lambda _, o=opt: _dd_pick(o))

            class _SelDropdown:
                @property
                def value(self_):
                    return _dd_state['value']
                @value.setter
                def value(self_, v):
                    if v in _dd_items:
                        _dd_pick(v)
                    else:
                        _dd_state['value'] = v
                        _dd_label.text = str(v)
                def set_visibility(self_, visible):
                    _dd_container.set_visibility(visible)
                def on_change(self_, fn):
                    _dd_cbs.append(fn)
                    return self_
                def props(self_, prop_str=''):
                    if 'disable' in prop_str and 'remove' not in prop_str:
                        _dd_state['disabled'] = True
                        _trigger.classes(add='disabled')
                    elif 'enable' in prop_str or 'remove' in prop_str:
                        _dd_state['disabled'] = False
                        _trigger.classes(remove='disabled')
                    return self_
                def on(self_, event, fn):
                    _dd_cbs.append(lambda v: fn(v))
                    return self_
            return _SelDropdown()

        #  CHIP MODE (8 options  Method, Account, etc.) 
        _state = {'value': value, 'disabled': disabled}
        _chips: dict = {}
        _cbs: list = []

        def _pick(opt):
            if _state['disabled'] or _state['value'] == opt:
                return
            old = _state['value']
            _state['value'] = opt
            if old in _chips:
                _chips[old].classes(remove='active')
                _chips[old].style('background: rgba(0,0,0,0.15) !important; color: var(--mf-text) !important; box-shadow: inset 0 1px 2px rgba(0,0,0,0.2); border: 1px solid rgba(255,255,255,0.05) !important; font-weight: 600;')
            if opt in _chips:
                _chips[opt].classes(add='active')
                _chips[opt].style(f'background: linear-gradient(135deg, {accent_color}, {accent_color}dd) !important; color: white !important; box-shadow: 0 4px 12px {accent_color}40; border: none !important; font-weight: 700;')
            for cb in _cbs:
                try:
                    cb(opt)
                except Exception:
                    pass

        _container = ui.column().classes('w-full gap-2')
        with _container:
            if label:
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.label(label).classes('text-xs font-bold').style(
                        'color: var(--mf-muted); text-transform: uppercase; letter-spacing: 0.08em; opacity: 0.9;'
                    )
            _row_cls = 'mf-chip-row' + (' mf-chip-scroll' if scrollable else '')
            with ui.element('div').classes(_row_cls).style('gap: 10px; padding-bottom: 4px;'):
                for opt in options:
                    _cls = 'mf-chip'
                    if opt == value:
                        _cls += ' active'
                    if disabled:
                        _cls += ' disabled'
                    
                    # Premium chip styling base overrides
                    chip = ui.element('div').classes(_cls)
                    if not disabled:
                        chip.on('click', lambda _, o=opt: _pick(o))
                        if opt == value:
                            # Active Premium State
                            chip.style(f'background: linear-gradient(135deg, {accent_color}, {accent_color}dd) !important; color: white !important; box-shadow: 0 4px 12px {accent_color}40; border: none !important; font-weight: 700;')
                        else:
                            # Inactive Premium State (Lighter Grey)
                            chip.style('background: rgba(255,255,255,0.08) !important; color: var(--mf-text) !important; border: 1px solid rgba(255,255,255,0.1) !important; box-shadow: inset 0 1px 2px rgba(0,0,0,0.2); font-weight: 600;')
                    
                    with chip:
                        ui.label(str(opt)).style('pointer-events: none;')
                    _chips[opt] = chip
            if hint:
                ui.label(hint).classes('text-[11px] font-medium').style('color: var(--mf-muted); opacity: 0.5;')

        class _Sel:
            @property
            def value(self_):
                return _state['value']
            @value.setter
            def value(self_, v):
                if v in _chips:
                    _pick(v)
                else:
                    # Allow setting value to text not in options (e.g. split label)
                    _state['value'] = v
            def set_visibility(self_, visible):
                _container.set_visibility(visible)
            def on_change(self_, fn):
                _cbs.append(fn)
                return self_
            def props(self_, prop_str=''):
                """Compatibility shim  handles 'disable'/'enable' props."""
                if 'disable' in prop_str and 'remove' not in prop_str:
                    _state['disabled'] = True
                    for c in _chips.values():
                        c.classes(add='disabled')
                elif 'enable' in prop_str or 'remove' in prop_str:
                    _state['disabled'] = False
                    for c in _chips.values():
                        c.classes(remove='disabled')
                return self_
            def on(self_, event, fn):
                """Compatibility shim  maps any change event to on_change."""
                _cbs.append(lambda v: fn(v))
                return self_
        return _Sel()

    def open_add_dialog(entry_type: str, *, preset_category: str | None = None, preset_method: str | None = None, preset_account: str | None = None, auto_scan: bool = False):
        rules = load_rules()
        owners = owners_list()
        accounts = accounts_list()
        categories = categories_list()
        methods = methods_list()

        # Remember last-used method/account for Expense (Debit) so you don't reselect every time.
        last_debit_method = str(app.storage.user.get('last_debit_method', '') or '').strip()
        last_debit_account = str(app.storage.user.get('last_debit_account', '') or '').strip()

        # Map entry types to accent colors and icons
        _dlg_accents = {
            'debit': ('#ef4444', 'shopping_cart', 'Expense'),
            'expense': ('#ef4444', 'shopping_cart', 'Expense'),
            'credit': ('#22c55e', 'trending_up', 'Income'),
            'income': ('#22c55e', 'trending_up', 'Income'),
            'investment': ('#a855f7', 'show_chart', 'Investment'),
            'cc repay': ('#eab308', 'credit_card', 'CC Repay'),
            'cc_repay': ('#eab308', 'credit_card', 'CC Repay'),
            'loc draw': ('#60a5fa', 'account_balance', 'LOC Draw'),
            'loc_draw': ('#60a5fa', 'account_balance', 'LOC Draw'),
            'loc withdrawal': ('#60a5fa', 'account_balance', 'LOC Draw'),
            'loc_withdrawal': ('#60a5fa', 'account_balance', 'LOC Draw'),
            'loc repay': ('#2dd4bf', 'swap_horiz', 'LOC Repay'),
            'loc_repay': ('#2dd4bf', 'swap_horiz', 'LOC Repay'),
            'loc repayment': ('#2dd4bf', 'swap_horiz', 'LOC Repay'),
            'loc_repayment': ('#2dd4bf', 'swap_horiz', 'LOC Repay'),
            'international': ('#f472b6', 'public', 'International Transfer'),
            'international transfer': ('#f472b6', 'public', 'International Transfer'),
        }
        _accent, _dicon, _dlabel = _dlg_accents.get(entry_type.lower(), ('#6366f1', 'add_circle', entry_type))

        dlg = ui.dialog()
        dlg.props('transition-show="fade" transition-hide="fade" transition-duration="120"')
        with dlg, ui.card().classes("my-card mf-add-dialog w-full").style("max-width: min(680px, 95vw); max-height: 88vh; overflow-y: auto; padding: 0; border-radius: 32px; box-shadow: 0 40px 80px rgba(0,0,0,0.4), inset 0 2px 4px rgba(255,255,255,0.05); background: linear-gradient(180deg, var(--mf-surface-1), var(--mf-surface-2));"):
            # Premium dialog header  accent strip + header area with background
            ui.element('div').style(f'height: 6px; background: linear-gradient(90deg, {_accent}, {_accent}88); border-radius: 32px 32px 0 0;')
            with ui.element('div').style(
                f'padding: 24px 24px 16px 24px; width: 100%; box-sizing: border-box;'
                f'background: linear-gradient(180deg, {_accent}1A, transparent);'
                f'border-bottom: 1px solid rgba(255,255,255,0.03);'
            ):
                with ui.row().classes('items-center gap-4').style('width: 100%;'):
                    with ui.element('div').style(
                        f'width: 48px; height: 48px; border-radius: 16px; display: flex; align-items: center; justify-content: center;'
                        f'background: linear-gradient(135deg, {_accent}33, {_accent}11); border: 1px solid {_accent}44; box-shadow: 0 8px 16px {_accent}1A;'
                    ):
                        ui.icon(_dicon).style(f'font-size: 24px; color: {_accent};')
                    with ui.column().classes('gap-1'):
                        ui.label(f"Add {_dlabel}").classes('text-2xl font-black').style('letter-spacing: -0.03em; color: var(--mf-text);')
                        ui.label('Fill in the details below').classes('text-sm font-medium').style('color: var(--mf-muted);')
                    ui.element('div').style('flex: 1;')
                    ui.button('', icon='close', on_click=dlg.close).props('flat round dense').style('opacity: 0.5; background: rgba(128,128,128,0.1); border-radius: 50%; padding: 4px; transition: opacity 0.2s; position: absolute; right: 20px; top: 20px;')

            #  Section 1: Date & Amount 
            with ui.element('div').style('padding: 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('items-center gap-2 mb-4'):
                    ui.icon('event').style(f'font-size: 16px; color: {_accent}; opacity: 0.9;')
                    ui.label('Date & Amount').classes('text-xs font-black').style('text-transform: uppercase; letter-spacing: 0.1em; color: var(--mf-muted); opacity: 0.8;')
                
                with ui.row().classes('w-full gap-4'):
                    # Neumorphic Date Input
                    with ui.element('div').classes('flex-1 position-relative'):
                        d_date = ui.input(value=today().isoformat()).props("type=date dense borderless").classes("w-full mf-premium-input")
                        d_date.style('background: rgba(0,0,0,0.2) !important; border: 1px solid rgba(255,255,255,0.08) !important; border-radius: 16px; padding: 4px 12px; font-weight: 600; font-size: 15px; color: var(--mf-text); box-shadow: inset 0 2px 4px rgba(0,0,0,0.3); transition: all 0.2s ease;')

                    # Neumorphic Amount Input (Giant font for emphasis)
                    with ui.element('div').classes('flex-1 position-relative'):
                        d_amount = ui.number(value=0).props("dense borderless").classes("w-full mf-premium-input")
                        d_amount.style('background: rgba(0,0,0,0.2) !important; border: 1px solid rgba(255,255,255,0.08) !important; border-radius: 16px; padding: 4px 12px; font-weight: 800; font-size: 18px; color: var(--mf-text); font-feature-settings: "tnum"; box-shadow: inset 0 2px 4px rgba(0,0,0,0.3); transition: all 0.2s ease;')


            _et = entry_type.lower().strip()
            is_debit = _et in ('debit', 'expense')
            is_income = _et in ('credit', 'income')
            is_invest = _et == 'investment'
            is_cc_repay = _et in ('cc repay', 'cc_repay', 'ccrepay', 'credit card repay', 'credit card repayment')
            is_loc_draw = _et in ('loc draw', 'loc_draw', 'loc withdrawal', 'loc_withdrawal')
            is_loc_repay = _et in ('loc repay', 'loc_repay', 'loc repayment', 'loc_repayment')

            # Phase 6.5+: OCR-triggered multi-category split (Walmart/Costco/Superstore)
            # Stores a plan of category->amount which will be written as multiple transaction rows on Save.
            split_plan: Dict[str, Any] = {
                "enabled": False,
                "merchant": "",
                "amounts": {},  # e.g., {"Groceries": 120.00, "Household": 30.00, "Shopping": 10.00, "Health": 5.00}
                "detected_amounts": {},
            }

            def _norm_merchant(s: str) -> str:
                return re.sub(r'\s+', ' ', (s or '').strip().lower())

            def _is_split_merchant(s: str) -> bool:
                """Merchants where we offer OCR-driven multi-category split."""
                t = _norm_merchant(s)
                return ('walmart' in t) or ('costco' in t) or ('superstore' in t)
            # Entry-type specific defaults
            fixed_method = None
            hide_method = False
            disable_account = False
            hide_category = False
            fixed_category = preset_category

            if is_income:
                fixed_method = 'Bank'
                hide_method = True
                disable_account = True  # income goes to bank; avoid card/LOC accounts
                categories = ['Salary', 'Others']  # 9.9: only two income categories
            if is_invest:
                fixed_method = 'Bank'
                hide_method = True
                disable_account = True
                if not fixed_category:
                    fixed_category = 'Investment'
            if is_cc_repay:
                fixed_method = 'Card'
                hide_method = True
                hide_category = True
                fixed_category = 'CC Repay'
                # 8.4: Restrict CC Repay to only credit card / LOC accounts
                CC_REPAY_ACCOUNTS = ["RBC Line of Credit", "RBC VISA", "RBC Mastercard", "CT Mastercard - Black", "CT Mastercard - Grey"]
                accounts = [a for a in accounts if a in CC_REPAY_ACCOUNTS]
                if not accounts:
                    accounts = CC_REPAY_ACCOUNTS
            if is_loc_draw:
                if not fixed_method:
                    fixed_method = preset_method or 'Card'
                hide_method = True
                if not fixed_category:
                    fixed_category = preset_category or 'LOC Utilization'
            if is_loc_repay:
                if not fixed_method:
                    fixed_method = preset_method or 'Bank'
                hide_method = True
                if not fixed_category:
                    fixed_category = preset_category or 'Repayment'

            default_method = ("Card" if is_debit else ("Bank" if (is_income or is_invest or is_loc_repay) else ("Card" if is_loc_draw else "Bank")))

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
            # 8.4: For CC Repay, force default to first valid CC account (not a stale last_debit_account)
            if is_cc_repay and account_default not in (accounts or []):
                account_default = accounts[0] if accounts else ""
            if account_default and account_default not in (accounts or []):
                accounts = [account_default] + [a for a in (accounts or []) if a != account_default]

            #  Section 2: Payment 
            ui.element('div').style('height: 1px; background: linear-gradient(90deg, transparent, rgba(255,255,255,0.08), transparent); margin: 0 24px;')
            with ui.element('div').style('padding: 16px 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('items-center gap-2 mb-3'):
                    ui.icon('account_balance_wallet').style(f'font-size: 16px; color: {_accent}; opacity: 0.9;')
                    ui.label('Payment').classes('text-xs font-black').style('text-transform: uppercase; letter-spacing: 0.1em; color: var(--mf-muted); opacity: 0.8;')

                # 8.7: Chip-based selects  no Quasar q-select
                if hide_method:
                    d_method = None
                else:
                    d_method = _chip_select(
                        methods or ["Bank"], value=(method_default if method_default in (methods or []) else (methods or ["Bank"])[0]),
                        label="Payment Method",
                        hint="Select payment method" if is_debit else None,
                        accent_color=_accent
                    )

                d_account = _chip_select(
                    accounts or ["Bank"], value=(account_default if account_default in (accounts or []) else (accounts or ["Bank"])[0]),
                    label="Account",
                    hint="Choose your account" if is_debit else None,
                    disabled=disable_account,
                    accent_color=_accent
                )

                if hide_method and fixed_method:
                    ui.label(f"Method: {fixed_method}").classes("text-[11px] font-bold").style("color: var(--mf-muted); opacity: 0.5; margin-top: -4px;")

            #  Section 3: Category & Notes 
            # 8.2.2: hide_category=True for CC Repay (not an expense, no category needed)
            if not hide_category:
                ui.element('div').style('height: 1px; background: linear-gradient(90deg, transparent, rgba(255,255,255,0.08), transparent); margin: 0 24px;')
            with ui.element('div').style('padding: 16px 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                if not hide_category:
                    with ui.row().classes('items-center gap-2 mb-3'):
                        ui.icon('category').style(f'font-size: 16px; color: {_accent}; opacity: 0.9;')
                        ui.label('Category & Details').classes('text-xs font-black').style('text-transform: uppercase; letter-spacing: 0.1em; color: var(--mf-muted); opacity: 0.8;')

                d_category = _chip_select(
                    categories or ["Uncategorized"],
                    value=(fixed_category or "Uncategorized"),
                    label="Category",
                    hint="Pick a spending category" if is_debit else None,
                    accent_color=_accent
                )
                if hide_category:
                    d_category.set_visibility(False)
                
                with ui.element('div').classes('mt-4 position-relative'):
                    d_notes = ui.textarea("Notes / Merchant", value="").props("dense borderless rows=2 autogrow").classes("w-full mf-premium-input")
                    d_notes.style('background: rgba(0,0,0,0.2) !important; border: 1px solid rgba(255,255,255,0.08) !important; border-radius: 16px; padding: 12px 16px; font-weight: 500; font-size: 14px; color: var(--mf-text); box-shadow: inset 0 2px 4px rgba(0,0,0,0.3); transition: all 0.2s ease;')
                
                with ui.element('div').style('margin-top: 16px; padding: 12px 16px; background: var(--mf-surface); border-radius: 12px; border: 1px solid var(--mf-border); display: flex; align-items: center; gap: 12px;'):
                    d_rec = ui.checkbox("Mark as recurring template").style('font-weight: 600; font-size: 13px; color: var(--mf-text);')
                    d_rec.props('color="primary"')
                if hide_category:
                    d_rec.set_visibility(False)

            # Receipt scan (Expense only): opens camera on mobile, runs free OCR in the browser (tesseract.js)
            if entry_type.lower() == 'debit':
                scan_state: Dict[str, Any] = {"data_url": None}

                scan_dlg = ui.dialog()
                scan_progress_dlg = ui.dialog()
                with scan_progress_dlg, ui.card().classes('p-4').style('min-width:260px'):
                    ui.spinner(size='lg')
                    ui.label('Scanning...').classes('text-subtitle1')
                parsed_state: Dict[str, Any] = {"parsed": None}
                scan_dlg.props('transition-show="fade" transition-hide="fade" transition-duration="150"')
                with scan_dlg, ui.card().classes('my-card p-0 w-[720px] max-w-[95vw]').style('background: var(--mf-bg); border: 1px solid var(--mf-border); box-shadow: 0 24px 48px rgba(0,0,0,0.3); max-height: min(88vh, 80dvh); height: min(88vh, 80dvh); display:flex; flex-direction:column; overflow:hidden; border-radius: 24px;'):
                    ui.element('div').style('height: 4px; width: 100%; background: linear-gradient(90deg, #6366f1, #3b82f6, #10b981); flex-shrink: 0;')
                    with ui.column().classes('w-full').style('flex:1; overflow-y:auto; padding: 20px;'):
                        with ui.row().classes('items-center gap-3 mb-4'):
                            with ui.element("div").style("width: 40px; height: 40px; border-radius: 12px; background: linear-gradient(135deg, rgba(99,102,241,0.15), rgba(59,130,246,0.08)); display: flex; align-items: center; justify-content: center; border: 1px solid rgba(99,102,241,0.2);"):
                                ui.icon("document_scanner").style("font-size: 22px; color: #818cf8;")
                            with ui.column().classes("gap-0"):
                                ui.label('Smart Scan').classes('text-lg font-extrabold').style('letter-spacing: -0.02em;')
                                ui.label('Upload receipt to auto-extract details').classes('text-xs').style('color: var(--mf-muted);')

                        preview = ui.image('').classes('w-full rounded').style('display:none')

                        scan_spinner = ui.spinner(size='lg').classes('mx-auto').style('display:none')

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
                                # Keep original bytes for server-side OCR (faster + no huge base64 on mobile)
                                scan_state['img_bytes'] = data

                                # Generate a lightweight preview (downscaled) to avoid iOS lag when selecting images
                                try:
                                    from PIL import Image
                                    im = Image.open(io.BytesIO(data))
                                    im = im.convert('RGB')
                                    im.thumbnail((900, 900))
                                    buf = io.BytesIO()
                                    im.save(buf, format='JPEG', quality=70, optimize=True)
                                    preview_bytes = buf.getvalue()
                                    scan_state['data_url'] = f"data:image/jpeg;base64,{base64.b64encode(preview_bytes).decode('utf-8')}"
                                except Exception:
                                    # fallback: still allow previewless scanning
                                    scan_state['data_url'] = f"data:{mime};base64,{base64.b64encode(data).decode('utf-8')}"

                                if scan_state.get('data_url'):
                                    preview.set_source(scan_state['data_url'])
                                    preview.style('display:block')
                                else:
                                    preview.style('display:none')

                                raw_out.value = ''
                                parsed_state['parsed'] = None
                                parsed_card.style('display:none')
                                apply_btn.disable()
                                _sync_run_btn()
                            except Exception as ex:
                                ui.notify(f'Upload failed: {ex}', type='negative')

                        upload_holder = ui.column().classes('w-full')
                        upload_receipt = None

                        def _mount_upload():
                            nonlocal upload_receipt
                            upload_holder.clear()
                            upload_receipt = ui.upload(auto_upload=True, label='Capture / Upload receipt')                                .props("accept='image/*'")                                .classes('w-full')                                .on_upload(_on_upload)

                        def _sync_run_btn():
                            """Enable/disable the Run scan button based on whether we have an image."""
                            try:
                                has_img = bool(scan_state.get('img_bytes') or scan_state.get('data_url'))
                                if has_img:
                                    try: run_btn.enable()
                                    except Exception: run_btn.props('disable=false')
                                else:
                                    try: run_btn.disable()
                                    except Exception: run_btn.props('disable=true')
                            except Exception:
                                pass

                        def _sync_apply_btn():
                            """Enable/disable the Apply button based on whether we have parsed OCR results."""
                            try:
                                parsed = parsed_state.get('parsed') or scan_state.get('parsed')
                                if parsed:
                                    try: apply_btn.enable()
                                    except Exception: apply_btn.props('disable=false')
                                else:
                                    try: apply_btn.disable()
                                    except Exception: apply_btn.props('disable=true')
                            except Exception:
                                pass

                        def _reset_scan_ui():
                            scan_state['data_url'] = None
                            scan_state['img_bytes'] = None
                            scan_state['ocr_text'] = ''
                            scan_state['parsed'] = None
                            raw_out.value = ''
                            _sync_apply_btn()
                            _sync_run_btn()
                            _mount_upload()

                        _mount_upload()

                        async def _run_ocr() -> None:
                            if not scan_state.get('data_url') and not scan_state.get('img_bytes'):
                                # Some mobile browsers show upload as complete but the server event may not fire.
                                # Try to recover from the upload component's value before warning the user.
                                try:
                                    maybe_files = getattr(upload_receipt, 'value', None)
                                    if maybe_files:
                                        await _on_upload(maybe_files[0])
                                except Exception:
                                    pass
                                if not scan_state.get('data_url') and not scan_state.get('img_bytes'):
                                    ui.notify('Please upload a receipt image first.', type='warning')
                                    return
                            ui.notify('Scanning', type='info', timeout=8.0)
                            # Show busy indicator (mobile Safari can take a while)
                            try:
                                scan_spinner.style('display:block')
                            except Exception:
                                pass
                            try:
                                run_btn.disable()
                            except Exception:
                                pass

                            use_gcv = bool((os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON') or '').strip())
                            result = None
                            if use_gcv:
                                # Prefer server-side OCR (Google Vision) to reduce mobile lag and improve accuracy
                                gcv_text, gcv_dbg = await run.io_bound(lambda: server_ocr_from_data_url(str(scan_state.get('data_url') or ''), return_debug=True))
                                if str(gcv_text).strip():
                                    result = {'ok': True, 'text': str(gcv_text), 'debug': str(gcv_dbg or '')}
                                else:
                                    raw_out.value = (gcv_dbg or 'OCR returned empty text.')
                                    try:
                                        scan_progress_dlg.close()
                                    except Exception:
                                        pass
                                    ui.notify('OCR failed. Details shown in OCR debug box.', type='negative')
                                    return
                            else:
                                # Quick client-side dependency check (if CDN blocked, fail fast with clear message)
                                try:
                                    dep = await ui.run_javascript("return {ok: !!window.Tesseract, ua: navigator.userAgent}", timeout=5.0)
                                    if not (isinstance(dep, dict) and dep.get('ok')):
                                        ui.notify('OCR engine not loaded. Please refresh the page and try again (network/CDN blocked).', type='negative')
                                        return
                                except Exception:
                                    # If this check fails, continue and let main OCR report errors.
                                    pass
                                
                                img_literal = json.dumps(str(scan_state.get('data_url', '')))
                                # Clean, robust JS OCR (client-side). If it fails or returns empty, fall back to server OCR.
                                js = f"""
(async () => {{
  try {{
    const img = {img_literal};
    if (!img) return {{ ok:false, error:'no image' }};
    if (window.Tesseract && typeof Tesseract.recognize === 'function') {{
      const preprocess = async (dataUrl) => new Promise((resolve) => {{
        const im = new Image();
        im.onload = () => {{
          try {{
            const maxW = 1400;
            const maxH = 2400;
            let w = im.width, h = im.height;
            const scale = Math.min(1, maxW / w, maxH / h);
            w = Math.max(1, Math.floor(w * scale));
            h = Math.max(1, Math.floor(h * scale));
            const c = document.createElement('canvas');
            c.width = w; c.height = h;
            const ctx = c.getContext('2d', {{ willReadFrequently: true }});
            ctx.drawImage(im, 0, 0, w, h);
            const imgData = ctx.getImageData(0, 0, w, h);
            const d = imgData.data;
            const contrast = 1.2;
            const intercept = -18;
            for (let i = 0; i < d.length; i += 4) {{
              const r = d[i], g = d[i+1], b = d[i+2];
              let y = 0.2126*r + 0.7152*g + 0.0722*b;
              y = y * contrast + intercept;
              y = Math.max(0, Math.min(255, y));
              const v = (y > 150) ? 255 : 0;
              d[i] = d[i+1] = d[i+2] = v;
            }}
            ctx.putImageData(imgData, 0, 0);
            resolve(c.toDataURL('image/jpeg', 0.9));
          }} catch (e) {{ resolve(dataUrl); }}
        }};
        im.onerror = () => resolve(dataUrl);
        im.src = dataUrl;
      }});
      const small = await preprocess(img);
      const res = await Tesseract.recognize(small, 'eng');
      return {{ ok:true, text:(res?.data?.text || '') }};
    }}
    return {{ ok:false, error:'tesseract.js not loaded' }};
  }} catch (e) {{
    return {{ ok:false, error:String(e) }};
  }}
}})()
"""
                                try:
                                    result = await ui.run_javascript(js, timeout=120.0)
                                except TimeoutError:
                                    result = None
                                except Exception:
                                    result = None
                                # If client OCR failed (or returned empty), fall back to server OCR
                                if (not result) or (not isinstance(result, dict)) or (not result.get('ok')) or (not str((result or {}).get('text') or '').strip()):
                                    fallback_text = server_ocr_from_data_url(str(scan_state.get('data_url') or ''), return_debug=True)[0]
                                    if str(fallback_text).strip():
                                        result = {'ok': True, 'text': str(fallback_text)}
                                if not result or not isinstance(result, dict) or not result.get('ok'):
                                    err = (result or {}).get('error', 'Unknown OCR error') if isinstance(result, dict) else 'Unknown OCR error'
                                    ui.notify(f'OCR failed: {err}', type='negative')
                                    return

                            text = str(result.get('text') or '')
                            raw_out.value = text

                            parsed = parse_receipt_text(text)
                            parsed_state['parsed'] = parsed
                            # Phase 6.5: OCR line-item intelligence (rule-sheet driven)
                            try:
                                rules = load_rules(force=False)
                                items = extract_receipt_line_items(text)

                                # Category split:
                                # Prefer priced line-items. If we can't reliably extract line-items (common on some receipts),
                                # fall back to receipt-level keyword signals (e.g., PHARMACY/RX => Health).
                                detected_total = float(parsed.get('amount') or 0.0)
                                category_amounts: Dict[str, float] = {}
                                category_debug: str = ""

                                if items:
                                    cat_result = classify_receipt_items(items, rules)
                                    category_amounts = cat_result.get('detected_amounts', {}) or {}
                                    category_debug = f"line-item classification: {len(items)} items, total={cat_result.get('detected_total', 0)}"
                                else:
                                    lowtxt = (text or "").lower()

                                    def _blank_split(total: float, main: str) -> Dict[str, float]:
                                        total = float(total or 0.0)
                                        out = {'Groceries': 0.0, 'Household': 0.0, 'Shopping': 0.0, 'Health': 0.0}
                                        if main in out:
                                            out[main] = round(total, 2)
                                        else:
                                            # For 'Uncategorized' or unexpected categories, add them
                                            out[main] = round(total, 2)
                                        return out

                                    # Strong overrides first
                                    if any(k in lowtxt for k in ['pharmacy', 'pharm', ' rx', 'rx ', 'prescription', 'drug', 'dispens', 'otc ', ' otc', 'wellness', 'health care']):
                                        category_amounts = _blank_split(detected_total, 'Health')
                                        category_debug = "(fallback) receipt-level signal: Health (pharmacy/rx)"
                                    elif any(k in lowtxt for k in ['petro canada', 'petro-canada', 'shell', 'costco gas', 'co-op', 'esso', 'gas station']):
                                        category_amounts = _blank_split(detected_total, 'Auto & Transport')
                                        category_debug = "(fallback) receipt-level signal: Auto & Transport (gas merchant)"
                                    elif any(k in lowtxt for k in ["gill's supermarket", "bombay spices", "dino's", "superstore", "no frills", "freshco", "loblaws"]):
                                        category_amounts = _blank_split(detected_total, 'Groceries')
                                        category_debug = "(fallback) receipt-level signal: Groceries (grocery merchant)"
                                    elif any(k in lowtxt for k in ['dollarama', 'dollar tree', 'canadian tire', 'ikea', 'winners', 'marshall', 'value village']):
                                        category_amounts = _blank_split(detected_total, 'Shopping')
                                        category_debug = "(fallback) receipt-level signal: Shopping (store keyword)"
                                    else:
                                        # Lightweight scoring
                                        scores = {'Groceries': 0.0, 'Household': 0.0, 'Shopping': 0.0, 'Health': 0.0}

                                        shop_kw = ['shirt', 'jeans', 'pant', 'pants', 'sock', 'socks', 'shoe', 'shoes', 'apparel', 'clothing', 'jacket', 'coat', 'toy', 'toys', 'electronics', 'headphone', 'beauty', 'makeup', 'jewelry']
                                        house_kw = ['table', 'tables', 'chair', 'chairs', 'desk', 'furniture', 'rug', 'lamp', 'detergent', 'bleach', 'soap', 'paper', 'towel', 'towels', 'toilet', 'tissue', 'dish', 'clean', 'cleaner', 'garbage', 'trash', 'broom', 'mop', 'shampoo', 'household', 'hhold', 'lysol', 'clorox', 'windex', 'swiffer', 'tide', 'downy', 'bounce', 'glad', 'hefty', 'charmin', 'bounty', 'sponge', 'laundry', 'disinfect', 'wipes']
                                        health_kw = ['vitamin', 'medicine', 'medical', 'clinic', 'doctor', 'pharmacy', 'pharm', 'rx', 'otc', 'drug', 'prescription', 'tylenol', 'advil', 'supplement', 'health', 'wellness', 'bandage', 'ointment', 'cough', 'cold medicine', 'first aid']
                                        # 9.8: expanded grocery keywords for better fallback detection
                                        grocery_kw = ['banana', 'bananas', 'apple', 'apples', 'milk', 'bread', 'tofu', 'spinach', 'cauliflower', 'watermelon', 'pear', 'avocado', 'yogurt', 'chicken', 'beef', 'salmon', 'rice', 'pasta', 'cheese', 'egg', 'eggs', 'butter', 'cereal', 'juice', 'coffee', 'snack', 'sauce', 'frozen', 'produce', 'meat', 'deli', 'bakery']

                                        for w in shop_kw:
                                            if w in lowtxt:
                                                scores['Shopping'] += 1.0
                                        for w in house_kw:
                                            if w in lowtxt:
                                                scores['Household'] += 1.0
                                        for w in health_kw:
                                            if w in lowtxt:
                                                scores['Health'] += 1.5
                                        for w in grocery_kw:
                                            if w in lowtxt:
                                                scores['Groceries'] += 1.0

                                        # Walmart is multi-category  do NOT bias toward Groceries

                                        best = max(scores, key=lambda k: scores[k])
                                        if scores[best] <= 0.0:
                                            best = 'Uncategorized'
                                        category_amounts = _blank_split(detected_total, best)
                                        category_debug = f"(fallback) receipt-level signal: {best} | scores={scores}"
                                # Persist line-items and category split for the Split dialog
                                parsed['line_items'] = items
                                parsed['category_amounts'] = category_amounts
                                parsed['category_split_debug'] = category_debug
                                parsed['classified_items'] = cat_result.get('items', []) if items else []
                            except Exception as _cls_err:
                                import traceback
                                _tb = traceback.format_exc()
                                print(f"[FinTrackr] classify_receipt_items error: {_cls_err}\n{_tb}")
                                # Ensure parsed still has empty defaults so the rest of the flow works
                                parsed.setdefault('category_amounts', {})
                                parsed.setdefault('classified_items', [])

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
                            pv_conf.text = f"Amount confidence: {conf:.1f}/10 (source: {src})" + ("  please double-check" if conf < 3.0 else "")
                            parsed_card.style('display:block')
                            apply_btn.enable()

                            if conf < 3.0:
                                ui.notify('Low confidence TOTAL detected  verify amount before applying.', type='warning', timeout=2.0)
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

                            # Phase 6.5+: Build detected category amounts from OCR line-item classification
                            try:
                                split_plan['merchant'] = merch
                                split_plan['enabled'] = False
                                split_plan['amounts'] = {}
                                det = {}
                                try:
                                    cat_amounts = (parsed or {}).get('category_amounts') or {}
                                    if isinstance(cat_amounts, dict):
                                        for k in ['Groceries', 'Household', 'Shopping', 'Health']:
                                            v = float(cat_amounts.get(k, 0.0) or 0.0)
                                            if v > 0.0001:
                                                det[k] = round(v, 2)
                                except Exception:
                                    det = {}
                                split_plan['detected_amounts'] = det

                                # Count how many distinct categories OCR detected
                                _detected_cats = [k for k, v in det.items() if v > 0.01]
                                _n_cats = len(_detected_cats)

                                if entry_type.lower() == 'debit' and _n_cats >= 2:
                                    # Multi-category receipt: open split dialog (any store, not just Walmart)
                                    # Set category to the largest bucket so single-save still works if user skips split
                                    _dominant = max(det, key=det.get) if det else 'Groceries'
                                    _set_category_safely(_dominant)
                                    _open_split_dialog()
                                elif _n_cats == 1:
                                    # Single-category receipt: use the OCR-detected category directly
                                    # (overrides the Notes-based rule inference which may default to Groceries)
                                    _set_category_safely(_detected_cats[0])
                                else:
                                    # No OCR categories detected: fall back to Notes-based suggestion
                                    _refresh_suggestion_now()
                            except Exception:
                                # Fallback: use Notes-based suggestion
                                _refresh_suggestion_now()
                            if conf < 3.0:
                                ui.notify('Applied, but amount confidence was low  please verify before saving.', type='warning')
                            else:
                                ui.notify('Applied scan results. Please review and save.', type='positive')
                            _reset_scan_ui()
                            scan_dlg.close()

                    # Sticky footer so buttons don't get pushed below the upload card on mobile
                    with ui.row().classes('w-full items-center gap-2').style('position: sticky; bottom: 0; background: var(--mf-bg); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); z-index: 20; padding: 14px 20px; border-top: 1px solid var(--mf-border);'):
                        run_btn = ui.button('Scan', icon='document_scanner', on_click=_run_ocr).props('unelevated').classes('flex-1').style(
                            'background: linear-gradient(135deg, #6366f1, #3b82f6) !important; color: #fff !important; border-radius: 10px; font-weight: 600;'
                        )
                        apply_btn = ui.button('Apply', icon='check', on_click=_apply_to_form).props('unelevated')
                        apply_btn.classes('flex-1').style('border-radius: 10px; font-weight: 600;')
                        apply_btn.disable()
                        ui.button('', icon='close', on_click=scan_dlg.close).props('flat round dense').style('border: 1px solid var(--mf-border); border-radius: 10px;')

                def _open_scan_dialog():
                    """Open the receipt scanner dialog (and reset it so user can scan again).
                    9.8: Open dialog first, defer reset to reduce perceived lag."""
                    scan_dlg.open()
                    # Lightweight state reset (no DOM rebuild)
                    scan_state['data_url'] = None
                    scan_state['img_bytes'] = None
                    scan_state['ocr_text'] = ''
                    scan_state['parsed'] = None
                    raw_out.value = ''
                    _sync_apply_btn()
                    _sync_run_btn()
                    # Defer heavy upload widget rebuild to after dialog is rendered
                    async def _deferred_mount():
                        await asyncio.sleep(0.08)
                        _mount_upload()
                        try:
                            ui.run_javascript("""try{const el=document.querySelector('.q-uploader__input'); if(el) el.value='';}catch(e){}""")
                        except Exception:
                            pass
                    asyncio.ensure_future(_deferred_mount())

                with ui.element('div').style('padding: 12px 24px 0 24px;'):
                    btn_scan_receipt = ui.button('Scan receipt', icon='document_scanner', on_click=_open_scan_dialog).props('outline').classes('w-full').style(
                        'border-radius: 12px; border-color: var(--mf-border); color: var(--mf-text); font-weight: 600;'
                    )

                # Auto-open scan dialog if requested (from "Scan Now" hero button)
                # 9.8.1: Defer scan dialog open to let the Add dialog fully render first
                if auto_scan:
                    async def _deferred_auto_scan():
                        await asyncio.sleep(0.3)
                        _open_scan_dialog()
                    asyncio.ensure_future(_deferred_auto_scan())


                # Phase 6.5: Multi-category split UI  shown only after OCR Apply for Walmart/Costco/Superstore
                with ui.element('div').style('padding: 0 24px;'):
                    split_banner = ui.element('div').style(
                        'display:none; background: linear-gradient(135deg, rgba(34,197,94,0.10), rgba(251,191,36,0.08));'
                        'border: 1px solid rgba(34,197,94,0.25); border-radius: 14px; padding: 14px 16px;'
                        'margin: 4px 0;'
                    )
                with split_banner:
                    with ui.row().classes('items-center gap-2 mb-2'):
                        ui.icon('call_split').style('font-size: 18px; color: #22c55e;')
                        ui.label('Split Active').classes('text-sm font-bold').style('color: #22c55e;')
                    split_hint = ui.label("").classes("text-xs").style("color: var(--mf-text); line-height: 1.5;")
                    split_item_list = ui.column().classes('gap-1 mt-1')

                split_dlg = ui.dialog()
                with split_dlg, ui.card().classes("my-card mf-split-card p-4 w-[600px] max-w-[95vw]").style("max-height: 78vh; overflow-y:auto;"):
                    ui.label("Split this receipt").classes("text-lg font-bold")
                    ui.label("We detected line items. Adjust amounts if needed, then Apply.").classes("text-xs").style("color: var(--mf-muted)")

                    # Four fixed buckets (your preferred setup)
                    split_cats = ["Groceries", "Household", "Shopping", "Health"]
                    amt_inputs: Dict[str, Any] = {}
                    pct_labels: Dict[str, Any] = {}
                    warn_lbl = ui.label("").classes("text-xs q-mt-sm").style("color: var(--mf-muted)")

                    def _round2(x: float) -> float:
                        try:
                            return round(float(x), 2)
                        except Exception:
                            return 0.0

                    def _sum_amounts() -> float:
                        total = 0.0
                        for c in split_cats:
                            total += float(to_float(getattr(amt_inputs[c], 'value', 0.0) or 0.0))
                        return _round2(total)

                    # 9.8: Track which category the user is actively editing for auto-balance
                    _split_editing: Dict[str, Any] = {"active_cat": None, "_updating": False}

                    def _auto_balance(changed_cat: str) -> None:
                        """When user changes one split amount, auto-adjust the largest OTHER bucket to keep total balanced."""
                        if _split_editing.get('_updating'):
                            return
                        _split_editing['_updating'] = True
                        try:
                            total_amt = _round2(float(to_float(d_amount.value)))
                            # Sum of all OTHER categories (not the one being edited)
                            others_sum = 0.0
                            for c in split_cats:
                                if c != changed_cat:
                                    others_sum += _round2(float(to_float(getattr(amt_inputs[c], 'value', 0.0) or 0.0)))
                            changed_val = _round2(float(to_float(getattr(amt_inputs[changed_cat], 'value', 0.0) or 0.0)))
                            remainder = _round2(total_amt - changed_val - others_sum)

                            # Find the largest OTHER category to absorb the remainder
                            if abs(remainder) > 0.02:
                                # Pick the primary/largest bucket among others
                                best_cat = None
                                best_val = -1.0
                                for c in split_cats:
                                    if c != changed_cat:
                                        v = _round2(float(to_float(getattr(amt_inputs[c], 'value', 0.0) or 0.0)))
                                        if v > best_val:
                                            best_val = v
                                            best_cat = c
                                if not best_cat:
                                    best_cat = split_cats[0] if split_cats[0] != changed_cat else split_cats[1]
                                new_val = _round2(float(to_float(getattr(amt_inputs[best_cat], 'value', 0.0) or 0.0)) + remainder)
                                if new_val < 0:
                                    new_val = 0.0
                                amt_inputs[best_cat].value = new_val
                        except Exception:
                            pass
                        finally:
                            _split_editing['_updating'] = False

                    def _refresh_pcts(_: Any = None) -> None:
                        total_amt = _round2(float(to_float(d_amount.value)))
                        cur_sum = _sum_amounts()
                        # percent labels
                        for c in split_cats:
                            v = _round2(float(to_float(getattr(amt_inputs[c], 'value', 0.0) or 0.0)))
                            pct = 0
                            if total_amt > 0.0001:
                                pct = int(round(100.0 * (v / total_amt)))
                            try:
                                pct_labels[c].text = f"{pct}%"
                            except Exception:
                                pass
                        # warning / remainder
                        diff = _round2(total_amt - cur_sum)
                        if abs(diff) <= 0.02:
                            warn_lbl.text = f"Total: ${total_amt:,.2f}  Split: ${cur_sum:,.2f}"
                        else:
                            if diff > 0:
                                warn_lbl.text = f"Total: ${total_amt:,.2f}  Split: ${cur_sum:,.2f}  Remaining: ${diff:,.2f}"
                            else:
                                warn_lbl.text = f"Total: ${total_amt:,.2f}  Split: ${cur_sum:,.2f}  Over by: ${abs(diff):,.2f}"

                    def _make_on_change(cat_name: str):
                        """Create a per-category change handler that auto-balances then refreshes."""
                        def _handler(_: Any = None):
                            _auto_balance(cat_name)
                            _refresh_pcts()
                        return _handler

                    # Grid-like rows
                    for c in split_cats:
                        with ui.row().classes("w-full items-center justify-between gap-2 q-mt-sm"):
                            ui.label(c).classes("text-sm font-medium")
                            pct_labels[c] = ui.label("0%").classes("text-xs").style("color: var(--mf-muted)")
                            amt_inputs[c] = ui.number(value=0, step=0.01).props('dense outlined prefix=$').classes('w-40')
                            amt_inputs[c].on('update:model-value', _make_on_change(c))

                    def _largest_bucket() -> str:
                        """Return the category with the largest current amount (for remainder allocation)."""
                        best_cat, best_val = 'Groceries', 0.0
                        for c in split_cats:
                            try:
                                v = float(to_float(getattr(amt_inputs[c], 'value', 0.0) or 0.0))
                            except Exception:
                                v = 0.0
                            if v > best_val:
                                best_val = v
                                best_cat = c
                        return best_cat

                    def _reset_to_detected() -> None:
                        det = split_plan.get('detected_amounts') or {}
                        total_amt = _round2(float(to_float(d_amount.value)))
                        # start with detected
                        for c in split_cats:
                            try:
                                amt_inputs[c].value = _round2(float(det.get(c, 0.0)))
                            except Exception:
                                amt_inputs[c].value = 0.0
                        # ensure it sums to total by allocating remainder to the largest bucket
                        cur_sum = _sum_amounts()
                        diff = _round2(total_amt - cur_sum)
                        if abs(diff) > 0.02:
                            target = _largest_bucket()
                            try:
                                amt_inputs[target].value = _round2(float(to_float(amt_inputs[target].value)) + diff)
                            except Exception:
                                pass
                        _refresh_pcts()

                    def _all_to_groceries() -> None:
                        total_amt = _round2(float(to_float(d_amount.value)))
                        for c in split_cats:
                            amt_inputs[c].value = 0.0
                        amt_inputs['Groceries'].value = total_amt
                        _refresh_pcts()

                    with ui.row().classes('w-full justify-between items-center q-mt-md'):
                        ui.button('Reset to detected', on_click=_reset_to_detected).props('flat')
                        ui.button('All to Groceries', on_click=_all_to_groceries).props('flat')

                    def _apply_multi_split() -> None:
                        total_amt = _round2(float(to_float(d_amount.value)))
                        cur_sum = _sum_amounts()
                        diff = _round2(total_amt - cur_sum)
                        # If slightly off, auto-fix by nudging the largest bucket
                        if abs(diff) <= 0.05:
                            target = _largest_bucket()
                            try:
                                amt_inputs[target].value = _round2(float(to_float(amt_inputs[target].value)) + diff)
                            except Exception:
                                pass
                            cur_sum = _sum_amounts()
                            diff = _round2(total_amt - cur_sum)
                        if abs(diff) > 0.05:
                            ui.notify('Split must match the receipt total (adjust amounts).', type='warning')
                            return

                        plan: Dict[str, float] = {}
                        for c in split_cats:
                            v = _round2(float(to_float(getattr(amt_inputs[c], 'value', 0.0) or 0.0)))
                            if v > 0.009:
                                plan[c] = v

                        # Store plan; actual save happens on Save click
                        split_plan['enabled'] = True
                        split_plan['amounts'] = plan
                        n_cats = len(plan)

                        # Show prominent split banner in main dialog
                        try:
                            split_banner.style('display: block;')
                            parts = [f"{k}: ${v:.2f}" for k, v in plan.items()]
                            split_hint.text = f"Will save as {n_cats} separate transactions:"
                            split_item_list.clear()
                            with split_item_list:
                                for k, v in plan.items():
                                    pct = int(round(100 * (v / total_amt))) if total_amt > 0 else 0
                                    with ui.row().classes('items-center gap-2'):
                                        ui.element('div').style(f'width: 8px; height: 8px; border-radius: 50%; background: #22c55e; flex-shrink: 0;')
                                        ui.label(f"{k}  ${v:.2f} ({pct}%)").classes('text-xs font-medium')
                            # Disable category selector (split overrides it)
                            d_category.props('disable')
                            d_category.value = f'Split ({n_cats} categories)'
                        except Exception:
                            pass

                        split_dlg.close()
                        ui.notify(f'Split applied: {n_cats} categories will be saved separately.', type='positive')

                    with ui.row().classes("w-full justify-end gap-2 q-mt-md"):
                        ui.button("Cancel", on_click=split_dlg.close).props("flat")
                        ui.button("Apply", on_click=_apply_multi_split).props("unelevated")

                def _open_split_dialog() -> None:
                    # Initialize from detected amounts (or fallback)
                    _reset_to_detected()
                    split_dlg.open()

            # --- Live category suggestion (Phase 6.2+): auto-categorize as you type Notes (debounced),
            #     show a small chip "Auto: <Category>", and never override a manual category choice ---
            category_touched = {"v": False}         # user manually changed category
            _setting_category = {"v": False}        # internal guard so programmatic changes don't mark touched
            _debounce_task = {"t": None}

            # Small chip-style feedback (shown only when auto is active and suggestion is meaningful)
            with ui.element('div').style('padding: 0 24px;'):
                suggest_chip = ui.chip("").classes("q-mt-xs").style(
                    "background: rgba(120,160,255,0.14); border: 1px solid var(--mf-border); color: var(--mf-text);"
                )
            try:
                suggest_chip.set_visibility(False)
            except Exception:
                suggest_chip.visible = False

            # Use the rules loaded at dialog open; fall back to a non-forced load once if empty.
            _active_rules = rules or []
            if not _active_rules:
                try:
                    _active_rules = load_rules(force=False) or []
                except Exception:
                    _active_rules = []

            def _set_category_safely(val: str) -> None:
                try:
                    _setting_category["v"] = True
                    d_category.value = val
                finally:
                    _setting_category["v"] = False

            def _update_chip(text: str, show: bool) -> None:
                try:
                    suggest_chip.set_text(text)
                except Exception:
                    suggest_chip.text = text
                try:
                    suggest_chip.set_visibility(show)
                except Exception:
                    suggest_chip.visible = bool(show)

            def _refresh_suggestion_now() -> None:
                if category_touched["v"]:
                    _update_chip("", False)
                    return

                active_rules = _active_rules
                note_txt = str(d_notes.value or "").strip()
                if not active_rules:
                    _set_category_safely("Uncategorized")
                    _update_chip("", False)
                    return

                suggestion = infer_category(note_txt, active_rules) or "Uncategorized"
                _set_category_safely(suggestion)
                show = bool(note_txt) and suggestion != "Uncategorized"
                _update_chip(f"Auto: {suggestion}", show)

            async def _debounced_refresh() -> None:
                try:
                    await asyncio.sleep(0.35)
                    _refresh_suggestion_now()
                except Exception:
                    pass

            def _schedule_refresh(_: Any = None) -> None:
                t = _debounce_task.get("t")
                try:
                    if t and not t.done():
                        t.cancel()
                except Exception:
                    pass
                try:
                    _debounce_task["t"] = asyncio.create_task(_debounced_refresh())
                except Exception:
                    _refresh_suggestion_now()

            def _on_category_change(e: Any) -> None:
                if _setting_category["v"]:
                    return
                category_touched["v"] = True
                _update_chip("", False)

            d_category.on('update:model-value', _on_category_change)
            d_notes.on('update:model-value', _schedule_refresh)
            _refresh_suggestion_now()

            def autofill():
                category_touched["v"] = True
                _update_chip("", False)
                fresh_rules = load_rules(force=True)
                if not fresh_rules:
                    ui.notify('No rules loaded (check Rules sheet columns). Keeping Uncategorized.', type='warning')
                    d_category.value = 'Uncategorized'
                    return
                d_category.value = infer_category(d_notes.value or "", fresh_rules) or "Uncategorized"
                ui.notify("Category updated", type="positive")

            with ui.element('div').style('padding: 0 24px;'):
                ui.button("Auto-category", on_click=autofill).props("flat")

            async def save():

                dd = parse_date(d_date.value) or today()

                amt = float(to_float(d_amount.value))

                owner = "Family"

                method = str(((d_method.value if d_method is not None else method_default) or "Bank")).strip()

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

                # Phase 6.5+: If a multi-split plan is enabled, save as multiple linked transactions
                if entry_type.lower() == 'debit' and bool(split_plan.get("enabled")) and isinstance(split_plan.get("amounts"), dict):
                    try:
                        total_amt = float(to_float(d_amount.value))
                        plan: Dict[str, float] = {k: float(v) for k, v in (split_plan.get('amounts') or {}).items()}
                        # Validate plan sums to total (tolerate rounding)
                        s = round(sum(plan.values()), 2)
                        if abs(round(total_amt - s, 2)) > 0.05:
                            ui.notify('Split total does not match receipt total. Please adjust and try again.', type='warning')
                            return
                        # Nudge rounding diff into the largest category bucket
                        diff = round(total_amt - s, 2)
                        if abs(diff) <= 0.05 and abs(diff) > 0.001:
                            largest_cat = max(plan, key=lambda k: plan.get(k, 0.0)) if plan else 'Groceries'
                            plan[largest_cat] = round(plan.get(largest_cat, 0.0) + diff, 2)
                        # Filter zero/negative
                        plan = {k: round(v, 2) for k, v in plan.items() if v and v > 0.009}
                        if not plan:
                            ui.notify('Split plan is empty.', type='warning')
                            return

                        group_id = sha16(f"SPLIT|{owner}|{dd.isoformat()}|{account}|{method}|{total_amt}|{notes}|{dt.datetime.now().isoformat()}")
                        idx = 1
                        n = len(plan)
                        # 9.8.1: Async split saves
                        for cat, _split_amt in plan.items():
                            _split_payload = dict(
                                tx_id=sha16(group_id + f"|{idx}"),
                                date_=dd, owner=owner, type_=entry_type,
                                amount=float(_split_amt), method=method, account=account,
                                category=str(cat),
                                notes=(notes + f" | split:{group_id} {idx}/{n}").strip(),
                                recurring_id="",
                            )
                            await run.io_bound(lambda p=_split_payload: append_tx(**p))
                            idx += 1

                        invalidate('transactions')
                        cats_str = ', '.join(plan.keys())
                        ui.notify(f" Saved {n} separate transactions: {cats_str}", type="positive", timeout=5.0)
                        dlg.close()
                        return
                    except Exception as e:
                        ui.notify(f"Split save failed: {e}", type="negative")
                        return

                try:

                    # Build tx id (unique)
                    tx_id = sha16(f"{owner}|{dd.isoformat()}|{entry_type}|{amt}|{method}|{account}|{category}|{notes}|{dt.datetime.now().isoformat()}")

                    rec_id = ""
                    if d_rec.value:
                        rec_id = create_or_update_recurring_template(
                            owner=owner, type_=entry_type, amount=amt,
                            method=method, account=account, category=category,
                            notes=notes, day_of_month=dd.day, start_date=dd, active=True,
                        )

                    # 9.8.1: Async save — write to Google Sheets in background for snappy UX
                    _tx_payload = dict(
                        tx_id=tx_id, date_=dd, owner=owner, type_=entry_type,
                        amount=amt, method=method, account=account,
                        category=category, notes=notes, recurring_id=rec_id,
                    )
                    await run.io_bound(lambda: append_tx(**_tx_payload))

                    invalidate('transactions')
                    if d_rec.value:
                        invalidate('recurring')

                    ui.notify("\u2713 Saved", type="positive")
                    dlg.close()

                except Exception as e:

                    ui.notify(f"Save failed: {e}", type="negative")


            # Sticky footer so Save/Cancel never get pushed off-screen on mobile
            with ui.row().classes("w-full justify-end gap-3 sticky bottom-0 z-50").style(
                "padding: 16px 24px 20px 24px;"
                "background: linear-gradient(to top, var(--mf-surface-2) 60%, transparent); backdrop-filter: blur(8px); -webkit-backdrop-filter: blur(8px);"
                "border-top: 1px solid rgba(255,255,255,0.05);"
                "border-radius: 0 0 32px 32px;"
            ):
                ui.button("Cancel", on_click=dlg.close).props("flat").style("border-radius: 12px; font-weight: 600; color: var(--mf-text); opacity: 0.8;")

                ui.button("Save", on_click=save, icon="check").props("unelevated").style(
                    f"background: linear-gradient(135deg, {_accent}, {_accent}dd) !important; color: #fff !important;"
                    "font-weight: 800; border-radius: 14px; padding: 10px 36px; font-size: 15px;"
                    f"box-shadow: 0 8px 24px {_accent}40; border: 1px solid rgba(255,255,255,0.1);"
                )

        ui.run_javascript('window.mfSetTheme(localStorage.getItem(\\"mf_theme\\")||\\"Midnight Blue\\");')
        dlg.open()

    # 
    # 8.4: Line of Credit  full dialog with date, amount, type selector
    # 
    def open_loc_dialog():
        loc_dlg = ui.dialog()
        with loc_dlg, ui.card().classes("my-card mf-add-dialog").style(
            "width: min(640px, 95vw); max-width: 95vw; max-height: 88vh; overflow-y: auto; padding: 0; border-radius: 24px;"
        ):
            ui.element('div').style('height: 4px; background: linear-gradient(90deg, #60a5fa, #60a5fa66); border-radius: 24px 24px 0 0;')
            with ui.element('div').style('padding: 12px 24px 6px 24px; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('items-center gap-3').style('width: 100%;'):
                    with ui.element('div').style(
                        'width: 36px; height: 36px; border-radius: 12px; display: flex; align-items: center; justify-content: center;'
                        'background: rgba(96,165,250,0.18); border: 1px solid rgba(96,165,250,0.22);'
                    ):
                        ui.icon('account_balance').style('font-size: 18px; color: #60a5fa;')
                    with ui.column().classes('gap-0'):
                        ui.label('Line of Credit').classes('text-lg font-extrabold').style('letter-spacing: -0.02em;')
                        ui.label('Record LOC withdrawal or repayment').classes('text-xs').style('color: var(--mf-muted);')
                    ui.element('div').style('flex: 1;')
                    ui.button('', icon='close', on_click=loc_dlg.close).props('flat round dense').style('opacity: 0.7;')

            # Date & Amount
            with ui.element('div').style('padding: 0 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('w-full gap-3'):
                    loc_date = ui.input(value=today().isoformat()).props("type=date outlined dense").classes("flex-1 mf-no-label")
                    loc_amount = ui.number(value=0).props("outlined dense").classes("flex-1 mf-no-label")

            # Transaction type
            ui.element('div').style('height: 1px; background: var(--mf-border); opacity: 0.4; margin: 12px 24px 0 24px;')
            with ui.element('div').style('padding: 0 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('items-center gap-2 mt-3 mb-2'):
                    ui.icon('swap_horiz').style('font-size: 15px; color: #60a5fa; opacity: 0.7;')
                    ui.label('Transaction Details').classes('text-xs font-bold').style('text-transform: uppercase; letter-spacing: 0.08em; color: var(--mf-muted);')
                loc_type = _chip_select(
                    ['Withdrawal', 'Repayment'], value='Withdrawal', label='Transaction Type',
                )
                loc_notes = ui.textarea("Notes", value="").props("outlined dense rows=2").classes("w-full")

            # Actions
            with ui.row().classes("w-full justify-end gap-3").style("padding: 14px 24px 12px 24px;"):
                ui.button("Cancel", on_click=loc_dlg.close).props("flat").style("border-radius: 10px;")
                def _save_loc():
                    dd = parse_date(loc_date.value) or today()
                    amt_val = float(to_float(loc_amount.value))
                    if amt_val <= 0:
                        ui.notify("Enter a valid amount", type="warning")
                        return
                    is_withdraw = loc_type.value == 'Withdrawal'
                    _type = 'LOC Draw' if is_withdraw else 'LOC Repay'
                    _method = 'Card' if is_withdraw else 'Bank'
                    _category = 'LOC Utilization' if is_withdraw else 'Repayment'
                    _notes_str = str(loc_notes.value or "").strip()
                    tx_id = sha16(f"Family|{dd.isoformat()}|{_type}|{amt_val}|{_method}|RBC Line of Credit|{_category}|{_notes_str}|{dt.datetime.now().isoformat()}")
                    append_tx(
                        tx_id=tx_id, date_=dd, owner="Family",
                        type_=_type, amount=amt_val,
                        method=_method, account="RBC Line of Credit",
                        category=_category,
                        notes=_notes_str,
                        recurring_id=""
                    )
                    invalidate('transactions')
                    _action = "withdrawal" if is_withdraw else "repayment"
                    ui.notify(f"LOC {_action} of {currency(amt_val)} saved", type="positive")
                    loc_dlg.close()
                ui.button("Save", on_click=_save_loc, icon="check").props("unelevated").style(
                    "background: linear-gradient(135deg, #60a5fa, #60a5facc) !important; color: #fff !important;"
                    "font-weight: 700; border-radius: 12px; padding: 8px 32px;"
                )
        ui.run_javascript('window.mfSetTheme(localStorage.getItem(\\"mf_theme\\")||\\"Midnight Blue\\");')
        loc_dlg.open()

    # 
    # 8.5: CC Repay  rebuilt from scratch, standalone dialog
    # 
    def open_cc_repay_dialog():
        # Only the 4 credit cards (NOT Line of Credit  that's handled by LOC dialog)
        CC_CARDS = [
            'CT Mastercard - Black',
            'CT Mastercard - Grey',
            'RBC VISA',
            'RBC Mastercard',
        ]

        cc_dlg = ui.dialog()
        with cc_dlg, ui.card().classes("my-card mf-add-dialog").style(
            "width: min(640px, 95vw); max-width: 95vw; max-height: 88vh; overflow-y: auto; padding: 0; border-radius: 24px;"
        ):
            ui.element('div').style('height: 4px; background: linear-gradient(90deg, #eab308, #eab30866); border-radius: 24px 24px 0 0;')
            with ui.element('div').style('padding: 12px 24px 6px 24px; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('items-center gap-3').style('width: 100%;'):
                    with ui.element('div').style(
                        'width: 36px; height: 36px; border-radius: 12px; display: flex; align-items: center; justify-content: center;'
                        'background: rgba(251,191,36,0.18); border: 1px solid rgba(251,191,36,0.22);'
                    ):
                        ui.icon('credit_card').style('font-size: 18px; color: #eab308;')
                    with ui.column().classes('gap-0'):
                        ui.label('CC Repayment').classes('text-lg font-extrabold').style('letter-spacing: -0.02em;')
                        ui.label('Record a credit card payment').classes('text-xs').style('color: var(--mf-muted);')
                    ui.element('div').style('flex: 1;')
                    ui.button('', icon='close', on_click=cc_dlg.close).props('flat round dense').style('opacity: 0.7;')

            # Date & Amount
            with ui.element('div').style('padding: 0 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('w-full gap-3'):
                    cc_date = ui.input(value=today().isoformat()).props("type=date outlined dense").classes("flex-1 mf-no-label")
                    cc_amount = ui.number(value=0).props("outlined dense").classes("flex-1 mf-no-label")

            # Card selection
            ui.element('div').style('height: 1px; background: var(--mf-border); opacity: 0.4; margin: 12px 24px 0 24px;')
            with ui.element('div').style('padding: 0 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('items-center gap-2 mt-3 mb-2'):
                    ui.icon('credit_card').style('font-size: 15px; color: #eab308; opacity: 0.7;')
                    ui.label('Card').classes('text-xs font-bold').style('text-transform: uppercase; letter-spacing: 0.08em; color: var(--mf-muted);')
                cc_card = _chip_select(
                    CC_CARDS, value=CC_CARDS[0],
                    hint="Select which card you paid",
                )
                cc_notes = ui.textarea("Notes", value="").props("outlined dense rows=2").classes("w-full")

            # Actions
            with ui.row().classes("w-full justify-end gap-3").style("padding: 14px 24px 12px 24px;"):
                ui.button("Cancel", on_click=cc_dlg.close).props("flat").style("border-radius: 10px;")
                def _save_cc_repay():
                    dd = parse_date(cc_date.value) or today()
                    amt_val = float(to_float(cc_amount.value))
                    if amt_val <= 0:
                        ui.notify("Enter a valid amount", type="warning")
                        return
                    _card = str(cc_card.value).strip()
                    _notes_str = str(cc_notes.value or "").strip()
                    tx_id = sha16(f"Family|{dd.isoformat()}|CC Repay|{amt_val}|Card|{_card}|CC Repay|{_notes_str}|{dt.datetime.now().isoformat()}")
                    append_tx(
                        tx_id=tx_id, date_=dd, owner="Family",
                        type_="CC Repay", amount=amt_val,
                        method="Card", account=_card,
                        category="CC Repay",
                        notes=_notes_str,
                        recurring_id=""
                    )
                    invalidate('transactions')
                    ui.notify(f"CC payment of {currency(amt_val)} to {_card} saved", type="positive")
                    cc_dlg.close()
                ui.button("Save", on_click=_save_cc_repay, icon="check").props("unelevated").style(
                    "background: linear-gradient(135deg, #eab308, #eab308cc) !important; color: #fff !important;"
                    "font-weight: 700; border-radius: 12px; padding: 8px 32px;"
                )
        ui.run_javascript('window.mfSetTheme(localStorage.getItem(\\"mf_theme\\")||\\"Midnight Blue\\");')
        cc_dlg.open()

    # 
    # 8.2.2: Invest  custom dialog with investment account picker
    # 
    def open_invest_dialog():
        INVEST_ACCOUNTS = ['FHSA', 'TFSA', 'RRSP', 'Indhu-TFSA']

        inv_dlg = ui.dialog()
        with inv_dlg, ui.card().classes("my-card mf-add-dialog").style(
            "width: min(640px, 95vw); max-width: 95vw; max-height: 88vh; overflow-y: auto; padding: 0; border-radius: 24px;"
        ):
            ui.element('div').style('height: 4px; background: linear-gradient(90deg, #a855f7, #a855f766); border-radius: 24px 24px 0 0;')
            with ui.element('div').style('padding: 12px 24px 6px 24px; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('items-center gap-3').style('width: 100%;'):
                    with ui.element('div').style(
                        'width: 36px; height: 36px; border-radius: 12px; display: flex; align-items: center; justify-content: center;'
                        'background: rgba(168,85,247,0.18); border: 1px solid rgba(168,85,247,0.22);'
                    ):
                        ui.icon('show_chart').style('font-size: 18px; color: #a855f7;')
                    with ui.column().classes('gap-0'):
                        ui.label('Add Investment').classes('text-lg font-extrabold').style('letter-spacing: -0.02em;')
                        ui.label('Choose investment account & amount').classes('text-xs').style('color: var(--mf-muted);')
                    ui.element('div').style('flex: 1;')
                    ui.button('', icon='close', on_click=inv_dlg.close).props('flat round dense').style('opacity: 0.7;')

            # Date & Amount
            with ui.element('div').style('padding: 0 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('w-full gap-3'):
                    inv_date = ui.input(value=today().isoformat()).props("type=date outlined dense").classes("flex-1 mf-no-label")
                    inv_amount = ui.number(value=0).props("outlined dense").classes("flex-1 mf-no-label")

            # Investment account & Source
            ui.element('div').style('height: 1px; background: var(--mf-border); opacity: 0.4; margin: 12px 24px 0 24px;')
            with ui.element('div').style('padding: 0 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('items-center gap-2 mt-3 mb-2'):
                    ui.icon('savings').style('font-size: 15px; color: #a855f7; opacity: 0.7;')
                    ui.label('Investment Details').classes('text-xs font-bold').style('text-transform: uppercase; letter-spacing: 0.08em; color: var(--mf-muted);')
                inv_account = _chip_select(
                    INVEST_ACCOUNTS, value='TFSA', label='Investment Account',
                    hint="Where to invest (FHSA, TFSA, RRSP...)",
                )
                inv_source = _chip_select(
                    ['Bank'], value='Bank', label='Source Account', disabled=True,
                )
                inv_notes = ui.textarea("Notes", value="").props("outlined dense rows=2").classes("w-full")

            # Actions
            with ui.row().classes("w-full justify-end gap-3").style("padding: 14px 24px 12px 24px;"):
                ui.button("Cancel", on_click=inv_dlg.close).props("flat").style("border-radius: 10px;")
                def _save_invest():
                    dd = parse_date(inv_date.value) or today()
                    amt_val = float(to_float(inv_amount.value))
                    if amt_val <= 0:
                        ui.notify("Enter a valid amount", type="warning")
                        return
                    _notes_str = str(inv_notes.value or "").strip()
                    if inv_source.value:
                        _notes_str = ((_notes_str + " " if _notes_str else "") + f"[from {inv_source.value}]")
                    tx_id = sha16(f"Family|{dd.isoformat()}|Investment|{amt_val}|Bank|{inv_account.value}|Investment|{_notes_str}|{dt.datetime.now().isoformat()}")
                    append_tx(
                        tx_id=tx_id, date_=dd, owner="Family",
                        type_="Investment", amount=amt_val,
                        method="Bank", account=str(inv_account.value),
                        category="Investment",
                        notes=_notes_str,
                        recurring_id=""
                    )
                    invalidate('transactions')
                    ui.notify(f"Investment of {currency(amt_val)} to {inv_account.value} saved", type="positive")
                    inv_dlg.close()
                ui.button("Save", on_click=_save_invest, icon="check").props("unelevated").style(
                    "background: linear-gradient(135deg, #a855f7, #a855f7cc) !important; color: #fff !important;"
                    "font-weight: 700; border-radius: 12px; padding: 8px 32px;"
                )
        ui.run_javascript('window.mfSetTheme(localStorage.getItem(\\"mf_theme\\")||\\"Midnight Blue\\");')
        inv_dlg.open()

    # 
    # 8.2.2: International Transfer  custom dialog (CAD only)
    # 
    def open_intl_dialog():
        INTL_SOURCES = ['Bank', 'CT Mastercard - Grey']

        intl_dlg = ui.dialog()
        with intl_dlg, ui.card().classes("my-card mf-add-dialog").style(
            "width: min(640px, 95vw); max-width: 95vw; max-height: 88vh; overflow-y: auto; padding: 0; border-radius: 24px;"
        ):
            ui.element('div').style('height: 4px; background: linear-gradient(90deg, #f472b6, #f472b666); border-radius: 24px 24px 0 0;')
            with ui.element('div').style('padding: 12px 24px 6px 24px; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('items-center gap-3').style('width: 100%;'):
                    with ui.element('div').style(
                        'width: 36px; height: 36px; border-radius: 12px; display: flex; align-items: center; justify-content: center;'
                        'background: rgba(244,114,182,0.18); border: 1px solid rgba(244,114,182,0.22);'
                    ):
                        ui.icon('public').style('font-size: 18px; color: #f472b6;')
                    with ui.column().classes('gap-0'):
                        ui.label('International Transfer').classes('text-lg font-extrabold').style('letter-spacing: -0.02em;')
                        ui.label('Record international transfer (CAD)').classes('text-xs').style('color: var(--mf-muted);')
                    ui.element('div').style('flex: 1;')
                    ui.button('', icon='close', on_click=intl_dlg.close).props('flat round dense').style('opacity: 0.7;')

            # Date & Amount
            with ui.element('div').style('padding: 0 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('w-full gap-3'):
                    intl_date = ui.input(value=today().isoformat()).props("type=date outlined dense").classes("flex-1 mf-no-label")
                    intl_amount = ui.number(value=0).props("outlined dense").classes("flex-1 mf-no-label")
                ui.label('Amount in CAD').classes('text-xs').style('color: var(--mf-muted); margin-top: -4px;')

            # Withdrawal source
            ui.element('div').style('height: 1px; background: var(--mf-border); opacity: 0.4; margin: 12px 24px 0 24px;')
            with ui.element('div').style('padding: 0 24px; display: flex; flex-direction: column; align-items: stretch; width: 100%; box-sizing: border-box;'):
                with ui.row().classes('items-center gap-2 mt-3 mb-2'):
                    ui.icon('public').style('font-size: 15px; color: #f472b6; opacity: 0.7;')
                    ui.label('Transfer Details').classes('text-xs font-bold').style('text-transform: uppercase; letter-spacing: 0.08em; color: var(--mf-muted);')
                intl_source = _chip_select(
                    INTL_SOURCES, value='Bank', label='Withdrawal Source',
                    hint="Bank or CT - grey card only",
                )
                intl_notes = ui.textarea("Notes / Recipient", value="").props("outlined dense rows=2").classes("w-full")

            # Actions
            with ui.row().classes("w-full justify-end gap-3").style("padding: 14px 24px 12px 24px;"):
                ui.button("Cancel", on_click=intl_dlg.close).props("flat").style("border-radius: 10px;")
                def _save_intl():
                    dd = parse_date(intl_date.value) or today()
                    amt_val = float(to_float(intl_amount.value))
                    if amt_val <= 0:
                        ui.notify("Enter a valid amount", type="warning")
                        return
                    _notes_str = str(intl_notes.value or "").strip()
                    tx_id = sha16(f"Family|{dd.isoformat()}|International|{amt_val}|{intl_source.value}|{intl_source.value}|International Transfer|{_notes_str}|{dt.datetime.now().isoformat()}")
                    append_tx(
                        tx_id=tx_id, date_=dd, owner="Family",
                        type_="International", amount=amt_val,
                        method=str(intl_source.value),
                        account=str(intl_source.value),
                        category="International Transfer",
                        notes=_notes_str,
                        recurring_id=""
                    )
                    invalidate('transactions')
                    ui.notify(f"International transfer of {currency(amt_val)} saved", type="positive")
                    intl_dlg.close()
                ui.button("Save", on_click=_save_intl, icon="check").props("unelevated").style(
                    "background: linear-gradient(135deg, #f472b6, #f472b6cc) !important; color: #fff !important;"
                    "font-weight: 700; border-radius: 12px; padding: 8px 32px;"
                )
        ui.run_javascript('window.mfSetTheme(localStorage.getItem(\\"mf_theme\\")||\\"Midnight Blue\\");')
        intl_dlg.open()

    def content():
        # --- Hero: Scan Receipt (premium gradient card) ---
        with ui.card().classes("my-card p-0").style(
            "background: linear-gradient(135deg, rgba(99,102,241,0.16) 0%, rgba(59,130,246,0.08) 50%, rgba(16,185,129,0.06) 100%) !important;"
            "border: 1px solid rgba(99,102,241,0.20);"
        ):
            ui.element('div').style('height: 3px; background: linear-gradient(90deg, #6366f1, #3b82f6, #10b981); border-radius: 0;')
            with ui.row().classes("w-full items-center p-6 gap-5"):
                with ui.column().classes("flex-1 gap-2"):
                    ui.label("Scan a Receipt").classes("text-2xl font-extrabold").style("letter-spacing: -0.02em;")
                    ui.label("Snap a photo or upload  AI reads total, date & splits items by category.").classes("text-sm").style("color: var(--mf-muted); line-height: 1.6;")
                    ui.button("Scan Now", icon="document_scanner", on_click=lambda: open_add_dialog("Debit", auto_scan=True)).props("unelevated").classes("mt-1").style(
                        "background: linear-gradient(135deg, #6366f1, #3b82f6) !important; color: #fff !important;"
                        "font-weight: 700; letter-spacing: 0.01em; padding: 10px 32px; border-radius: 12px;"
                        "box-shadow: 0 4px 14px rgba(99,102,241,0.30);"
                    )
                with ui.element("div").style(
                    "width: 64px; height: 64px; border-radius: 18px; display: flex; align-items: center; justify-content: center;"
                    "background: rgba(99,102,241,0.12); flex-shrink: 0;"
                ):
                    ui.icon("document_scanner").style("font-size: 32px; color: rgba(99,102,241,0.65);")

        # --- Quick Add Grid (9.4 Premium) ---
        # 8.2.2: Reorganized tile grid  merged LOC, renamed Invest, added Intl Transfer
        tiles = [
            ("Expense",        "shopping_cart",   "Debit",      {},  "rgba(239,68,68,0.10)",  "#ef4444"),
            ("Income",         "trending_up",     "Credit",     {},  "rgba(34,197,94,0.10)",  "#22c55e"),
            ("Invest",         "show_chart",      "__INVEST__", {},  "rgba(168,85,247,0.10)", "#a855f7"),
            ("CC Repay",       "credit_card",     "__CC_REPAY__", {},  "rgba(251,191,36,0.10)", "#eab308"),
            ("Line of Credit", "account_balance", "__LOC__",    {},  "rgba(96,165,250,0.10)", "#60a5fa"),
            ("Intl Transfer",  "public",          "__INTL__",   {},  "rgba(244,114,182,0.10)","#f472b6"),
        ]

        def _tile_click(et, k):
            if et == '__LOC__':
                open_loc_dialog()
            elif et == '__INVEST__':
                open_invest_dialog()
            elif et == '__INTL__':
                open_intl_dialog()
            elif et == '__CC_REPAY__':
                open_cc_repay_dialog()
            else:
                open_add_dialog(et, **k)

        ui.label('Quick Add').classes('text-lg font-extrabold mt-2 mb-3 px-1').style('letter-spacing: -0.02em;')
        with ui.element("div").classes("w-full").style(
            "display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px;"
        ):
            for label, icon, etype, kw, bg, accent in tiles:
                with ui.element('div').style(
                    f'background: var(--mf-card-top); border: 1px solid var(--mf-card-border); border-radius: 18px;'
                    f'padding: 24px 16px; display: flex; flex-direction: column; align-items: center; gap: 12px;'
                    f'cursor: pointer; transition: transform 0.12s ease, box-shadow 0.12s ease; position: relative; overflow: hidden;'
                ).on("click", lambda _evt=None, et=etype, k=kw: _tile_click(et, k)):
                    # Subtle glow behind icon
                    ui.element('div').style(f'position:absolute;top:-10px;width:60px;height:60px;border-radius:50%;background:{accent};filter:blur(35px);opacity:0.15;pointer-events:none;')
                    with ui.element("div").style(
                        f"width: 48px; height: 48px; border-radius: 14px; display: flex; align-items: center; justify-content: center;"
                        f"background: {bg}; position: relative;"
                    ):
                        ui.icon(icon).style(f"font-size: 24px; color: {accent};")
                    ui.label(label).classes("text-sm font-bold text-center").style("color: var(--mf-text); letter-spacing: -0.01em;")
        # --- Recurring (9.4 compact) ---
        with ui.element('div').style(
            'display: flex; align-items: center; justify-content: space-between; padding: 16px 20px;'
            'background: var(--mf-card-top); border: 1px solid var(--mf-card-border); border-radius: 16px; margin-top: 8px;'
        ):
            with ui.row().classes('items-center gap-3'):
                with ui.element('div').style('width: 36px; height: 36px; border-radius: 10px; background: rgba(34,197,94,0.10); display: flex; align-items: center; justify-content: center;'):
                    ui.icon('autorenew').style('font-size: 18px; color: #22c55e;')
                with ui.column().classes('gap-0'):
                    ui.label('Recurring Entries').classes('text-sm font-bold').style('color: var(--mf-text);')
                    ui.label('Auto-generated on due date').classes('text-xs').style('color: var(--mf-muted);')
            ui.button('Run Now', icon='autorenew', on_click=lambda: ui.notify(f"Created {generate_recurring_for_date(today())} entries", type="positive")).props('outline dense').style('border-radius: 10px; font-size: 12px;')

    # 8.8: Auto-open dialog when arriving from Home page quick-add buttons
    _auto_mode = str(app.storage.user.pop('add_auto_open', '') or '').strip().lower()

    def _auto_open():
        if _auto_mode == 'expense':
            open_add_dialog('Debit')
        elif _auto_mode == 'income':
            open_add_dialog('Credit')

    shell(content)

    if _auto_mode:
        ui.timer(0.3, _auto_open, once=True)


@ui.page("/admin")
def admin_page() -> None:
    if not require_login():
        nav_to("/login")
        return

    def content() -> None:
        with ui.card().classes("my-card p-0 mb-4").style("overflow: visible; background: linear-gradient(135deg, rgba(99,102,241,0.05), transparent); border: 1px solid rgba(99,102,241,0.1);"):
            # Glowing orb background
            ui.html('<div style="position:absolute;top:-50px;right:-20px;width:150px;height:150px;background:radial-gradient(circle, rgba(139,92,246,0.15) 0%, transparent 70%);border-radius:50%;pointer-events:none;"></div>')
            ui.element('div').style('height: 4px; background: linear-gradient(90deg, #6366f1, #8b5cf6, #d946ef); border-radius: 16px 16px 0 0;')
            with ui.column().classes("p-6 gap-4"):
                with ui.row().classes("items-center gap-4"):
                    with ui.element("div").style("width: 48px; height: 48px; border-radius: 16px; display: flex; align-items: center; justify-content: center; background: linear-gradient(135deg, #6366f1, #8b5cf6); box-shadow: 0 8px 24px rgba(99,102,241,0.3);"):
                        ui.icon("admin_panel_settings").style("font-size: 24px; color: white;")
                    with ui.column().classes("gap-0"):
                        ui.label("Control Center").classes("text-2xl font-black").style("letter-spacing: -0.03em; background: linear-gradient(to right, #ffffff, #a5b4fc); -webkit-background-clip: text; -webkit-text-fill-color: transparent;")
                        ui.label("System configurations & data management").classes("text-xs font-semibold uppercase tracking-wider").style("color: var(--mf-muted)")

        with ui.element("div").style("display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 16px; width: 100%;"):
            _admin_links = [
                ("RulesEngine", "rule", "/rules", "#22c55e", "linear-gradient(135deg, rgba(34,197,94,0.1), transparent)"),
                ("Cards Vault", "credit_card", "/cards", "#3b82f6", "linear-gradient(135deg, rgba(59,130,246,0.1), transparent)"),
                ("Recurring", "autorenew", "/recurring", "#a855f7", "linear-gradient(135deg, rgba(168,85,247,0.1), transparent)"),
                ("Ledger", "receipt_long", "/tx", "#ef4444", "linear-gradient(135deg, rgba(239,68,68,0.1), transparent)"),
                ("Budget Matrix", "account_balance_wallet", "/budgets", "#eab308", "linear-gradient(135deg, rgba(251,191,36,0.1), transparent)"),
                ("Data Upload", "cloud_upload", "/data_upload", "#06b6d4", "linear-gradient(135deg, rgba(6,182,212,0.1), transparent)"),
                ("Reports Hub", "assessment", "/reports", "#f43f5e", "linear-gradient(135deg, rgba(244,63,94,0.1), transparent)"),
                ("Color Matrix", "palette", "/color_matrix", "#ec4899", "linear-gradient(135deg, rgba(236,72,153,0.1), transparent)"),
            ]
            for label, icon, href, accent_color, bg_gradient in _admin_links:
                with ui.element("div").style(
                    f"cursor: pointer; border: 1px solid rgba(255,255,255,0.05); border-radius: 20px;"
                    f"background: var(--mf-surface-2); position: relative; overflow: hidden;"
                    f"transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); box-shadow: 0 4px 12px rgba(0,0,0,0.1);"
                ).classes("mf-admin-tile").on("click", lambda _evt=None, h=href: nav_to(h)):
                    ui.html(f'<div style="position:absolute;top:0;left:0;right:0;bottom:0;background:{bg_gradient};opacity:0.5;pointer-events:none;"></div>')
                    with ui.column().classes("items-center justify-center p-6 gap-3").style("min-height: 140px; position: relative; z-index: 10;"):
                        with ui.element("div").style(f"width: 44px; height: 44px; border-radius: 50%; background: {accent_color}22; display: flex; align-items: center; justify-content: center; box-shadow: 0 0 16px {accent_color}20;"):
                            ui.icon(icon).style(f"font-size: 22px; color: {accent_color};")
                        ui.label(label).classes("text-sm font-bold text-center").style("line-height: 1.2;")

        with ui.card().classes("my-card p-5 mt-4 items-center").style("background: rgba(255,255,255,0.02); border: 1px dashed rgba(255,255,255,0.1); border-radius: 16px;"):
            with ui.row().classes("items-center gap-3 w-full justify-between"):
                with ui.row().classes("items-center gap-3"):
                    ui.icon("lock").style("color: var(--mf-muted); font-size: 20px; opacity: 0.7;")
                    with ui.column().classes("gap-0"):
                        ui.label("Month Locks").classes("text-sm font-bold")
                        ui.label("Toggle locks directly from the Ledger page.").classes("text-[11px] uppercase tracking-wider").style("color: var(--mf-muted);")
                ui.button("Go to Ledger", on_click=lambda: nav_to("/tx")).props("outline rounded size=sm").style("color: var(--mf-text); border-color: rgba(255,255,255,0.1);")

    shell(content)


# 9.8.3: Color Matrix — dedicated page (was inline card in admin before)
@ui.page("/color_matrix")
def color_matrix_page() -> None:
    if not require_login():
        nav_to("/login")
        return

    def content() -> None:
        _default_ring = ['#8B5CF6', '#3B82F6', '#F59E0B', '#10B981', '#EC4899', '#EF4444']
        _saved_ring = app.storage.user.get('budget_ring_colors', None)
        _current_ring = _saved_ring if (isinstance(_saved_ring, list) and len(_saved_ring) >= 3) else list(_default_ring)
        _current_sb_color = app.storage.user.get('spending_breakdown_color', '#3B82F6')

        # Color template presets for budget rings
        _color_presets = {
            'Vivid (default)': ['#8B5CF6', '#3B82F6', '#F59E0B', '#10B981', '#EC4899', '#EF4444'],
            'Ocean': ['#06b6d4', '#0ea5e9', '#38bdf8', '#22d3ee', '#67e8f9', '#a5f3fc'],
            'Sunset': ['#f97316', '#ef4444', '#ec4899', '#f59e0b', '#e11d48', '#fb923c'],
            'Forest': ['#22c55e', '#16a34a', '#84cc16', '#10b981', '#059669', '#4ade80'],
            'Neon': ['#e879f9', '#c084fc', '#818cf8', '#22d3ee', '#34d399', '#fbbf24'],
            'Royal': ['#7c3aed', '#6366f1', '#8b5cf6', '#a78bfa', '#c4b5fd', '#ddd6fe'],
        }

        # Accent color presets for spending breakdown
        _sb_accent_presets = {
            'Blue (default)': '#3B82F6',
            'Emerald': '#10B981',
            'Rose': '#F43F5E',
            'Amber': '#F59E0B',
            'Cyan': '#06B6D4',
            'Violet': '#8B5CF6',
            'Orange': '#F97316',
            'Teal': '#14B8A6',
        }

        # Page header
        with ui.card().classes("my-card p-0 mb-4").style("overflow: visible; background: linear-gradient(135deg, rgba(236,72,153,0.05), transparent); border: 1px solid rgba(236,72,153,0.1);"):
            ui.element('div').style('height: 4px; background: linear-gradient(90deg, #ec4899, #8b5cf6, #3b82f6); border-radius: 16px 16px 0 0;')
            with ui.column().classes("p-6 gap-2"):
                with ui.row().classes("items-center gap-4"):
                    with ui.element("div").style("width: 48px; height: 48px; border-radius: 16px; display: flex; align-items: center; justify-content: center; background: linear-gradient(135deg, #ec4899, #8b5cf6); box-shadow: 0 8px 24px rgba(236,72,153,0.3);"):
                        ui.icon("palette").style("font-size: 24px; color: white;")
                    with ui.column().classes("gap-0"):
                        ui.label("Color Matrix").classes("text-2xl font-black").style("letter-spacing: -0.03em; background: linear-gradient(to right, #ffffff, #f9a8d4); -webkit-background-clip: text; -webkit-text-fill-color: transparent;")
                        ui.label("Customize widget colors for your homepage").classes("text-xs font-semibold uppercase tracking-wider").style("color: var(--mf-muted)")

        # ── Section 1: Budget Ring Colors ──
        with ui.card().classes("my-card p-0").style("overflow: hidden;"):
            ui.element('div').style('height: 3px; background: linear-gradient(90deg, #8B5CF6, #3B82F6, #F59E0B); border-radius: 0;')
            with ui.column().classes("p-5 gap-4"):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('donut_large').style('font-size: 18px; color: #8B5CF6;')
                    ui.label('Budget Ring Colors').classes('text-base font-extrabold').style('letter-spacing: -0.02em;')

                # Live preview
                _preview_holder = ui.element('div').style('display: flex; align-items: center; gap: 16px; flex-wrap: wrap;')

                def _render_ring_preview(colors):
                    _preview_holder.clear()
                    with _preview_holder:
                        _pv_size = 90
                        _pv_cx, _pv_cy = _pv_size / 2, _pv_size / 2
                        _pv_sw = 7
                        _pv_gap = 2
                        _pv_r0 = (_pv_size / 2) - 6
                        _pv_parts = [f'<svg viewBox="0 0 {_pv_size} {_pv_size}" style="width: 90px; height: 90px;">']
                        for _pi in range(min(3, len(colors))):
                            _pr = _pv_r0 - _pi * (_pv_sw + _pv_gap)
                            if _pr < 8:
                                break
                            _pc = 2 * 3.14159265 * _pr
                            _pd = (0.7 - _pi * 0.15) * _pc
                            _pv_parts.append(f'<circle cx="{_pv_cx}" cy="{_pv_cy}" r="{_pr}" fill="none" stroke="var(--mf-border)" stroke-width="{_pv_sw}" opacity="0.25"/>')
                            _pv_parts.append(f'<circle cx="{_pv_cx}" cy="{_pv_cy}" r="{_pr}" fill="none" stroke="{colors[_pi]}" stroke-width="{_pv_sw}" stroke-dasharray="{_pd} {_pc}" stroke-linecap="round" transform="rotate(-90 {_pv_cx} {_pv_cy})"/>')
                        _pv_parts.append('</svg>')
                        ui.html('\n'.join(_pv_parts))
                        with ui.column().classes('gap-1'):
                            for _ci in range(min(3, len(colors))):
                                with ui.row().classes('items-center gap-2'):
                                    ui.element('div').style(f'width: 12px; height: 12px; border-radius: 50%; background: {colors[_ci]};')
                                    ui.label(f'Ring {_ci + 1}').classes('text-xs font-medium').style('color: var(--mf-muted);')

                _render_ring_preview(_current_ring)

                # Preset selector
                ui.label("Presets").classes("text-[10px] font-semibold").style("color: var(--mf-muted); text-transform: uppercase; letter-spacing: 0.06em;")
                with ui.element('div').style('display: flex; flex-wrap: wrap; gap: 8px;'):
                    for _pname, _pcolors in _color_presets.items():
                        def _apply_preset(_pc=_pcolors, _pn=_pname):
                            app.storage.user['budget_ring_colors'] = list(_pc)
                            _render_ring_preview(_pc)
                            ui.notify(f'Applied "{_pn}" palette.', type='positive', timeout=1500)
                        with ui.element('div').style(
                            'cursor: pointer; padding: 8px 14px; border-radius: 12px; border: 1px solid var(--mf-border);'
                            'background: var(--mf-surface); display: flex; align-items: center; gap: 8px;'
                            'transition: all 0.15s ease;'
                        ).on('click', _apply_preset):
                            for _sc in _pcolors[:3]:
                                ui.element('div').style(f'width: 14px; height: 14px; border-radius: 50%; background: {_sc};')
                            ui.label(_pname).classes('text-xs font-semibold').style('color: var(--mf-text);')

                # Custom color inputs
                with ui.expansion('Custom Ring Colors', icon='tune').classes('w-full').style('margin-top: 4px;'):
                    _color_inputs = {}
                    for _cci in range(min(6, len(_current_ring))):
                        with ui.row().classes('items-center gap-3'):
                            ui.label(f'Color {_cci + 1}').classes('text-xs font-medium w-16').style('color: var(--mf-muted);')
                            _ci_input = ui.color_input(label='', value=_current_ring[_cci]).props('dense').classes('w-32')
                            _color_inputs[_cci] = _ci_input

                    def _save_custom():
                        _new_colors = []
                        for _k in range(len(_color_inputs)):
                            _v = str(_color_inputs[_k].value or '#8B5CF6').strip()
                            if not _v.startswith('#'):
                                _v = '#' + _v
                            _new_colors.append(_v)
                        app.storage.user['budget_ring_colors'] = _new_colors
                        _render_ring_preview(_new_colors)
                        ui.notify('Custom colors saved.', type='positive', timeout=1500)

                    ui.button('Save Custom Colors', icon='save', on_click=_save_custom).props('unelevated size=sm').classes('mt-2').style(
                        'background: linear-gradient(135deg, #8B5CF6, #6366f1) !important; color: #fff !important; border-radius: 10px; font-weight: 600;'
                    )

        # ── Section 2: Spending Breakdown Accent Color ──
        with ui.card().classes("my-card p-0 mt-4").style("overflow: hidden;"):
            ui.element('div').style(f'height: 3px; background: linear-gradient(90deg, {_current_sb_color}, #6366f1); border-radius: 0;')
            with ui.column().classes("p-5 gap-4"):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('insights').style('font-size: 18px; color: #3B82F6;')
                    ui.label('Spending Breakdown Accent').classes('text-base font-extrabold').style('letter-spacing: -0.02em;')

                # Live preview sparkline
                _sb_preview_holder = ui.element('div').style('display: flex; align-items: center; gap: 16px; flex-wrap: wrap;')

                def _render_sb_preview(color):
                    _sb_preview_holder.clear()
                    with _sb_preview_holder:
                        _pv_svg = f'''<svg viewBox="0 0 120 36" style="width: 120px; height: 36px;">
                            <defs><linearGradient id="sbPv" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="{color}" stop-opacity="0.2"/><stop offset="100%" stop-color="{color}" stop-opacity="0.02"/></linearGradient></defs>
                            <polygon points="8,28 28,18 48,24 68,12 88,20 108,8 108,28 8,28" fill="url(#sbPv)"/>
                            <polyline points="8,28 28,18 48,24 68,12 88,20 108,8" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
                            <circle cx="108" cy="8" r="3" fill="{color}"/>
                        </svg>'''
                        ui.html(_pv_svg)
                        with ui.column().classes('gap-0'):
                            ui.label('Preview').classes('text-[10px] font-semibold').style('color: var(--mf-muted); text-transform: uppercase; letter-spacing: 0.06em;')
                            ui.label('$1,234').classes('text-lg font-extrabold').style(f'color: {color}; font-feature-settings: "tnum"; letter-spacing: -0.02em;')

                _render_sb_preview(_current_sb_color)

                # Accent presets as colored pills
                with ui.element('div').style('display: flex; flex-wrap: wrap; gap: 8px;'):
                    for _sb_pname, _sb_pcolor in _sb_accent_presets.items():
                        def _apply_sb_preset(_c=_sb_pcolor, _n=_sb_pname):
                            app.storage.user['spending_breakdown_color'] = _c
                            _render_sb_preview(_c)
                            ui.notify(f'Applied "{_n}" accent.', type='positive', timeout=1500)
                        _is_active = _current_sb_color.lower() == _sb_pcolor.lower()
                        with ui.element('div').style(
                            f'cursor: pointer; padding: 6px 14px; border-radius: 12px;'
                            f'border: {"2px" if _is_active else "1px"} solid {_sb_pcolor if _is_active else "var(--mf-border)"};'
                            f'background: {_sb_pcolor}{"1A" if _is_active else "0D"}; display: flex; align-items: center; gap: 8px;'
                            f'transition: all 0.15s ease;'
                        ).on('click', _apply_sb_preset):
                            ui.element('div').style(f'width: 14px; height: 14px; border-radius: 50%; background: {_sb_pcolor};')
                            ui.label(_sb_pname).classes('text-xs font-semibold').style('color: var(--mf-text);')

                # Custom accent color input
                with ui.expansion('Custom Accent Color', icon='tune').classes('w-full').style('margin-top: 4px;'):
                    _sb_custom_input = ui.color_input(label='Accent Hex', value=_current_sb_color).props('dense').classes('w-40')

                    def _save_sb_custom():
                        _v = str(_sb_custom_input.value or '#3B82F6').strip()
                        if not _v.startswith('#'):
                            _v = '#' + _v
                        app.storage.user['spending_breakdown_color'] = _v
                        _render_sb_preview(_v)
                        ui.notify('Accent color saved.', type='positive', timeout=1500)

                    ui.button('Save Accent Color', icon='save', on_click=_save_sb_custom).props('unelevated size=sm').classes('mt-2').style(
                        'background: linear-gradient(135deg, #3B82F6, #1d4ed8) !important; color: #fff !important; border-radius: 10px; font-weight: 600;'
                    )

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

        # Premium header
        with ui.element("div").style("margin-bottom: -8px;"):
            with ui.card().classes("my-card p-0").style("overflow: hidden; border-bottom-left-radius: 0; border-bottom-right-radius: 0;"):
                ui.element('div').classes('mf-accent-strip')
                with ui.row().classes("items-center gap-3 p-5"):
                    with ui.element("div").classes("mf-icon-box").style("background: rgba(99,102,241,0.12);"):
                        ui.icon("receipt_long").style("font-size: 20px; color: #6366f1;")
                    ui.label("Transactions").classes("text-xl font-extrabold").style("letter-spacing: -0.02em;")

        with ui.card().classes("my-card p-5"):
            _all_cats = sorted({str(c).strip() for c in tx.get('category', pd.Series([])).tolist() if str(c).strip()})
            _all_accts = sorted({str(a).strip() for a in tx.get('account', pd.Series([])).tolist() if str(a).strip()})
            _all_methods = sorted({str(m).strip() for m in tx.get('method', pd.Series([])).tolist() if str(m).strip()})
            sort_opts = ["Date (new  old)", "Date (old  new)", "Amount (high  low)", "Amount (low  high)"]

            ui.label("Search & Filters").classes("text-sm font-bold uppercase tracking-wider mb-3").style("color: var(--mf-muted);")
            with ui.element('div').style('display: grid; gap: 12px; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); width: 100%; margin-bottom: 8px;'):
                f_text = ui.input("Search text").props("outlined dense clearable").classes("w-full")
                f_category = ui.select(['All'] + _all_cats, value='All', label='Category').props('outlined dense options-dense use-input').classes('w-full')
                f_type = ui.select(["All"] + types, value="All", label="Type").props('outlined dense options-dense').classes("w-full")
                f_account = ui.select(['All'] + _all_accts, value='All', label='Account').props('outlined dense options-dense').classes('w-full')
                f_method = ui.select(['All'] + _all_methods, value='All', label='Method').props('outlined dense options-dense').classes('w-full')
                f_sort = ui.select(sort_opts, value=sort_opts[0], label="Sort").props('outlined dense options-dense').classes("w-full")
                with ui.row().classes('items-center gap-2 w-full no-wrap'):
                    f_min_amt = ui.number('Min $', value=None, format='%.2f').props('dense outlined clearable').classes('flex-1')
                    f_max_amt = ui.number('Max $', value=None, format='%.2f').props('dense outlined clearable').classes('flex-1')
                with ui.row().classes('items-center gap-2 w-full no-wrap'):
                    f_from = ui.input('From').props('type=date dense outlined clearable').classes('flex-1')
                    f_to = ui.input('To').props('type=date dense outlined clearable').classes('flex-1')

            try:
                q_prefill = (app.storage.user.get('tx_search_prefill') or '').strip()
                if q_prefill:
                    f_text.value = q_prefill
                    app.storage.user.pop('tx_search_prefill', None)
            except Exception:
                pass

            try:
                if app.storage.user.get('tx_quick_filter') == 'uncat':
                    f_text.value = 'Uncategorized'
                    app.storage.user.pop('tx_quick_filter', None)
            except Exception:
                pass

            # Quick filter presets
            _filter_state = {'min_amt': 0}
            _presets = [
                ('All', {}),
                ('Uncategorized', {'text': 'Uncategorized'}),
                ('Large (>$100)', {'text': '', '_min_amt': 100}),
                ('Groceries', {'cat': 'Groceries'}),
                ('Health', {'cat': 'Health'}),
            ]
            with ui.row().classes('w-full gap-1 mt-1 mb-2').style('flex-wrap: wrap;'):
                for _preset_name, _preset_vals in _presets:
                    def _apply_preset(pv=_preset_vals, pn=_preset_name):
                        if pv.get('text') is not None:
                            f_text.value = pv['text']
                        if pv.get('cat'):
                            f_category.value = pv['cat']
                        else:
                            f_category.value = 'All'
                        if pv.get('_min_amt'):
                            _filter_state['min_amt'] = pv['_min_amt']
                        else:
                            _filter_state['min_amt'] = 0
                        if not pv:
                            f_text.value = ''
                            f_type.value = 'All'
                            f_category.value = 'All'
                            f_account.value = 'All'
                            f_method.value = 'All'
                            f_min_amt.value = None
                            f_max_amt.value = None
                            _filter_state['min_amt'] = 0
                        refresh_table()
                    ui.button(_preset_name, on_click=_apply_preset).props('flat dense').style(
                        'border-radius: 8px; font-size: 11px; padding: 4px 12px; border: 1px solid var(--mf-border);'
                    )

            #  Saved Filter Presets 
            _PRESETS_KEY = 'tx_saved_presets'

            def _get_saved_presets() -> list:
                try:
                    return app.storage.user.get(_PRESETS_KEY) or []
                except Exception:
                    return []

            def _save_current_as_preset():
                with ui.dialog() as sdlg, ui.card().classes('my-card p-5 w-80'):
                    ui.label('Save Filter Preset').classes('text-lg font-bold')
                    pname = ui.input('Preset name', placeholder='e.g. Grocery expenses').classes('w-full')
                    def _do_save():
                        name = (pname.value or '').strip()
                        if not name:
                            ui.notify('Enter a name', type='warning')
                            return
                        preset = {
                            'name': name,
                            'type': f_type.value or 'All',
                            'category': f_category.value or 'All',
                            'account': f_account.value or 'All',
                            'method': f_method.value or 'All',
                            'text': f_text.value or '',
                            'min_amt': f_min_amt.value,
                            'max_amt': f_max_amt.value,
                        }
                        presets = _get_saved_presets()
                        # Replace if same name exists
                        presets = [p for p in presets if p.get('name') != name]
                        presets.append(preset)
                        try:
                            app.storage.user[_PRESETS_KEY] = presets
                        except Exception as e:
                            _logger.warning("Failed to save preset: %s", e)
                        ui.notify(f'Saved preset "{name}"', type='positive')
                        sdlg.close()
                        nav_to('/tx')
                    with ui.row().classes('w-full justify-end gap-2 mt-3'):
                        ui.button('Cancel', on_click=sdlg.close).props('flat')
                        ui.button('Save', on_click=_do_save).props('unelevated')
                sdlg.open()

            def _delete_saved_preset(name: str):
                presets = _get_saved_presets()
                presets = [p for p in presets if p.get('name') != name]
                try:
                    app.storage.user[_PRESETS_KEY] = presets
                except Exception as e:
                    _logger.warning("Failed to delete preset: %s", e)
                ui.notify(f'Deleted preset "{name}"', type='info')
                nav_to('/tx')

            saved_presets = _get_saved_presets()
            if saved_presets:
                with ui.row().classes('w-full gap-1 mb-2').style('flex-wrap: wrap;'):
                    for sp in saved_presets:
                        _sp_name = sp.get('name', '?')
                        def _apply_saved(s=sp):
                            f_type.value = s.get('type', 'All')
                            f_category.value = s.get('category', 'All')
                            f_account.value = s.get('account', 'All')
                            f_method.value = s.get('method', 'All')
                            f_text.value = s.get('text', '')
                            f_min_amt.value = s.get('min_amt')
                            f_max_amt.value = s.get('max_amt')
                            _filter_state['min_amt'] = 0
                            refresh_table()
                        with ui.element('div').style('display: inline-flex; align-items: center; gap: 2px;'):
                            ui.button(_sp_name, icon='bookmark', on_click=_apply_saved).props('flat dense').style(
                                'border-radius: 8px; font-size: 11px; padding: 4px 10px; border: 1px solid var(--mf-accent); color: var(--mf-accent);'
                            )
                            ui.button('', icon='close', on_click=lambda n=_sp_name: _delete_saved_preset(n)).props('flat round dense size=xs').style(
                                'font-size: 10px; color: var(--mf-muted); min-width: 20px; padding: 0;'
                            )

            ui.button('Save current filters', icon='bookmark_add', on_click=_save_current_as_preset).props('flat dense').style(
                'border-radius: 8px; font-size: 11px; padding: 4px 12px; color: var(--mf-muted);'
            )

            # Date range filter (defaults to current month start to today)
            try:
                _today = datetime.date.today()
                _from = _today.replace(day=1).isoformat()
                _to = _today.isoformat()
            except Exception:
                _from = ''
                _to = ''
            # f_from and f_to already created in grid above
            f_from.value = _from
            f_to.value = _to

            # Phase 4: export + quick fix tools (wired after the table is created)
            last_view: Dict[str, Any] = {'df': None}

            # Table: show compact columns by default (mobile-friendly). Use Details to view/edit full row.
            _page_size = 30
            _page_state = {'current': 0}

            with ui.element('div').classes('w-full overflow-x-auto'):
                table = ui.table(columns=[
                    {"name": "date", "label": "Date", "field": "date"},
                    {"name": "type", "label": "Type", "field": "type"},
                    {"name": "amount", "label": "Amount", "field": "amount", "align": "right"},
                    {"name": "category", "label": "Category", "field": "category"},
                ], rows=[], row_key="id", selection='single').classes("w-full mf-tx-table")
                table.props('flat bordered')

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

            with ui.row().classes('w-full items-center justify-between mt-2'):
                _page_info = ui.label('').classes('text-xs').style('color: var(--mf-muted);')
                with ui.row().classes('gap-1'):
                    def _prev_page():
                        if _page_state['current'] > 0:
                            _page_state['current'] -= 1
                            refresh_table()
                    def _next_page():
                        _page_state['current'] += 1
                        refresh_table()
                    ui.button('', icon='chevron_left', on_click=_prev_page).props('flat round dense')
                    ui.button('', icon='chevron_right', on_click=_next_page).props('flat round dense')

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

            # B7: mobile-friendly stacked toolbar with wrapping
            with ui.element('div').style('display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-top: 10px;'):
                ui.button('Export CSV', icon='download').props('outline dense').style('font-size: 12px;').on('click', lambda: _export_csv(last_view))
                ui.button('Duplicates', icon='difference').props('flat dense').style('font-size: 12px;').on('click', lambda: _show_duplicates(last_view))
                with ui.row().classes('gap-2 items-center').style('flex-wrap: wrap;'):
                    fix_cat = ui.select(cat_choices, value=cat_choices[0], label='Quick category').classes('').style('min-width: 160px; max-width: 220px;').props('dense outlined')
                    ui.button('Apply', icon='label').props('unelevated dense').style('font-size: 12px;').on('click', lambda: _apply_category_selected(table, fix_cat.value))
            def refresh_table():
                df = tx.copy()
                if f_type.value != "All":
                    df = df[df["type"].astype(str) == f_type.value]
                if f_category.value and f_category.value != 'All':
                    df = df[df.get('category', pd.Series(dtype=str)).astype(str).str.strip() == f_category.value]
                if f_account.value and f_account.value != 'All':
                    df = df[df.get('account', pd.Series(dtype=str)).astype(str).str.strip() == f_account.value]
                if f_method.value and f_method.value != 'All':
                    df = df[df.get('method', pd.Series(dtype=str)).astype(str).str.strip() == f_method.value]
                # Amount range filter
                try:
                    _amt_min = float(f_min_amt.value) if f_min_amt.value is not None else None
                except (TypeError, ValueError):
                    _amt_min = None
                try:
                    _amt_max = float(f_max_amt.value) if f_max_amt.value is not None else None
                except (TypeError, ValueError):
                    _amt_max = None
                if _amt_min is not None or _amt_max is not None:
                    df['_amt_rng'] = df['amount'].apply(to_float)
                    if _amt_min is not None:
                        df = df[df['_amt_rng'] >= _amt_min]
                    if _amt_max is not None:
                        df = df[df['_amt_rng'] <= _amt_max]
                    df = df.drop(columns=['_amt_rng'], errors='ignore')
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
                if _filter_state.get('min_amt', 0) > 0:
                    _min = float(_filter_state['min_amt'])
                    df['_amt_f'] = df['amount'].apply(to_float)
                    df = df[df['_amt_f'] >= _min]
                    df = df.drop(columns=['_amt_f'], errors='ignore')
                # Sorting
                try:
                    sort_choice = f_sort.value or "Date (new  old)"
                except Exception:
                    sort_choice = "Date (new  old)"

                if "Amount" in sort_choice:
                    df["__amt"] = df["amount"].apply(to_float)
                    ascending = "low  high" in sort_choice
                    df = df.sort_values(by="__amt", ascending=ascending)
                    df = df.drop(columns=["__amt"], errors="ignore")
                else:
                    # Date sorting uses parsed date
                    if "date_parsed" not in df.columns:
                        df["date_parsed"] = df["date"].apply(parse_date)
                    ascending = "old  new" in sort_choice
                    df = df.sort_values(by="date_parsed", ascending=ascending)

                # keep a copy of the current filtered/sorted view for export & diagnostics
                try:
                    last_view['df'] = df.copy()
                except Exception:
                    last_view['df'] = None

                _total_rows = len(df)
                _start = _page_state['current'] * _page_size
                df_page = df.iloc[_start:_start + _page_size].copy()
                df_page["amount"] = df_page["amount"].apply(lambda x: currency(to_float(x)))
                table.rows = df_page.to_dict(orient="records")
                table.update()
                # Update pagination info
                try:
                    _page_info.set_text(f"Showing {_start+1}\u2013{min(_start+_page_size, _total_rows)} of {_total_rows}")
                except Exception:
                    pass

            def _reset_page_and_refresh():
                _page_state['current'] = 0
                refresh_table()
            f_type.on("update:model-value", lambda e: _reset_page_and_refresh())
            f_text.on("update:model-value", lambda e: _reset_page_and_refresh())
            f_category.on("update:model-value", lambda e: _reset_page_and_refresh())
            f_account.on("update:model-value", lambda e: _reset_page_and_refresh())
            f_method.on("update:model-value", lambda e: _reset_page_and_refresh())
            f_min_amt.on("update:model-value", lambda e: _reset_page_and_refresh())
            f_max_amt.on("update:model-value", lambda e: _reset_page_and_refresh())
            f_sort.on("update:model-value", lambda e: _reset_page_and_refresh())
            f_from.on("update:model-value", lambda e: _reset_page_and_refresh())
            f_to.on("update:model-value", lambda e: _reset_page_and_refresh())

            refresh_table()

            # Edit/Delete
            def open_edit(row: Dict[str, Any]):
                dlg = ui.dialog()
                with dlg, ui.card().classes("my-card p-5 w-[720px] max-w-[95vw]"):
                    ui.label("Edit transaction").classes("text-lg font-bold")
                    tid = str(row.get("id", "")).strip()

                    e_date = ui.input("Date", value=str(row.get("date", ""))).props("type=date autofocus").classes("w-full")
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
                rec_id = str(row.get("recurring_id", "")).strip()
                with ui.dialog() as confirm_dlg, ui.card().classes("my-card p-5 max-w-sm"):
                    ui.label("Delete Transaction?").classes("text-lg font-bold")
                    if rec_id:
                        with ui.row().classes("items-center gap-2 mt-2"):
                            ui.icon("warning").style("color: #f59e0b; font-size: 20px;")
                            ui.label("This transaction is linked to a recurring template.").classes("text-sm").style("color: #f59e0b;")
                    ui.label(f"Date: {row.get('date','')}  |  Amount: {row.get('amount','')}").classes("text-sm mt-2").style("color: var(--mf-muted);")
                    def _confirm():
                        if delete_row_by_id("transactions", "id", tid):
                            invalidate("transactions")
                            ui.notify("Deleted", type="positive")
                            confirm_dlg.close()
                            nav_to("/tx")
                        else:
                            ui.notify("Delete failed", type="negative")
                    with ui.row().classes("w-full justify-end gap-2 mt-4"):
                        ui.button("Cancel", on_click=confirm_dlg.close).props("flat")
                        ui.button("Delete", on_click=_confirm).props("unelevated").style("background: #ef4444 !important; color: #fff !important;")
                confirm_dlg.open()

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
        # Premium security header
        with ui.element("div").style("margin-bottom: -8px;"):
            with ui.card().classes("my-card p-0").style("overflow: hidden;"):
                ui.element('div').style('height: 3px; background: linear-gradient(90deg, #22c55e, #10b981); border-radius: 0;')
                with ui.column().classes("p-5 gap-3"):
                    with ui.row().classes("items-center gap-3"):
                        with ui.element("div").classes("mf-icon-box").style("background: rgba(34,197,94,0.12);"):
                            ui.icon("fingerprint").style("font-size: 22px; color: #22c55e;")
                        ui.label("Passkeys / Face ID").classes("text-xl font-extrabold").style("letter-spacing: -0.02em;")
                    ui.label("Register a passkey for quick biometric login (iPhone Face ID, Touch ID, etc.).").classes("text-sm").style("color: var(--mf-muted)")

        with ui.card().classes("my-card p-5"):
            default_user = os.environ.get('APP_USER') or os.environ.get('APP_USERNAME') or 'admin'
            u_in = ui.input("Username for passkey", value=default_user).classes("w-full").props("id=pk_user")

            def do_register():
                username = (u_in.value or "").strip()
                if not username:
                    ui.notify("Username required", type="warning")
                    return
                ui.notify("Opening Face ID / Passkey prompt", type="info", timeout=1.5)
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
                    const bufToB64url = (b) => btoa(String.fromCharCode(...new Uint8Array(b))).replace(/\\+/g,'-').replace(/\\//g,'_').replace(/=+$/g,'');
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
                    document.getElementById('pk_status')?.replaceChildren('Passkey registered ');
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
                        ui.label(f"Credential ID: {str(data.get('credential_id',''))[:18]}").classes("text-xs").style("color: var(--mf-muted)")
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
        # Premium cards header
        with ui.card().classes('my-card p-0 mb-4').style('overflow: hidden;'):
            ui.element('div').style('height: 3px; background: linear-gradient(90deg, #3b82f6, #6366f1, #a855f7); border-radius: 0;')
            with ui.row().classes('items-center gap-3 p-5'):
                with ui.element("div").classes("mf-icon-box").style("background: rgba(59,130,246,0.12);"):
                    ui.icon("credit_card").style("font-size: 22px; color: #3b82f6;")
                with ui.column().classes('gap-0'):
                    ui.label('Cards').classes('text-xl font-extrabold').style('letter-spacing: -0.02em;')
                    ui.label('Credit limits, utilization & billing cycles').classes('text-xs').style('color: var(--mf-muted);')

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
        emojis = pick(['emoji', 'Emoji'], default='')
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

                # 8.5: Include LOC Draw as utilization for LOC cards, and LOC Repay as repayment
                _acct_match = scope.get('account','').astype(str).str.strip() == card_key
                spend_mask = (scope['type_norm'].isin(['debit','expense','spend','loc draw','loc_draw','loc withdrawal','loc_withdrawal'])) & _acct_match
                util_used = float(scope.loc[spend_mask, 'amount_num'].sum())

                repay_mask = (scope['type_norm'].isin(['credit card repay','cc repay','credit card repayment','cc repayment','loc repay','loc_repay','loc repayment','loc_repayment'])) & _acct_match
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
            return ('canadiantire' in n) or ('canadian tire' in n) or ('ct mastercard' in n)

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
            
            # Master Gradients for Premium Graphical Cards
            if _is_ct(c):
                if 'black' in nlow:
                    grad = 'linear-gradient(135deg, #1f2937, #030712)' # Deep Black
                    text_color = '#ffffff'
                    accent = '#ef4444' # CT Red
                else: # CT Triangle or Grey
                    grad = 'linear-gradient(135deg, #9ca3af, #4b5563)' # Metallic Grey
                    text_color = '#ffffff'
                    accent = '#ef4444'
            elif _is_rbc(c):
                grad = 'linear-gradient(135deg, #003168, #005baa)' # RBC Royal Blue
                text_color = '#ffffff'
                accent = '#fbbf24' # RBC Gold
            elif _is_loc(c):
                grad = 'linear-gradient(135deg, #1e3a8a, #1e40af)' # Navy blue
                text_color = '#ffffff'
                accent = '#38bdf8' # Light blue accent
            else:
                grad = 'linear-gradient(135deg, #4f46e5, #312e81)' # Default Indigo
                text_color = '#ffffff'
                accent = '#c7d2fe'

            # Utilization color
            pct_val = float(c.get('pct', 0.0))
            util_grad = 'linear-gradient(90deg, #10b981, #059669)' if pct_val < 0.50 else ('linear-gradient(90deg, #f59e0b, #d97706)' if pct_val < 0.80 else 'linear-gradient(90deg, #ef4444, #b91c1c)')
            pct_display = f"{int(round(pct_val * 100))}%"

            with ui.element('div').classes(col).style('padding: 8px;'):
                # The Physical Card Visual — 9.8.1: realistic card proportions + rounded
                with ui.element('div').style(
                    f'background: {grad};'
                    'border-radius: 24px;'
                    'padding: 24px;'
                    'position: relative;'
                    'overflow: hidden;'
                    'box-shadow: 0 20px 40px rgba(0,0,0,0.3), inset 0 2px 4px rgba(255,255,255,0.2);'
                    'min-height: 200px; max-width: 420px; aspect-ratio: 1.586 / 1;'
                    'display: flex;'
                    'flex-direction: column;'
                    'justify-content: space-between;'
                    f'color: {text_color};'
                ):
                    # Glassmorphic overlay shapes for shine
                    ui.html('''
                        <div style="position: absolute; top: -50%; left: -50%; width: 200%; height: 200%; background: linear-gradient(to bottom right, rgba(255,255,255,0.15) 0%, rgba(255,255,255,0) 40%, rgba(255,255,255,0) 100%); transform: rotate(30deg); pointer-events: none;"></div>
                        <div style="position: absolute; top: 0; right: 0; width: 120px; height: 120px; background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%); border-radius: 50%; pointer-events: none;"></div>
                    ''')

                    # Top section: Issuer and EMV Chip
                    with ui.row().classes('w-full justify-between items-start position-relative z-10'):
                        with ui.column().classes('gap-1'):
                            with ui.row().classes('items-center gap-2'):
                                ui.label(c['emoji']).classes('text-lg')
                                disp_name = "CT Mastercard" if _is_ct(c) else c['name']
                                ui.label(disp_name).classes('text-sm font-black tracking-wider uppercase').style('letter-spacing: 0.1em; opacity: 0.9;')
                            if c.get('method'):
                                ui.label(c['method']).classes('text-xs font-semibold').style('opacity: 0.6; letter-spacing: 0.05em;')
                        
                        # EMV Chip SVG
                        ui.html(f'''
                            <svg width="40" height="32" viewBox="0 0 40 32" fill="none" xmlns="http://www.w3.org/2000/svg" style="opacity: 0.8; filter: drop-shadow(0 2px 4px rgba(0,0,0,0.2));">
                                <rect width="40" height="32" rx="6" fill="#fbbf24"/>
                                <path d="M0 10H12V22H0V10Z" fill="#f59e0b"/>
                                <path d="M28 10H40V22H28V10Z" fill="#f59e0b"/>
                                <path d="M14 0H26V10H14V0Z" fill="#f59e0b"/>
                                <path d="M14 22H26V32H14V22Z" fill="#f59e0b"/>
                                <path d="M12 10H28V22H12V10Z" stroke="#d97706" stroke-width="1"/>
                            </svg>
                        ''')

                    # Bottom section: Balances and Utilization
                    with ui.column().classes('w-full gap-4 position-relative z-10 mt-6'):
                        with ui.row().classes('w-full justify-between items-end'):
                            with ui.column().classes('gap-0'):
                                ui.label('Balance').classes('text-xs font-semibold uppercase tracking-wider').style('opacity: 0.7;')
                                ui.label(currency(c.get('balance', 0.0))).classes('text-2xl font-black').style('letter-spacing: -0.02em; font-feature-settings: "tnum"; text-shadow: 0 2px 8px rgba(0,0,0,0.3);')
                            with ui.column().classes('gap-0 items-end'):
                                ui.label('Available').classes('text-xs font-semibold uppercase tracking-wider').style('opacity: 0.7;')
                                ui.label(currency(c.get('remaining', 0.0)) if c.get('limit') else '---').classes('text-base font-bold').style(f'color: {text_color}; opacity: 0.95; font-feature-settings: "tnum";')

                        # Custom Utilization Bar (integrated into the card)
                        with ui.column().classes('w-full gap-1'):
                            with ui.row().classes('w-full justify-between items-center'):
                                ui.label(f"Limit: {currency(c['limit']) if c.get('limit') else '---'}").classes('text-xs font-medium').style('opacity: 0.8; font-feature-settings: "tnum";')
                                ui.label(pct_display).classes('text-xs font-extrabold').style(f'color: {accent}; text-shadow: 0 1px 2px rgba(0,0,0,0.5);')
                            with ui.element('div').style('width: 100%; height: 6px; border-radius: 3px; background: rgba(0,0,0,0.3); box-shadow: inset 0 1px 2px rgba(0,0,0,0.2); overflow: hidden;'):
                                ui.element('div').style(f"width: {pct_val*100:.1f}%; height: 100%; border-radius: 3px; background: {util_grad}; box-shadow: 0 0 8px rgba(255,255,255,0.2);")

        def _two_row(items):
            # 9.8.1: Responsive grid — constrained card width on desktop for realistic card proportions
            with ui.element('div').classes('grid grid-cols-1 md:grid-cols-2 gap-5 w-full').style('max-width: 900px;'):
                for c in items:
                    _tile(c, col='w-full')

        # --- Render: Canadian Tire
        if ct:
            ui.label('Canadian Tire').classes('mf-section-title mt-4')
            _two_row(ct)

        # --- Render: RBC Cards
        if rbc:
            ui.label('RBC Cards').classes('mf-section-title mt-6')
            _two_row(rbc)

        if other:
            ui.label('Other Cards').classes('mf-section-title mt-6')
            _two_row(other)

        # LOC
        if loc:
            ui.label('Line of Credit').classes('mf-section-title mt-6')
            with ui.element('div').classes('grid grid-cols-1 gap-4 w-full'):
                for c in loc:
                    _tile(c, col='w-full', emph=True)

    shell(content)


@ui.page("/recurring")
def recurring_page():
    if not require_login():
        nav_to("/login")
        return

    def content():
        rdf = cached_df("recurring")
        # Premium header
        with ui.card().classes("my-card p-0").style("overflow: hidden; margin-bottom: 12px;"):
            ui.element('div').style('height: 3px; background: linear-gradient(90deg, #22c55e, #10b981); border-radius: 0;')
            with ui.row().classes("items-center gap-3 p-5"):
                with ui.element("div").classes("mf-icon-box").style("background: rgba(34,197,94,0.12);"):
                    ui.icon("autorenew").style("font-size: 20px; color: #22c55e;")
                with ui.column().classes("gap-0"):
                    ui.label("Recurring Templates").classes("text-xl font-extrabold").style("letter-spacing: -0.02em;")
                    ui.label("Auto-generated when due date arrives.").classes("text-xs").style("color: var(--mf-muted)")

        with ui.card().classes("my-card p-5"):
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
            ], rows=rdf2.to_dict(orient="records"), row_key="recurring_id", selection='single').classes("w-full")

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
            return "  ".join(shown) + (f"  +{tail}" if tail > 0 else "")

        # Premium rules header
        with ui.card().classes('my-card p-0 mb-4').style('overflow: hidden;'):
            ui.element('div').style('height: 3px; background: linear-gradient(90deg, #f59e0b, #f97316); border-radius: 0;')
            with ui.row().classes('items-center gap-3 p-5'):
                with ui.element("div").classes("mf-icon-box").style("background: rgba(245,158,11,0.12);"):
                    ui.icon("rule").style("font-size: 22px; color: #f59e0b;")
                with ui.column().classes('gap-0'):
                    ui.label('Rules').classes('text-xl font-extrabold').style('letter-spacing: -0.02em;')
                    ui.label('Keyword  category mapping used for Auto-category').classes('text-xs').style('color: var(--mf-muted);')

        with ui.row().classes("w-full gap-4 mt-4"):

            # ------------------------------
            # LEFT: Rule list (compact)
            # ------------------------------
            with ui.card().classes("my-card").style("width: 340px; max-width: 100%;"):
                with ui.row().classes("items-center justify-between"):
                    ui.label("Rule list").classes("text-sm font-semibold").style("color: var(--mf-text);")
                    ui.button("", icon="add").props("flat round").on("click", lambda e: clear_selection())
                ui.separator().classes("opacity-30 my-2")

                search = ui.input(placeholder="Search keyword/category").props("outlined dense clearable").classes("w-full mb-2")
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
                            ui.label(cat or "").classes("text-sm font-semibold").style("color: var(--mf-text);")
                            ui.label(chips_preview(keys, 4) or "").classes("text-xs").style("color: var(--mf-muted);")
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

                kw_input = ui.input('Keywords', placeholder="e.g. walmart, superstore, uber").props('outlined').classes("w-full mt-2")
                chips_row = ui.row().classes("w-full items-center gap-2").style("flex-wrap: wrap; margin-top: 10px;")
                hint_label = ui.label("Tip: Use multiple keywords separated by commas. Matching is case-insensitive.").classes("text-xs").style(
                    "color: var(--mf-muted); margin-top:6px;"
                )

                ui.separator().classes("opacity-30 my-3")

                cat_input = ui.input('Category', placeholder="e.g. Groceries").props('outlined').classes("w-full")

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
    for sep in ('|', ''):
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
        for sep in ('|', ''):
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
        # Premium budgets header
        with ui.card().classes('my-card p-0 mb-4').style('overflow: hidden;'):
            ui.element('div').style('height: 3px; background: linear-gradient(90deg, #8b5cf6, #a855f7); border-radius: 0;')
            with ui.row().classes('items-center gap-3 p-5'):
                with ui.element("div").classes("mf-icon-box").style("background: rgba(139,92,246,0.12);"):
                    ui.icon("savings").style("font-size: 22px; color: #8b5cf6;")
                with ui.column().classes('gap-0'):
                    ui.label('Budgets').classes('text-xl font-extrabold').style('letter-spacing: -0.02em;')
                    ui.label('Create and manage monthly budgets per category').classes('text-xs').style('color: var(--mf-muted);')

        with ui.card().classes('my-card p-5'):

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
            bud_in = ui.number('Monthly budget', value=0).classes('w-full')

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
def data_tools_redirect() -> None:
    """Legacy redirect."""
    nav_to('/data_upload')

@ui.page('/data_upload')
def data_upload_page() -> None:
    if not require_login():
        nav_to('/login')
        return

    def content() -> None:
        # 9.10: Smart Data Upload header
        with ui.card().classes('my-card p-0 mb-4').style('overflow: hidden;'):
            ui.element('div').style('height: 3px; background: linear-gradient(90deg, #06b6d4, #0ea5e9); border-radius: 0;')
            with ui.row().classes('items-center gap-3 p-5'):
                with ui.element("div").classes("mf-icon-box").style("background: rgba(6,182,212,0.12);"):
                    ui.icon("cloud_upload").style("font-size: 22px; color: #06b6d4;")
                with ui.column().classes('gap-0'):
                    ui.label('Data Upload').classes('text-xl font-extrabold').style('letter-spacing: -0.02em;')
                    ui.label('Smart spreadsheet import with card detection & recurring analysis').classes('text-xs').style('color: var(--mf-muted);')

        # 9.11: Single-section Data Upload with inline mode + restore
        _upload_state = {'mode': 'append', 'file_data': None, 'file_name': ''}
        _result_container = ui.column().classes('w-full gap-3')

        with ui.card().classes('my-card p-5'):
            # Step 1: Expected format (collapsible hint)
            with ui.expansion('Expected Spreadsheet Columns', icon='info').classes('w-full').style('font-weight:600;'):
                _cols_info = [
                    ('Date', 'Transaction date (any format)'),
                    ('International Transaction', 'Amount for international transfers'),
                    ('Credit', 'Income / salary credited'),
                    ('Investment', 'Investment transactions'),
                    ('Credit Card repay', 'CC repayment amounts'),
                    ('Debit', 'Expenses / debit amounts'),
                    ('Reason/Note', 'Description — used for card detection & category inference'),
                ]
                for _cn, _cd in _cols_info:
                    with ui.row().classes('items-center gap-3 w-full').style('padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.04);'):
                        ui.label(_cn).classes('text-xs font-bold').style('min-width:150px;color:#06b6d4;')
                        ui.label(_cd).classes('text-xs').style('color:var(--mf-muted);')

            ui.element('div').style('height:16px;')

            # Step 2: File picker
            ui.label('Choose your spreadsheet (.csv or .xlsx)').classes('text-sm font-semibold mb-2')

            _status_label = ui.label('').classes('text-xs mt-2').style('color:var(--mf-muted);')

            async def _on_file_selected(e):
                """Handle NiceGUI 3.x upload: e.file is a FileUpload with async read()."""
                try:
                    raw = await e.file.read()
                    _upload_state['file_data'] = raw if isinstance(raw, (bytes, bytearray)) else bytes(raw)
                    _upload_state['file_name'] = getattr(e.file, 'name', 'uploaded_file') or 'uploaded_file'
                    _status_label.set_text(f'✓ File ready: {_upload_state["file_name"]} ({len(raw):,} bytes)')
                except Exception as ex:
                    _logger.error('Upload read error: %s', ex)
                    ui.notify(f'Error reading file: {ex}', type='negative')

            ui.upload(
                label='Select file', auto_upload=True,
                on_upload=_on_file_selected,
            ).props('accept=.csv,.xlsx,.xls').classes('w-full')

            ui.element('div').style('height:12px;')

            # Step 3: Append or Replace toggle + Restore button
            with ui.row().classes('items-center gap-4 flex-wrap'):
                _mode_toggle = ui.toggle(
                    {'append': 'Append', 'replace': 'Replace All'},
                    value='append'
                ).props('no-caps rounded dense').style('font-weight:600;')
                def _on_mode_change(e):
                    _upload_state['mode'] = e.value
                _mode_toggle.on('update:model-value', _on_mode_change)

                async def _do_restore():
                    data = _upload_state.get('file_data')
                    if not data:
                        ui.notify('Upload a file first.', type='warning')
                        return
                    fname = _upload_state.get('file_name', '')
                    try:
                        if fname.endswith(('.xlsx', '.xls')):
                            try:
                                import openpyxl  # noqa: F401
                            except ImportError:
                                import subprocess, sys
                                _status_label.set_text('Installing xlsx support...')
                                await run.io_bound(lambda: subprocess.check_call(
                                    [sys.executable, '-m', 'pip', 'install', 'openpyxl', '-q']
                                ))
                            df = pd.read_excel(io.BytesIO(data), engine='openpyxl')
                        else:
                            df = parse_uploaded_csv(data)
                        if df is None or df.empty:
                            ui.notify('File is empty.', type='warning')
                            return

                        _file_cols = [str(c).strip() for c in df.columns]
                        _file_cols_lower = {c.lower() for c in _file_cols}
                        _is_file_wide = not ('type' in _file_cols_lower and 'amount' in _file_cols_lower)

                        mode = _upload_state.get('mode', 'append')
                        _status_label.set_text(f'Processing... ({mode} mode)')

                        # ---- Step 1: Serialize all values for Google Sheets ----
                        # Timestamps -> 'YYYY-MM-DD', NaN/NaT -> '', floats .0 -> int
                        def _serialize(val):
                            if val is None:
                                return ''
                            if isinstance(val, pd.Timestamp):
                                return val.strftime('%Y-%m-%d') if pd.notna(val) else ''
                            if isinstance(val, dt.datetime):
                                return val.strftime('%Y-%m-%d')
                            if isinstance(val, dt.date):
                                return val.isoformat()
                            if isinstance(val, float):
                                if pd.isna(val):
                                    return ''
                                if val == int(val):
                                    return str(int(val))
                                return str(val)
                            s = str(val).strip()
                            return '' if s.lower() == 'nan' or s.lower() == 'nat' else s

                        # Apply serialization to every cell
                        for col in df.columns:
                            df[col] = df[col].apply(_serialize)

                        # Drop fully-empty rows
                        _total_before = len(df)
                        df = df[df.apply(lambda r: any(v != '' for v in r.values), axis=1)]

                        imported = 0
                        _skipped = _total_before - len(df)  # empty rows dropped

                        # ---- Step 2: Ensure sheet headers match file columns ----
                        _sheet_hdrs = sheet_headers('transactions')
                        _sheet_lower_set = {h.lower().strip() for h in _sheet_hdrs}
                        _overlap = _file_cols_lower.intersection(_sheet_lower_set)
                        _headers_fixed = False
                        if len(_overlap) < 2:
                            # Sheet headers don't match file (corrupted by prior Replace).
                            # Fix them to match the file's column names.
                            _w = ws('transactions')
                            await run.io_bound(lambda: _w.update('A1', [_file_cols]))
                            _header_cache['transactions'] = _file_cols
                            _headers_fixed = True

                        # ---- Step 3: Write data ----
                        if mode == 'replace':
                            # One-shot batch: clear sheet, write headers + all rows
                            await run.io_bound(lambda: write_df_to_sheet('transactions', df, _file_cols))
                            _header_cache['transactions'] = _file_cols
                            invalidate('transactions')
                            imported = len(df)
                        else:
                            # Append: write each row via gspread append_row (positional)
                            # Use the worksheet directly with a list of values (not dict)
                            # to avoid header-matching issues with the dict-based append_row.
                            _w = ws('transactions')
                            for _, row in df.iterrows():
                                vals = [row[c] for c in _file_cols]
                                if all(v == '' for v in vals):
                                    _skipped += 1
                                    continue
                                await run.io_bound(
                                    lambda v=list(vals): _w.append_row(v, value_input_option='USER_ENTERED')
                                )
                                imported += 1
                            invalidate('transactions')

                        invalidate('recurring')

                        _fmt = 'Wide' if _is_file_wide else 'Long'

                        # Save restore timestamp for display
                        _restore_ts = now_iso()
                        app.storage.user['last_restore'] = {
                            'time': _restore_ts,
                            'file': fname,
                            'rows': imported,
                            'mode': mode,
                        }

                        _status_label.set_text('')
                        _result_container.clear()
                        with _result_container:
                            with ui.element('div').style(
                                'border-radius:16px;padding:20px;'
                                'background:var(--mf-card-bg, linear-gradient(165deg, var(--mf-card-top), var(--mf-card-bottom)));'
                                'border:1px solid rgba(34,197,94,0.25);box-shadow:0 4px 16px rgba(34,197,94,0.1);'
                            ):
                                with ui.row().classes('items-center gap-3 mb-3'):
                                    ui.icon('check_circle').style('font-size:28px;color:#22c55e;')
                                    ui.label('Import Complete').classes('text-lg font-extrabold').style('color:#22c55e;')
                                _stats = [
                                    ('Rows written to sheet', str(imported)),
                                    ('Rows skipped (empty)', str(_skipped)),
                                    ('File format', _fmt),
                                    ('Upload mode', 'Replace' if mode == 'replace' else 'Append'),
                                    ('Source rows in file', str(_total_before)),
                                    ('File', fname),
                                ]
                                if _headers_fixed:
                                    _stats.insert(3, ('Sheet headers', 'Auto-fixed to match file'))
                                for _sl, _sv in _stats:
                                    with ui.row().classes('items-center justify-between w-full').style('padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.04);'):
                                        ui.label(_sl).classes('text-xs font-medium').style('color:var(--mf-muted);')
                                        ui.label(_sv).classes('text-sm font-bold').style('font-feature-settings:"tnum";')
                        ui.notify(f'Imported {imported} rows ({_fmt}).', type='positive')

                    except Exception as ex:
                        ui.notify(f'Import failed: {ex}', type='negative')
                        _status_label.set_text(f'Error: {ex}')
                        import traceback
                        _logger.error('Smart upload error: %s', traceback.format_exc())

                ui.button('Restore', icon='restore', on_click=_do_restore).props('unelevated rounded').style(
                    'background:linear-gradient(135deg,#06b6d4,#0ea5e9);color:white;font-weight:700;text-transform:none;padding:8px 28px;')

            ui.label('Select Append to add new data or Replace All to clear existing data before import. Then click Restore.').classes('text-[11px] mt-2').style('color:var(--mf-muted);')

            # ── Restore History Note ──
            _last = app.storage.user.get('last_restore')
            if _last and isinstance(_last, dict):
                _lr_time = _last.get('time', '')
                _lr_file = _last.get('file', 'unknown')
                _lr_rows = _last.get('rows', '?')
                _lr_mode = _last.get('mode', '')
                # Format timestamp for display
                try:
                    _dt_obj = dt.datetime.fromisoformat(_lr_time)
                    _display_time = _dt_obj.strftime('%b %d, %Y at %I:%M %p UTC')
                except Exception:
                    _display_time = _lr_time or 'unknown'
                ui.element('div').style('height:16px;')
                with ui.element('div').style(
                    'border-radius:12px;padding:14px 18px;'
                    'background:rgba(6,182,212,0.06);'
                    'border:1px solid rgba(6,182,212,0.15);'
                ):
                    with ui.row().classes('items-center gap-2 mb-1'):
                        ui.icon('history').style('font-size:18px;color:#06b6d4;')
                        ui.label('Last Restore').classes('text-sm font-bold').style('color:#06b6d4;')
                    with ui.column().classes('gap-1 ml-1'):
                        ui.label(f'{_display_time}').classes('text-xs font-semibold')
                        ui.label(f'File: {_lr_file}  ·  {_lr_rows} rows  ·  {_lr_mode.title() if _lr_mode else ""}').classes('text-[11px]').style('color:var(--mf-muted);')

    shell(content)


# ── 9.11: Merchants page ─────────────────────────────────────
_MERCHANTS = [
    # (name, search_keywords, icon, category, brand_color, img_url_or_none)
    ("Walmart", ["walmart"], "shopping_cart", "Grocery & Supermarket", "#0071CE",
     "https://www.google.com/s2/favicons?domain=walmart.ca&sz=128"),
    ("Costco", ["costco"], "shopping_cart", "Grocery & Supermarket", "#E31837",
     "https://www.google.com/s2/favicons?domain=costco.ca&sz=128"),
    ("Gill's Supermarket", ["gill"], "local_grocery_store", "Grocery & Supermarket", "#4CAF50", None),
    ("Dino's", ["dino"], "local_grocery_store", "Grocery & Supermarket", "#8BC34A", None),
    ("Bombay Spices", ["bombay spice", "bombay"], "storefront", "Grocery & Supermarket", "#FF9800", None),
    ("McDonalds", ["mcdonald", "mcdonalds", "mcd"], "fastfood", "Restaurants & Dining", "#FFC72C",
     "https://www.google.com/s2/favicons?domain=mcdonalds.com&sz=128"),
    ("Tim Hortons", ["tim horton", "tims", "timhorton"], "local_cafe", "Restaurants & Dining", "#C8102E",
     "https://www.google.com/s2/favicons?domain=timhortons.com&sz=128"),
    ("Amazon", ["amazon"], "shopping_bag", "Discount & Online", "#FF9900",
     "https://www.google.com/s2/favicons?domain=amazon.ca&sz=128"),
    ("Dollarama", ["dollarama"], "store", "Discount & Online", "#00A651",
     "https://www.google.com/s2/favicons?domain=dollarama.com&sz=128"),
]

# Category icons for the merchant section header
_MERCH_CAT_ICONS = {
    "Grocery & Supermarket": "local_grocery_store",
    "Restaurants & Dining": "restaurant",
    "Discount & Online": "local_offer",
}

@ui.page('/merchants')
def merchants_page() -> None:
    if not require_login():
        nav_to('/login')
        return

    def content() -> None:
        # Header
        with ui.card().classes('my-card p-0 mb-4').style('overflow:hidden;'):
            ui.element('div').style('height:4px;background:linear-gradient(90deg,#6366f1,#8b5cf6,#ec4899);border-radius:16px 16px 0 0;')
            with ui.column().classes('p-6 gap-2'):
                with ui.row().classes('items-center gap-4'):
                    with ui.element('div').style(
                        'width:48px;height:48px;border-radius:16px;display:flex;align-items:center;justify-content:center;'
                        'background:linear-gradient(135deg,#6366f1,#8b5cf6);box-shadow:0 8px 24px rgba(99,102,241,0.3);'
                    ):
                        ui.icon('storefront').style('font-size:24px;color:white;')
                    with ui.column().classes('gap-0'):
                        ui.label('Merchants').classes('text-2xl font-black').style('letter-spacing:-0.03em;')
                        ui.label('Monthly spend & all-time totals by merchant').classes('text-xs font-semibold uppercase tracking-wider').style('color:var(--mf-muted);')

        # Load transactions
        tx = cached_df('transactions')
        if tx.empty:
            ui.label('No transaction data available yet.').style('color:var(--mf-muted);padding:20px;')
            return

        tx['date_parsed'] = tx['date'].apply(parse_date)
        tx = tx[tx['date_parsed'].notna()].copy()
        tx['amount_num'] = tx['amount'].apply(to_float)
        tx['type_l'] = tx.get('type', pd.Series(dtype=str)).astype(str).str.lower().str.strip()
        tx['notes_l'] = tx.get('notes', pd.Series(dtype=str)).astype(str).str.lower()
        tx['cat_l'] = tx.get('category', pd.Series(dtype=str)).astype(str).str.lower()
        tx['month'] = tx['date_parsed'].apply(lambda d: d.strftime('%Y-%m'))
        _cur_month = month_key(today())
        _prev_d = today().replace(day=1) - dt.timedelta(days=1)
        _prev_month = month_key(_prev_d)

        def _match_merchant(notes_lower: str, keywords: list) -> bool:
            return any(kw in notes_lower for kw in keywords)

        # Gather per-merchant data
        _all_merchant_data = []  # (name, icon, color, img, cur_spend, total, tx_count, diff_pct)
        _categories: dict[str, list] = {}
        for m_name, m_keys, m_icon, m_cat, m_color, m_img in _MERCHANTS:
            _categories.setdefault(m_cat, [])

            _m_mask = tx['notes_l'].apply(lambda n, k=m_keys: _match_merchant(n, k))
            _m_spend = tx[_m_mask & tx['type_l'].isin(['debit', 'expense'])]
            _total = float(_m_spend['amount_num'].sum()) if not _m_spend.empty else 0.0
            _tx_count = len(_m_spend)
            _cur_spend = float(_m_spend[_m_spend['month'] == _cur_month]['amount_num'].sum()) if not _m_spend.empty else 0.0
            _prev_spend = float(_m_spend[_m_spend['month'] == _prev_month]['amount_num'].sum()) if not _m_spend.empty else 0.0
            _diff = _cur_spend - _prev_spend
            _diff_pct = round((_diff / _prev_spend) * 100) if _prev_spend > 0 else 0

            _entry = (m_name, m_icon, m_color, m_img, _cur_spend, _total, _tx_count, _diff_pct, _diff)
            _categories[m_cat].append(_entry)
            _all_merchant_data.append(_entry)

        # ── Render each category ──
        for cat_name, merchants in _categories.items():
            _cat_icon = _MERCH_CAT_ICONS.get(cat_name, 'category')
            _cat_color = merchants[0][2] if merchants else '#6366f1'
            # Category header
            with ui.element('div').style(
                'width:100%;border-radius:20px;overflow:hidden;'
                'background:var(--mf-card-bg, linear-gradient(165deg, var(--mf-card-top), var(--mf-card-bottom)));'
                'border:1px solid var(--mf-card-border);box-shadow:0 6px 24px rgba(0,0,0,0.22);margin-bottom:16px;'
            ):
                ui.element('div').style(f'height:3px;background:linear-gradient(90deg,{_cat_color},{_cat_color}88);')
                with ui.element('div').style('padding:20px;'):
                    with ui.row().classes('items-center gap-2 mb-4'):
                        ui.icon(_cat_icon).style(f'font-size:18px;color:{_cat_color};')
                        ui.label(cat_name).classes('text-base font-extrabold').style('letter-spacing:-0.02em;')

                    # 9.11.2: Use NiceGUI ui.grid() for proper CSS grid
                    with ui.grid(columns='repeat(auto-fill, minmax(240px, 1fr))').style('gap:14px;width:100%;'):
                        for m_name, m_icon, m_color, m_img, _cur_spend, _total, _tx_count, _diff_pct, _diff in merchants:
                            with ui.element('div').style(
                                f'border-radius:18px;background:var(--mf-surface-2);'
                                f'border:1px solid {m_color}18;padding:16px;'
                                f'display:flex;flex-direction:column;gap:10px;transition:all 0.2s ease;'
                            ):
                                # Icon/logo row
                                with ui.row().classes('items-center gap-3'):
                                    if m_img:
                                        with ui.element('div').style(
                                            'width:40px;height:40px;border-radius:12px;display:flex;align-items:center;justify-content:center;'
                                            'background:white;box-shadow:0 2px 8px rgba(0,0,0,0.08);overflow:hidden;flex-shrink:0;'
                                        ):
                                            ui.html(f'<img src="{m_img}" style="width:30px;height:30px;object-fit:contain;" onerror="this.parentElement.innerHTML=\'<i class=q-icon notranslate material-icons style=font-size:22px;color:{m_color}>{m_icon}</i>\'"/>')
                                    else:
                                        with ui.element('div').style(
                                            f'width:40px;height:40px;border-radius:12px;display:flex;align-items:center;justify-content:center;'
                                            f'background:{m_color}15;flex-shrink:0;'
                                        ):
                                            ui.icon(m_icon).style(f'font-size:20px;color:{m_color};')
                                    ui.label(m_name).classes('text-sm font-bold').style('white-space:nowrap;overflow:hidden;text-overflow:ellipsis;')

                                # This month spend
                                ui.label('This Month').classes('text-[10px] font-semibold').style('color:var(--mf-muted);text-transform:uppercase;letter-spacing:0.06em;')
                                ui.label(currency(_cur_spend)).classes('text-lg font-black').style(f'color:{m_color};font-feature-settings:"tnum";letter-spacing:-0.02em;margin-top:-4px;')

                                # Month-over-month badge
                                if _diff != 0:
                                    _arrow = 'trending_up' if _diff > 0 else 'trending_down'
                                    _ac = '#ef4444' if _diff > 0 else '#22c55e'
                                    _sign = '+' if _diff > 0 else ''
                                    with ui.element('div').style(
                                        f'display:flex;align-items:center;gap:4px;background:{_ac}12;padding:3px 8px;border-radius:8px;align-self:flex-start;'
                                    ):
                                        ui.icon(_arrow).style(f'font-size:14px;color:{_ac};')
                                        ui.label(f'{_sign}{_diff_pct}% vs last mo').classes('text-[10px] font-semibold').style(f'color:{_ac};')

        # ── Fuel Section ──
        _fuel_keywords = ['fuel', 'gas', 'petro', 'shell', 'esso', 'pioneer', 'canadian tire gas', 'costco gas', 'gasoline']
        _fuel_mask = tx['notes_l'].apply(lambda n: any(k in n for k in _fuel_keywords)) | tx['cat_l'].str.contains('fuel|gas', na=False)
        _fuel_tx = tx[_fuel_mask & tx['type_l'].isin(['debit', 'expense'])]
        _fuel_total = float(_fuel_tx['amount_num'].sum()) if not _fuel_tx.empty else 0.0
        _fuel_count = len(_fuel_tx)
        _fuel_cur = float(_fuel_tx[_fuel_tx['month'] == _cur_month]['amount_num'].sum()) if not _fuel_tx.empty else 0.0
        _fuel_prev = float(_fuel_tx[_fuel_tx['month'] == _prev_month]['amount_num'].sum()) if not _fuel_tx.empty else 0.0
        _fuel_diff = _fuel_cur - _fuel_prev
        _fuel_pct = round((_fuel_diff / _fuel_prev) * 100) if _fuel_prev > 0 else 0

        with ui.card().classes('my-card p-0 mb-4').style('overflow:hidden;'):
            ui.element('div').style('height:3px;background:linear-gradient(90deg,#f97316,#ef4444);')
            with ui.column().classes('p-5 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.icon('local_gas_station').style('font-size:18px;color:#f97316;')
                    ui.label('Fuel & Gas').classes('text-base font-extrabold').style('letter-spacing:-0.02em;')
                with ui.element('div').style(
                    'display:flex;align-items:center;gap:14px;padding:16px;'
                    'border-radius:16px;background:linear-gradient(135deg,rgba(249,115,22,0.08),rgba(239,68,68,0.04));'
                    'border:1px solid rgba(249,115,22,0.15);'
                ):
                    with ui.element('div').style(
                        'width:52px;height:52px;border-radius:16px;display:flex;align-items:center;justify-content:center;'
                        'background:linear-gradient(135deg,#f97316,#ef4444);box-shadow:0 8px 24px rgba(249,115,22,0.25);flex-shrink:0;'
                    ):
                        ui.icon('local_gas_station').style('font-size:24px;color:white;')
                    with ui.column().classes('gap-1').style('flex:1;'):
                        ui.label('All-Time Fuel Spend').classes('text-xs font-semibold').style('color:var(--mf-muted);text-transform:uppercase;letter-spacing:0.06em;')
                        ui.label(currency(_fuel_total)).classes('text-2xl font-black').style('color:#f97316;font-feature-settings:"tnum";letter-spacing:-0.03em;')
                        with ui.row().classes('items-center gap-4'):
                            ui.label(f'{_fuel_count} fill-ups').classes('text-xs').style('color:var(--mf-muted);')
                            ui.label(f'This month: {currency(_fuel_cur)}').classes('text-xs font-semibold').style('color:#fb923c;')
                            if _fuel_prev > 0 or _fuel_cur > 0:
                                _f_arrow = 'trending_up' if _fuel_diff > 0 else 'trending_down'
                                _f_ac = '#ef4444' if _fuel_diff > 0 else '#22c55e'
                                _f_sign = '+' if _fuel_diff > 0 else ''
                                _f_word = 'higher' if _fuel_diff > 0 else 'lower'
                                with ui.element('div').style(f'display:flex;align-items:center;gap:4px;background:{_f_ac}12;padding:3px 8px;border-radius:8px;'):
                                    ui.icon(_f_arrow).style(f'font-size:14px;color:{_f_ac};')
                                    ui.label(f'{_f_sign}{_fuel_pct}% {_f_word} than last month').classes('text-[10px] font-semibold').style(f'color:{_f_ac};')

        # ── International Transfers section ──
        _intl_tx = tx[tx['type_l'].isin(['international', 'international transfer', 'intl'])]
        _intl_total = float(_intl_tx['amount_num'].sum()) if not _intl_tx.empty else 0.0
        _intl_count = len(_intl_tx)
        _intl_cur = float(_intl_tx[_intl_tx['month'] == _cur_month]['amount_num'].sum()) if not _intl_tx.empty else 0.0
        _intl_prev = float(_intl_tx[_intl_tx['month'] == _prev_month]['amount_num'].sum()) if not _intl_tx.empty else 0.0
        _intl_diff = _intl_cur - _intl_prev
        _intl_pct = round((_intl_diff / _intl_prev) * 100) if _intl_prev > 0 else 0

        with ui.card().classes('my-card p-0 mb-4').style('overflow:hidden;'):
            ui.element('div').style('height:3px;background:linear-gradient(90deg,#6366f1,#8b5cf6);')
            with ui.column().classes('p-5 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.icon('public').style('font-size:18px;color:#6366f1;')
                    ui.label('International Transfers').classes('text-base font-extrabold').style('letter-spacing:-0.02em;')
                with ui.element('div').style(
                    'display:flex;align-items:center;gap:14px;padding:16px;'
                    'border-radius:16px;background:linear-gradient(135deg,rgba(99,102,241,0.08),rgba(139,92,246,0.05));'
                    'border:1px solid rgba(99,102,241,0.15);'
                ):
                    with ui.element('div').style(
                        'width:52px;height:52px;border-radius:16px;display:flex;align-items:center;justify-content:center;'
                        'background:linear-gradient(135deg,#6366f1,#8b5cf6);box-shadow:0 8px 24px rgba(99,102,241,0.25);flex-shrink:0;'
                    ):
                        ui.icon('send').style('font-size:24px;color:white;')
                    with ui.column().classes('gap-1').style('flex:1;'):
                        ui.label('All-Time Total Sent').classes('text-xs font-semibold').style('color:var(--mf-muted);text-transform:uppercase;letter-spacing:0.06em;')
                        ui.label(currency(_intl_total)).classes('text-2xl font-black').style('color:#8b5cf6;font-feature-settings:"tnum";letter-spacing:-0.03em;')
                        with ui.row().classes('items-center gap-4'):
                            ui.label(f'{_intl_count} transfers').classes('text-xs').style('color:var(--mf-muted);')
                            ui.label(f'This month: {currency(_intl_cur)}').classes('text-xs font-semibold').style('color:#a78bfa;')
                            if _intl_prev > 0 or _intl_cur > 0:
                                _i_arrow = 'trending_up' if _intl_diff > 0 else 'trending_down'
                                _i_ac = '#ef4444' if _intl_diff > 0 else '#22c55e'
                                _i_sign = '+' if _intl_diff > 0 else ''
                                _i_word = 'higher' if _intl_diff > 0 else 'lower'
                                with ui.element('div').style(f'display:flex;align-items:center;gap:4px;background:{_i_ac}12;padding:3px 8px;border-radius:8px;'):
                                    ui.icon(_i_arrow).style(f'font-size:14px;color:{_i_ac};')
                                    ui.label(f'{_i_sign}{_intl_pct}% {_i_word} than last month').classes('text-[10px] font-semibold').style(f'color:{_i_ac};')

        # ── Bottom: All-Time Totals per Merchant ──
        with ui.card().classes('my-card p-0 mb-4').style('overflow:hidden;'):
            ui.element('div').style('height:3px;background:linear-gradient(90deg,#64748b,#94a3b8);')
            with ui.column().classes('p-5 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    ui.icon('leaderboard').style('font-size:18px;color:#94a3b8;')
                    ui.label('All-Time Totals').classes('text-base font-extrabold').style('letter-spacing:-0.02em;')
                _sorted = sorted(_all_merchant_data, key=lambda x: x[5], reverse=True)
                for m_name, m_icon, m_color, m_img, _cs, _total, _tc, _dp, _d in _sorted:
                    with ui.row().classes('items-center justify-between w-full').style('padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.04);'):
                        with ui.row().classes('items-center gap-3'):
                            ui.icon(m_icon).style(f'font-size:18px;color:{m_color};')
                            ui.label(m_name).classes('text-sm font-semibold')
                        with ui.row().classes('items-center gap-3'):
                            ui.label(f'{_tc} tx').classes('text-[10px]').style('color:var(--mf-muted);')
                            ui.label(currency(_total)).classes('text-sm font-extrabold').style(f'color:{m_color};font-feature-settings:"tnum";')

    shell(content)


@ui.page('/reports')
def reports_page() -> None:
    if not require_login():
        nav_to('/login')
        return

    def content() -> None:
        # Premium header
        with ui.card().classes('my-card p-0 mb-4').style('overflow: hidden;'):
            ui.element('div').style('height: 3px; background: linear-gradient(90deg, #6366f1, #a855f7); border-radius: 0;')
            with ui.row().classes('items-center gap-3 p-5'):
                with ui.element("div").classes("mf-icon-box").style("background: rgba(99,102,241,0.12);"):
                    ui.icon("assessment").style("font-size: 22px; color: #6366f1;")
                with ui.column().classes('gap-0'):
                    ui.label('Reports & Analytics').classes('text-xl font-extrabold').style('letter-spacing: -0.02em;')
                    ui.label('Year-over-year trends, category analysis & savings').classes('text-xs').style('color: var(--mf-muted);')

        tx = cached_df('transactions')
        if tx.empty:
            ui.label('No transaction data available.').style('color: var(--mf-muted);')
            return

        tx['date_parsed'] = tx['date'].apply(parse_date)
        tx = tx[tx['date_parsed'].notna()].copy()
        tx['amount_num'] = tx['amount'].apply(to_float)
        tx['type_l'] = tx.get('type', pd.Series(dtype=str)).astype(str).str.lower().str.strip()
        tx['month'] = tx['date_parsed'].apply(lambda d: d.strftime('%Y-%m'))

        spend = tx[tx['type_l'].isin(['debit', 'expense'])].copy()
        inc = tx[tx['type_l'].isin(['credit', 'income'])].copy()

        # 1. Monthly Spending Trend (last 12 months)
        with ui.card().classes('my-card p-5'):
            ui.label('Monthly Spending Trend').classes('mf-section-title')
            try:
                monthly = spend.groupby('month', as_index=False)['amount_num'].sum().sort_values('month').tail(12)
                if not monthly.empty:
                    import plotly.express as px
                    fig = px.bar(monthly, x='month', y='amount_num', template=plotly_template(),
                                 labels={'month': 'Month', 'amount_num': 'Spending'})
                    fig.update_traces(marker_color='#ef4444')
                    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)',
                                      font_color=plotly_font_color(), showlegend=False)
                    ui.plotly(fig).classes('w-full')
                else:
                    ui.label('Not enough data.').style('color: var(--mf-muted);')
            except Exception as e:
                ui.label(f'Chart error: {e}').style('color: var(--mf-muted);')

        # 2. Category Breakdown Over Time (stacked bar)
        with ui.card().classes('my-card p-5'):
            ui.label('Category Breakdown by Month').classes('mf-section-title')
            try:
                if not spend.empty and 'category' in spend.columns:
                    cat_month = spend.groupby(['month', 'category'], as_index=False)['amount_num'].sum()
                    cat_month = cat_month.sort_values('month')
                    # Keep top 6 categories, group rest as "Other"
                    top_cats = spend.groupby('category')['amount_num'].sum().nlargest(6).index.tolist()
                    cat_month['category'] = cat_month['category'].apply(lambda c: c if c in top_cats else 'Other')
                    cat_month = cat_month.groupby(['month', 'category'], as_index=False)['amount_num'].sum()
                    import plotly.express as px
                    fig2 = px.bar(cat_month, x='month', y='amount_num', color='category', template=plotly_template(),
                                  labels={'month': 'Month', 'amount_num': 'Amount', 'category': 'Category'})
                    fig2.update_layout(margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)',
                                       font_color=plotly_font_color(), barmode='stack')
                    ui.plotly(fig2).classes('w-full')
                else:
                    ui.label('No category data.').style('color: var(--mf-muted);')
            except Exception as e:
                ui.label(f'Chart error: {e}').style('color: var(--mf-muted);')

        # 3. Savings Rate Over Time
        with ui.card().classes('my-card p-5'):
            ui.label('Monthly Savings Rate').classes('mf-section-title')
            try:
                m_inc = inc.groupby('month', as_index=False)['amount_num'].sum().rename(columns={'amount_num': 'income'})
                m_exp = spend.groupby('month', as_index=False)['amount_num'].sum().rename(columns={'amount_num': 'expenses'})
                merged = pd.merge(m_inc, m_exp, on='month', how='outer').fillna(0).sort_values('month').tail(12)
                merged['savings_rate'] = ((merged['income'] - merged['expenses']) / merged['income'].replace(0, 1) * 100).clip(-100, 100)
                if not merged.empty:
                    import plotly.express as px
                    fig3 = px.line(merged, x='month', y='savings_rate', template=plotly_template(),
                                   labels={'month': 'Month', 'savings_rate': 'Savings Rate (%)'}, markers=True)
                    fig3.update_traces(line_color='#22c55e')
                    fig3.add_hline(y=20, line_dash="dash", line_color="#eab308", annotation_text="20% target")
                    fig3.update_layout(margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)',
                                       font_color=plotly_font_color())
                    ui.plotly(fig3).classes('w-full')
                else:
                    ui.label('Not enough data.').style('color: var(--mf-muted);')
            except Exception as e:
                ui.label(f'Chart error: {e}').style('color: var(--mf-muted);')

        # 4. Income vs Expenses Summary Table
        with ui.card().classes('my-card p-5'):
            ui.label('Income vs Expenses Summary').classes('mf-section-title')
            try:
                m_inc2 = inc.groupby('month', as_index=False)['amount_num'].sum().rename(columns={'amount_num': 'Income'})
                m_exp2 = spend.groupby('month', as_index=False)['amount_num'].sum().rename(columns={'amount_num': 'Expenses'})
                summary = pd.merge(m_inc2, m_exp2, on='month', how='outer').fillna(0).sort_values('month', ascending=False).head(12)
                summary['Net'] = summary['Income'] - summary['Expenses']
                summary['Income'] = summary['Income'].apply(lambda v: currency(v))
                summary['Expenses'] = summary['Expenses'].apply(lambda v: currency(v))
                summary['Net'] = summary['Net'].apply(lambda v: currency(v))
                rows = summary.to_dict(orient='records')
                ui.table(columns=[
                    {'name': 'month', 'label': 'Month', 'field': 'month'},
                    {'name': 'Income', 'label': 'Income', 'field': 'Income', 'align': 'right'},
                    {'name': 'Expenses', 'label': 'Expenses', 'field': 'Expenses', 'align': 'right'},
                    {'name': 'Net', 'label': 'Net', 'field': 'Net', 'align': 'right'},
                ], rows=rows, row_key='month').classes('w-full')
            except Exception as e:
                ui.label(f'Table error: {e}').style('color: var(--mf-muted);')

        # 9.10: 5. Top Spending Categories (Donut chart)
        with ui.card().classes('my-card p-5'):
            ui.label('Top Spending Categories').classes('mf-section-title')
            try:
                if not spend.empty and 'category' in spend.columns:
                    _cat_totals = spend.groupby('category', as_index=False)['amount_num'].sum().sort_values('amount_num', ascending=False).head(8)
                    if not _cat_totals.empty:
                        import plotly.express as px
                        fig_donut = px.pie(_cat_totals, names='category', values='amount_num',
                                           template=plotly_template(), hole=0.45,
                                           color_discrete_sequence=['#8B5CF6', '#3B82F6', '#EF4444', '#F59E0B', '#10B981', '#EC4899', '#06B6D4', '#F97316'])
                        fig_donut.update_layout(margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)',
                                                font_color=plotly_font_color(), showlegend=True,
                                                legend=dict(orientation='h', yanchor='bottom', y=-0.2))
                        fig_donut.update_traces(textposition='inside', textinfo='percent+label', textfont_size=11)
                        ui.plotly(fig_donut).classes('w-full')
                    else:
                        ui.label('No category data.').style('color: var(--mf-muted);')
                else:
                    ui.label('No spending data.').style('color: var(--mf-muted);')
            except Exception as e:
                ui.label(f'Chart error: {e}').style('color: var(--mf-muted);')

        # 9.10: 6. Income vs Expenses Overlay (grouped bar)
        with ui.card().classes('my-card p-5'):
            ui.label('Income vs Expenses Trend').classes('mf-section-title')
            try:
                import plotly.graph_objects as go
                m_inc3 = inc.groupby('month', as_index=False)['amount_num'].sum().rename(columns={'amount_num': 'income'}).sort_values('month').tail(12)
                m_exp3 = spend.groupby('month', as_index=False)['amount_num'].sum().rename(columns={'amount_num': 'expenses'}).sort_values('month').tail(12)
                merged3 = pd.merge(m_inc3, m_exp3, on='month', how='outer').fillna(0).sort_values('month')
                if not merged3.empty:
                    fig_ie = go.Figure()
                    fig_ie.add_trace(go.Bar(x=merged3['month'], y=merged3['income'], name='Income', marker_color='#22c55e', marker_cornerradius=6))
                    fig_ie.add_trace(go.Bar(x=merged3['month'], y=merged3['expenses'], name='Expenses', marker_color='#ef4444', marker_cornerradius=6))
                    fig_ie.update_layout(barmode='group', template=plotly_template(),
                                         margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)',
                                         font_color=plotly_font_color(), legend=dict(orientation='h', yanchor='bottom', y=1.02))
                    ui.plotly(fig_ie).classes('w-full')
                else:
                    ui.label('Not enough data.').style('color: var(--mf-muted);')
            except Exception as e:
                ui.label(f'Chart error: {e}').style('color: var(--mf-muted);')

        # 9.10: 7. Day of Week Spending Pattern
        with ui.card().classes('my-card p-5'):
            ui.label('Spending by Day of Week').classes('mf-section-title')
            try:
                if not spend.empty and 'date_parsed' in spend.columns:
                    _dow = spend.copy()
                    _dow['dow'] = _dow['date_parsed'].apply(lambda d: d.strftime('%A'))
                    _dow['dow_num'] = _dow['date_parsed'].apply(lambda d: d.weekday())
                    _dow_totals = _dow.groupby(['dow', 'dow_num'], as_index=False)['amount_num'].mean().sort_values('dow_num')
                    if not _dow_totals.empty:
                        import plotly.express as px
                        fig_dow = px.bar(_dow_totals, x='dow', y='amount_num', template=plotly_template(),
                                         labels={'dow': 'Day', 'amount_num': 'Avg Spending'},
                                         color='amount_num', color_continuous_scale=['#3B82F6', '#8B5CF6', '#EF4444'])
                        fig_dow.update_layout(margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)',
                                              font_color=plotly_font_color(), showlegend=False, coloraxis_showscale=False)
                        ui.plotly(fig_dow).classes('w-full')
                    else:
                        ui.label('Not enough data.').style('color: var(--mf-muted);')
                else:
                    ui.label('No spending data.').style('color: var(--mf-muted);')
            except Exception as e:
                ui.label(f'Chart error: {e}').style('color: var(--mf-muted);')

        # 9.10: 8. Payment Method Distribution
        with ui.card().classes('my-card p-5'):
            ui.label('Payment Method Distribution').classes('mf-section-title')
            try:
                if not spend.empty and 'method' in spend.columns:
                    _meth = spend.copy()
                    _meth['method'] = _meth['method'].astype(str).replace('', 'Unknown')
                    _meth_totals = _meth.groupby('method', as_index=False)['amount_num'].sum().sort_values('amount_num', ascending=False)
                    if not _meth_totals.empty and len(_meth_totals) > 1:
                        import plotly.express as px
                        fig_meth = px.pie(_meth_totals, names='method', values='amount_num',
                                          template=plotly_template(), hole=0.4,
                                          color_discrete_sequence=['#6366f1', '#22c55e', '#f59e0b', '#ef4444', '#06b6d4', '#ec4899'])
                        fig_meth.update_layout(margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor='rgba(0,0,0,0)',
                                               font_color=plotly_font_color(),
                                               legend=dict(orientation='h', yanchor='bottom', y=-0.2))
                        fig_meth.update_traces(textposition='inside', textinfo='percent+label', textfont_size=11)
                        ui.plotly(fig_meth).classes('w-full')
                    else:
                        ui.label('Not enough method data.').style('color: var(--mf-muted);')
                else:
                    ui.label('No method data.').style('color: var(--mf-muted);')
            except Exception as e:
                ui.label(f'Chart error: {e}').style('color: var(--mf-muted);')

    shell(content)


# -----------------------------
# About / Author
# -----------------------------
@ui.page('/about')
def about_page() -> None:
    if not require_login():
        nav_to('/login')
        return

    def content() -> None:
      with ui.element('div').classes('w-full py-4'):
        #  App Info Card
        with ui.card().classes('my-card p-0').style('overflow: hidden;'):
            ui.element('div').style('height: 4px; background: linear-gradient(90deg, #22C55E, #FBBF24); border-radius: 0;')
            with ui.column().classes('p-6 gap-4 items-center'):
                with ui.element('div').style(
                    'width: 80px; height: 80px; border-radius: 22px; display: flex; align-items: center; justify-content: center;'
                    'background: linear-gradient(135deg, #0F1923, #22C55E);'
                    'box-shadow: 0 8px 32px rgba(34,197,94,0.25);'
                ):
                    ui.icon('insights').style('font-size: 40px; color: #FBBF24;')
                ui.label('FinTrackr').style('font-size: 32px; font-weight: 800; letter-spacing: -0.04em; color: var(--mf-text);')
                ui.label(f'Version {APP_VERSION}').classes('text-sm').style('color: var(--mf-muted); margin-top: -8px;')
                ui.separator().classes('w-full opacity-20')
                ui.label(
                    'FinTrackr is a premium personal finance dashboard built to help you take control '
                    'of your money. Track every expense, scan receipts with AI-powered OCR, monitor '
                    'credit card utilization, set budgets, and visualize your spending patterns  all '
                    'from a single elegant interface.'
                ).classes('text-sm text-center').style('color: var(--mf-muted); line-height: 1.7; max-width: 720px;')

                # Feature highlights
                _features = [
                    ('document_scanner', 'AI Receipt Scanning', 'Snap a photo and let OCR extract amounts, merchants & categories automatically.'),
                    ('palette', '8 Premium Themes', 'From dark Midnight Blue to light Sand Gold  pick the look that suits you.'),
                    ('show_chart', 'Smart Analytics', 'Weekly cashflow charts, category breakdowns, budget alerts and monthly insights.'),
                    ('security', 'Passkey Auth', 'Biometric login with WebAuthn passkeys for secure, passwordless access.'),
                    ('call_split', 'Receipt Splitting', 'Multi-category split for Walmart, Costco & Superstore receipts.'),
                    ('autorenew', 'Recurring Templates', 'Set it and forget it  automatic transaction creation on due dates.'),
                ]
                with ui.element('div').classes('mf-about-features'):
                    for f_icon, f_title, f_desc in _features:
                        with ui.element('div').style(
                            'display: flex; align-items: flex-start; gap: 12px; padding: 12px;'
                            'border-radius: 12px; border: 1px solid var(--mf-border);'
                            'background: var(--mf-surface);'
                        ):
                            ui.icon(f_icon).style('font-size: 20px; color: var(--mf-accent); margin-top: 2px; flex-shrink: 0;')
                            with ui.column().classes('gap-0'):
                                ui.label(f_title).classes('text-sm font-bold').style('color: var(--mf-text);')
                                ui.label(f_desc).classes('text-xs').style('color: var(--mf-muted); line-height: 1.5;')

        #  Author Card
        with ui.card().classes('my-card p-0 mt-3').style('overflow: hidden;'):
            ui.element('div').style('height: 4px; background: linear-gradient(90deg, #6366f1, #a855f7); border-radius: 0;')
            with ui.column().classes('p-6 gap-4'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    with ui.element('div').classes('mf-icon-box').style('background: rgba(99,102,241,0.12);'):
                        ui.icon('person').style('font-size: 20px; color: #6366f1;')
                    ui.label('About the Author').classes('text-lg font-extrabold').style('letter-spacing: -0.02em;')

                with ui.element('div').classes('mf-about-author'):
                    # Avatar placeholder
                    with ui.element('div').style(
                        'width: 90px; height: 90px; border-radius: 50%; flex-shrink: 0;'
                        'background: linear-gradient(135deg, #6366f1, #a855f7);'
                        'display: flex; align-items: center; justify-content: center;'
                        'box-shadow: 0 6px 20px rgba(99,102,241,0.25);'
                    ):
                        ui.label('NR').style('font-size: 32px; font-weight: 800; color: #fff; letter-spacing: -0.03em;')

                    with ui.column().classes('gap-2 flex-1').style('min-width: 200px;'):
                        ui.label('Nishanth R').style('font-size: 22px; font-weight: 800; letter-spacing: -0.03em; color: var(--mf-text);')
                        ui.label('Oracle DBA').classes('text-sm font-medium').style('color: var(--mf-accent);')
                        ui.label(
                            'An experienced Oracle Database Administrator with a passion for building '
                            'elegant, data-driven applications. FinTrackr was born from a personal need '
                            'to track finances with the same precision and reliability that goes into '
                            'managing enterprise databases  clean architecture, robust error handling, '
                            'and a beautiful interface that makes financial management effortless.'
                        ).classes('text-sm').style('color: var(--mf-muted); line-height: 1.7;')

                ui.separator().classes('w-full opacity-20')

                # Contact links
                with ui.row().classes('items-center gap-4 flex-wrap'):
                    # Email
                    with ui.element('a').style(
                        'display: flex; align-items: center; gap: 8px; text-decoration: none;'
                        'padding: 8px 16px; border-radius: 10px; border: 1px solid var(--mf-border);'
                        'background: var(--mf-surface); color: var(--mf-text); cursor: pointer;'
                        'transition: background 0.2s ease;'
                    ).props('href="mailto:nishanth91.dba@gmail.com"'):
                        ui.icon('email').style('font-size: 18px; color: #ef4444;')
                        ui.label('nishanth91.dba@gmail.com').classes('text-sm font-medium')

                    # LinkedIn
                    with ui.element('a').style(
                        'display: flex; align-items: center; gap: 8px; text-decoration: none;'
                        'padding: 8px 16px; border-radius: 10px; border: 1px solid var(--mf-border);'
                        'background: var(--mf-surface); color: var(--mf-text); cursor: pointer;'
                        'transition: background 0.2s ease;'
                    ).props('href="https://www.linkedin.com/in/nishanth-r-ajay/" target="_blank"'):
                        ui.icon('work').style('font-size: 18px; color: #0A66C2;')
                        ui.label('LinkedIn').classes('text-sm font-medium')

                    # Instagram
                    with ui.element('a').style(
                        'display: flex; align-items: center; gap: 8px; text-decoration: none;'
                        'padding: 8px 16px; border-radius: 10px; border: 1px solid var(--mf-border);'
                        'background: var(--mf-surface); color: var(--mf-text); cursor: pointer;'
                        'transition: background 0.2s ease;'
                    ).props('href="https://www.instagram.com/n_1_5_h_/" target="_blank"'):
                        ui.icon('photo_camera').style('font-size: 18px; color: #E1306C;')
                        ui.label('@n_1_5_h_').classes('text-sm font-medium')

        #  Tech Stack Card
        with ui.card().classes('my-card p-0 mt-3').style('overflow: hidden;'):
            ui.element('div').style('height: 4px; background: linear-gradient(90deg, #22c55e, #3b82f6); border-radius: 0;')
            with ui.column().classes('p-6 gap-3'):
                with ui.row().classes('items-center gap-2 mb-1'):
                    with ui.element('div').classes('mf-icon-box').style('background: rgba(34,197,94,0.12);'):
                        ui.icon('code').style('font-size: 20px; color: #22c55e;')
                    ui.label('Tech Stack').classes('text-lg font-extrabold').style('letter-spacing: -0.02em;')

                _stack = [
                    ('Python + NiceGUI', 'Full-stack web framework with Quasar/Vue.js frontend'),
                    ('Google Sheets API', 'Zero-cost cloud database via gspread'),
                    ('Tesseract.js + Google Vision', 'Client-side & server-side OCR for receipt scanning'),
                    ('Plotly', 'Interactive charts for spending analytics'),
                    ('WebAuthn', 'Passwordless biometric authentication'),
                    ('Render', 'Cloud hosting with automatic deployments'),
                ]
                for tech, desc in _stack:
                    with ui.row().classes('items-center gap-3').style('padding: 8px 0; border-bottom: 1px solid rgba(128,128,128,0.06);'):
                        ui.element('div').style('width: 6px; height: 6px; border-radius: 50%; background: var(--mf-accent); flex-shrink: 0;')
                        with ui.column().classes('gap-0'):
                            ui.label(tech).classes('text-sm font-bold').style('color: var(--mf-text);')
                            ui.label(desc).classes('text-xs').style('color: var(--mf-muted);')

    shell(content)


# -----------------------------
# Boot
# -----------------------------
def bootstrap() -> None:
    # Safety: Render cold-starts may call ensure_tabs()/get_spreadsheet() before some caches exist.
    # Keep all bootstrap globals initialized so deploys don't crash on NameError.
    g = globals()
    g.setdefault('_ws', None)
    g.setdefault('_gc', None)
    g.setdefault('_ss', None)
    g.setdefault('_tabs_ready', False)
    g.setdefault('_tabs_ready_at', 0.0)
    g.setdefault('_header_cache', {})
    g.setdefault('_migrated_tx_ids', False)
    ensure_tabs()
    # One-time migration: older rows often have the unique id stored in `TxId` while
    # the newer logic edits by `id`. Backfill `id` from `TxId` so Edit works.
    _migrate_transactions_id_column()

bootstrap()

# Premium SVG favicon  "insights" style: zigzag trend + sparkle on dark-emerald bg
_FAVICON_SVG = '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#0F1923"/>
      <stop offset="100%" stop-color="#145038"/>
    </linearGradient>
    <filter id="glow">
      <feGaussianBlur stdDeviation="8" result="blur"/>
      <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
    <filter id="sparkglow">
      <feGaussianBlur stdDeviation="4" result="blur"/>
      <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
  </defs>
  <rect width="512" height="512" rx="108" fill="url(#bg)"/>
  <!-- Trend zigzag line (gold)  the "insights" signature shape -->
  <polyline points="90,340 175,180 280,280 395,110" fill="none" stroke="#FBBF24" stroke-width="14" stroke-linecap="round" stroke-linejoin="round" filter="url(#glow)"/>
  <!-- Node dots -->
  <circle cx="90" cy="340" r="18" fill="#FBBF24"/>
  <circle cx="175" cy="180" r="18" fill="#FBBF24"/>
  <circle cx="280" cy="280" r="18" fill="#FBBF24"/>
  <circle cx="395" cy="110" r="18" fill="#FBBF24"/>
  <!-- White center highlights on dots -->
  <circle cx="87" cy="337" r="6" fill="rgba(255,255,255,0.45)"/>
  <circle cx="172" cy="177" r="6" fill="rgba(255,255,255,0.45)"/>
  <circle cx="277" cy="277" r="6" fill="rgba(255,255,255,0.45)"/>
  <circle cx="392" cy="107" r="6" fill="rgba(255,255,255,0.45)"/>
  <!-- Sparkle star top-right -->
  <g transform="translate(410,70)" filter="url(#sparkglow)">
    <line x1="0" y1="-22" x2="0" y2="22" stroke="#FBBF24" stroke-width="5" stroke-linecap="round"/>
    <line x1="-22" y1="0" x2="22" y2="0" stroke="#FBBF24" stroke-width="5" stroke-linecap="round"/>
    <line x1="-12" y1="-12" x2="12" y2="12" stroke="#FBBF24" stroke-width="3" stroke-linecap="round" opacity="0.6"/>
    <line x1="12" y1="-12" x2="-12" y2="12" stroke="#FBBF24" stroke-width="3" stroke-linecap="round" opacity="0.6"/>
    <circle cx="0" cy="0" r="5" fill="#fff" opacity="0.7"/>
  </g>
  <!-- Small sparkle bottom-left -->
  <g transform="translate(80,400)" filter="url(#sparkglow)" opacity="0.5">
    <line x1="0" y1="-12" x2="0" y2="12" stroke="#FBBF24" stroke-width="3" stroke-linecap="round"/>
    <line x1="-12" y1="0" x2="12" y2="0" stroke="#FBBF24" stroke-width="3" stroke-linecap="round"/>
    <circle cx="0" cy="0" r="3" fill="#fff" opacity="0.6"/>
  </g>
</svg>'''

# ---------------------------
# Self-ping keepalive for Render free tier
# Prevents the service from spinning down after 15 min of inactivity.
# ---------------------------
_RENDER_URL = os.environ.get('RENDER_EXTERNAL_URL', '').strip()

async def _keepalive_loop():
    """Ping our own health endpoint every 5 minutes to prevent Render spin-down."""
    import aiohttp
    target = _RENDER_URL or f'http://localhost:{PORT}'
    ping_url = f'{target}/api/health'
    _logger.info(f'[keepalive] starting self-ping loop  {ping_url}')
    while True:
        await asyncio.sleep(300)  # 5 minutes  aggressive to avoid cold starts
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.get(ping_url) as resp:
                    _logger.info(f'[keepalive] ping {resp.status}')
        except Exception as e:
            # aiohttp may not be installed; fall back to urllib
            try:
                from urllib.request import urlopen
                urlopen(ping_url, timeout=15).read()
                _logger.info('[keepalive] ping ok (urllib)')
            except Exception:
                _logger.warning(f'[keepalive] ping failed: {e}')

@app.get('/api/health')
async def _health_check():
    return {'status': 'ok', 'version': APP_VERSION}

# ---------------------------
# Web App Manifest (improves iOS/Android PWA launch performance)
# ---------------------------
@app.get('/manifest.json')
async def _manifest():
    from starlette.responses import JSONResponse
    return JSONResponse({
        "name": "FinTrackr",
        "short_name": "FinTrackr",
        "description": "Premium personal finance dashboard",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#0F1923",
        "theme_color": "#0F1923",
        "orientation": "portrait-primary",
        "icons": [
            {"src": "/apple-touch-icon.png", "sizes": "180x180", "type": "image/png"},
        ],
    })

# ---------------------------
# Service Worker (caches fonts + static assets for faster repeat launches)
# ---------------------------
@app.get('/sw.js')
async def _service_worker():
    from starlette.responses import Response
    sw_js = f'''
const CACHE_NAME = 'fintrackr-v{APP_VERSION}';
const PRECACHE = [
  'https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap',
  'https://fonts.googleapis.com/icon?family=Material+Icons',
  '/apple-touch-icon.png',
  '/manifest.json',
];

self.addEventListener('install', e => {{
  e.waitUntil(
    caches.open(CACHE_NAME).then(c => c.addAll(PRECACHE)).then(() => self.skipWaiting())
  );
}});

self.addEventListener('activate', e => {{
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
}});

self.addEventListener('fetch', e => {{
  const url = e.request.url;
  // Skip WebSocket, API, and HTML navigation requests
  if (e.request.mode === 'websocket') return;
  if (url.includes('/api/')) return;
  if (e.request.mode === 'navigate') return;

  // Cache-first for fonts, icons, manifest, and NiceGUI static bundles
  const shouldCache = (
    url.includes('fonts.googleapis.com') || url.includes('fonts.gstatic.com') ||
    url.includes('/apple-touch-icon') || url.includes('/manifest.json') ||
    url.includes('/_nicegui/') ||
    url.includes('cdn.jsdelivr.net') ||
    url.endsWith('.js') || url.endsWith('.css') || url.endsWith('.woff2')
  );
  if (shouldCache) {{
    e.respondWith(
      caches.match(e.request).then(cached => {{
        if (cached) return cached;
        return fetch(e.request).then(resp => {{
          if (resp.ok) {{
            const clone = resp.clone();
            caches.open(CACHE_NAME).then(c => c.put(e.request, clone));
          }}
          return resp;
        }}).catch(() => cached);
      }})
    );
  }}
}});
'''
    return Response(content=sw_js, media_type='application/javascript',
                    headers={'Cache-Control': 'no-cache', 'Service-Worker-Allowed': '/'})

async def _prewarm_cache():
    """Pre-load transactions from Google Sheets on startup so first page load is instant."""
    await asyncio.sleep(2)  # let the server finish booting
    try:
        _logger.info('[prewarm] loading transactions into cache ')
        cached_df('transactions')
        _logger.info('[prewarm] transactions cache ready')
    except Exception as e:
        _logger.warning(f'[prewarm] failed: {e}')

app.on_startup(lambda: asyncio.create_task(_keepalive_loop()))
app.on_startup(lambda: asyncio.create_task(_prewarm_cache()))

try:
    _logger.info('[startup] FinTrackr %s starting on port %s', APP_VERSION, PORT)
    ui.run(
        host="0.0.0.0",
        port=PORT,
        show=False,           # headless — Render has no browser
        storage_secret=STORAGE_SECRET or "PLEASE_SET_NICEGUI_STORAGE_SECRET",
        title=APP_TITLE,
        favicon=_FAVICON_SVG,
        reconnect_timeout=15,
    )
except Exception as _startup_err:
    import traceback
    print(f'[FATAL] FinTrackr failed to start: {_startup_err}', flush=True)
    traceback.print_exc()
    raise

# Release: FinTrackr Phase 8.7
# 
# Phase 8.7  COMPLETE dropdown redesign:
#
# Quasar q-select REMOVED from all Add-page dialogs. Replaced with
# custom chip-based selector (`_chip_select`) built from plain HTML
# div elements + CSS. Zero Quasar dependency for selection UI.
#
# 1.  _chip_select()  new Python helper in add_page():
#     - Renders options as clickable styled chips (div + label)
#     - Selected chip gets accent border + tinted background
#     - Exposes .value property (compatible with all save logic)
#     - Supports: label, hint, disabled, scrollable (for many options)
#     - CSS: .mf-chip / .mf-chip.active / .mf-chip.disabled
#     - Compat shims: .props('disable'), .on(event, fn) for existing code
#     - Value setter handles out-of-options text (e.g. split label)
#     - NO q-select, NO q-menu, NO Vue reactivity issues
#
# 2.  Replaced ALL 8 dialog selects across 5 dialogs:
#     - open_add_dialog: Method, Account, Category
#     - open_cc_repay_dialog: Card
#     - open_loc_dialog: Transaction Type
#     - open_invest_dialog: Account, Source (disabled)
#     - open_intl_dialog: Source
#
# 3.  Removed all previous dropdown hacks:
#     - Removed: nuclear CSS wildcard selectors
#     - Removed: 500ms JS polling interval + event listeners
#     - Removed: behavior="menu" / behavior="dialog" props
#     - Removed: input-style / -webkit-text-fill-color hacks
#     - Kept: ui.dark_mode() in shell() (still useful for other components)
#
# 4.  Dropdown jumping eliminated  chips are inline elements,
#     no floating menu positioning needed at all.
#
# Carries forward all 8.6 + 8.5 + 8.4 + 8.3 fixes.
# 
#
# Release: FinTrackr Phase 8.8
# 
# Phase 8.8  Visual cleanup, dialog fixes, UX improvements:
#
# 1.  Custom pure-HTML dropdown for Category (9+ options):
#     - Built from scratch: clickable trigger + scrollable options panel
#     - CSS: .mf-dd-trigger, .mf-dd-panel, .mf-dd-item, .mf-dd-item.active
#     - NO ui.select / q-select used  permanently avoids invisible text bug
#     - _SelDropdown wrapper preserves .value/.props/.on/.set_visibility API
#     - Chips still used for low-count fields (Method, Account, LOC Type, etc.)
#
# 2.  Dialog header layout fixed (all 5 dialogs):
#     - Header outer div: added width: 100%; box-sizing: border-box
#     - Header row: explicit style('width: 100%') for full-width stretch
#     - Header padding: 20px 24px 16px  14px 24px 10px (tighter)
#     - Icon box: 44x44  36x36, icon font: 22px  18px (compact)
#
# 3.  Dialog footer spacing fix:
#     - Removed margin: 8px 0 0 0 from sticky footer in open_add_dialog
#
# 4.  Close button visibility: opacity 0.5  0.7 (all 5 dialogs)
#
# 5.  Home page quick-add buttons now auto-open dialogs:
#     - "Add expense"  navigates to /add AND opens Expense dialog
#     - "Add income"  navigates to /add AND opens Income dialog
#     - Uses app.storage.user['add_auto_open'] + ui.timer(0.3s)
#
# 6.  Desktop logout button added to sidebar (mf-rail):
#     - Red "Logout" button below version label
#     - Visible only on desktop (mf-rail-desktop-only class)
#     - Calls do_logout() (same as mobile logout)
#
# Carries forward all 8.7 + 8.6 + 8.5 + 8.4 + 8.3 fixes.
# 

# 
# Phase 9.0  Major UI overhaul + functional fixes:
#
# UI Enhancements:
#  1. Animated time-based greeting (Good morning/afternoon/evening) in hero card
#  2. Consolidated Financial Pulse card  Income, Expenses, Intl Transfer, Net
#     in a single 2x2 / 4-col responsive grid (replaces 4 separate summary boxes)
#  3. Smart Alerts rendered as slim banners above hero (max 3), not a card
#  4. Page transition animation: .mf-canvas gets 0.25s fade-in + translate
#
# Functional Fixes:
#  6. Payday owner label (Indhu/Abhi/Both) shown next to Next Payday in hero
#  7. Daily average spending added to hero card
#  8. International transfers separated from expenses:
#     - New `intl` variable for type in [international, international transfer, intl]
#     - net = income - expense - invest - intl (same for pay period)
#     - Does not appear in top expenses/merchants (spend df still filters debit/expense)
#  9. Recurring template fix: _header_cache.pop('recurring', None) before
#     both update and append paths in create_or_update_recurring_template()
# 10. Transaction page simplified:
#     - Removed month selector (f_month) and month lock toggle (lock_sw)
#     - Date range defaults to current month start  today
#     - Single consistent date-range filter
#
# Removed from Dashboard:
#  - Pay Period Breakdown card
#  - Upcoming Salary section (Nishanth/Indhu countdown cards)
#  - Top Merchants table
#  - Monthly Insights (_render_insights)
#  - Full-card Smart Alerts (_render_alerts replaced by slim banners)
#
# Carries forward all 8.8 + 8.7 + 8.6 + 8.5 + 8.4 + 8.3 fixes.
#
# ── v9.8.2  ──────────────────────────────────────────────────
# 1. Desktop orientation fix — hero tiles responsive sizing
#    (min(220px, 30vw) × 120px, flex scroll, snap alignment)
# 2. Budget health rings enlarged for desktop:
#    viewBox 180→220, stroke 10→14, SVG max-width 170→210px,
#    legend text-sm with wider column (max 360px)
# 3. Spending Breakdown accent color now user-configurable
#    (reads app.storage.user['spending_breakdown_color']),
#    default changed from violet #a855f7 to blue #3B82F6
# 4. Admin: new "Color Matrix" tile replaces "Budget Ring Colors"
#    - Section 1: Budget Ring color palette (presets + custom)
#    - Section 2: Spending Breakdown accent (8 presets + custom)
#    Each section has live preview
# 5. Spending Breakdown card gets .mf-home-section for full-width on desktop
#
# ── v9.8.3  ──────────────────────────────────────────────────
# 1. Desktop: Budgets + Spending Breakdown side by side (mf-home-2col)
#    flex-row on >900px, stacked column on mobile
# 2. Mobile: Budget widget redesigned as hero-tile-style
#    horizontally scrollable tiles with progress bars
#    (mf-budget-mobile / mf-budget-desktop CSS toggle)
# 3. Color Matrix moved to its own page /color_matrix
#    Admin tile grid now includes Color Matrix tile
#    Removed inline card from admin page
# 4. Budget rendering refactored into _render_budget_widget()
#    Data captured in _budget_rows for deferred rendering
#
# ── v9.8.4  ──────────────────────────────────────────────────
# 1. Desktop: Fixed Budgets + Spending Breakdown equal 50/50 sizing
#    flex: 1 1 0% + max-width: 50% + height: 100% for equal cards
# 2. Mobile: Removed tile-based budget view, rings on all viewports
#    Ring params scaled to fit both (viewBox 180, stroke 12, gap 3)
# 3. Color Matrix notify popups: timeout 1500ms, shorter messages
#    No longer blocks interaction after applying a color
# 4. Spending Breakdown card: removed mf-home-section class
#    (parent mf-home-2col handles layout)
#
# ── v9.9  ────────────────────────────────────────────────────
# 1. Mobile budget widget: ring + legend side-by-side (nowrap),
#    ring uses min(180px, 35vw) so legend fills remaining space
# 2. Category dropdown (Add Expense): larger touch targets,
#    font 15px, padding 13px 18px, full-width items
# 3. Add Income: categories restricted to Salary & Others only
# 4. Dead code cleanup: removed stale placeholder comments,
#    verbose labels, and orphaned version-note lines
#
# ── v9.10  ───────────────────────────────────────────────────
# Major feature release:
#
# 1. Budget widget redesigned to hero-ring style:
#    - Dark glassmorphism background matching cashflow hero
#    - Gradient SVG ring strokes with glow effects
#    - Center text showing overall budget % + total spent
#    - Glassmorphism pill badges for each category
#
# 2. Navigation restructured:
#    - Bottom nav: Tx replaced with Merchants page
#    - Tx (Ledger) moved to hamburger/More menu
#    - Desktop rail updated to match
#
# 3. New Merchants page (/merchants):
#    - 8 merchants: Walmart, Costco, Gill's, Dino's,
#      McDonalds, Tim Hortons, Bombay Spices, Dollarama
#    - Brand logos via Clearbit API with icon fallback
#    - Categorized: Grocery, Restaurants, Specialty, Discount
#    - All-time total spend per merchant
#    - Monthly comparison (current vs previous month %)
#    - International Transfers section with all-time total
#    - Monthly breakdown table for international
#
# 4. Smart Data Upload (/data_upload) replacing Data Tools:
#    - Wide-format spreadsheet: Date, Intl, Credit, Investment,
#      CC Repay, Debit, Reason/Note
#    - Card detection from notes:
#      * Default/Master card → CT Mastercard Grey
#      * Black Cc/Blac card → CT Mastercard Black
#      * RBC VISA → RBC VISA
#      * RBC Mastercard → RBC Mastercard
#      * LOC → RBC Line of Credit
#      * Credit/Investment → Bank method
#    - Category inference via rules engine
#    - Recurring transaction detection (2+ months auto-template)
#    - Replace vs Append mode toggle
#    - Excel (.xlsx) and CSV support
#    - Results summary card after import
#    - Backup download kept for convenience
#    - Legacy /data_tools redirects to /data_upload
#
# 5. Reports Hub enhanced with 4 new charts:
#    - Top Spending Categories (donut chart)
#    - Income vs Expenses Trend (grouped bar)
#    - Spending by Day of Week (avg per day)
#    - Payment Method Distribution (pie chart)
#
# 6. Admin tile: "Data Importer" → "Data Upload" with new icon
#
# ── v9.11  ───────────────────────────────────────────────────
# 1. Budget widget: removed dark glassmorphism background,
#    now uses standard light card (var(--mf-card-top)) with
#    concentric rings and category badges — theme-aware
#
# 2. Merchants page completely rebuilt:
#    - Grid layout (auto-fill 160px) instead of vertical list
#    - Each tile shows THIS MONTH spend (primary), % vs last month
#    - Dino's and Bombay Spices moved to Grocery & Supermarket
#    - Amazon added to Discount & Online (with Dollarama)
#    - Fixed icons: Wikimedia SVG URLs for major brands,
#      Material Icons fallback for local stores
#    - Category headers with appropriate icons
#
# 3. New Fuel & Gas section: tracks all fuel spend,
#    all-time total + this month + % higher/lower than last month
#
# 4. International Transfers: removed monthly breakdown table,
#    now shows % increase/decrease vs last month inline
#
# 5. All-Time Totals section at bottom of Merchants page,
#    sorted by total spend descending
#
# 6. Data Upload rebuilt as single card:
#    - File picker + inline Append/Replace toggle + Restore button
#    - Column format in collapsible expansion
#    - Removed separate Backup & Restore and Upload Mode sections
#    - Same smart logic: card detection, category inference,
#      recurring detection, replace/append modes
#
# ── v9.11.1  ─────────────────────────────────────────────────
# 1. Merchant icons fixed: replaced broken Wikimedia SVG URLs
#    with Google Favicon API (google.com/s2/favicons?domain=&sz=128)
#    — reliable cross-browser loading for all branded merchants
#
# 2. Desktop layout: wider merchant grid (minmax 240px vs 160px),
#    nav sidebar buttons now row layout (icon + label side-by-side)
#    with consistent left alignment
#
# 3. Data Upload restore fixed: switched from .on('upload') to
#    NiceGUI on_upload parameter — file bytes now properly captured
#    via SpooledTemporaryFile.seek(0) + .read()
#
# 4. Smart note scanning: fixed card detection for 'invest' type
#    (was checking 'investment' key that never matched), ensures
#    investment transactions correctly route to Bank method
#
# 5. Budget widget: dark gray glassmorphism background matching
#    hero tile style (not white), rings and content fully centered
#    on mobile with flex centering, white text on dark bg,
#    subtle radial glow accents, drop-shadow on rings
#
# 6. Version bump to 9.11.1
#
# ── v9.11.2  ─────────────────────────────────────────────────
# 1. xlsx import: openpyxl auto-installed on-demand if missing
#    (fixes "Import.openpyxl failed" on Render)
#
# 2. Merchant grid REBUILT: replaced ui.element('div') + manual
#    grid CSS with NiceGUI's native ui.grid(columns=...).
#    Removed .my-card/.column wrapper that was forcing
#    width:100%!important on grid children, collapsing to 1 col.
#    Category cards now use plain div with card-like styling
#    to avoid Quasar class interference.
#
# 3. Version bump to 9.11.2
#
# ── v9.11.3  ─────────────────────────────────────────────────
# 1. Restore data flow FIXED: root cause was Google Sheet uses
#    WIDE format headers (Date, International Transaction,
#    Credit, Debit, Reason/Note) but append_tx wrote LONG format
#    keys (id, date, type, amount) that didn't match sheet
#    headers — append_row silently dropped all unmatched values.
#    Now auto-detects sheet format:
#    - WIDE sheets: writes rows directly using uploaded column
#      names matching sheet headers (pass-through mode)
#    - LONG sheets: parses via append_tx with card detection
#    Filters NaN amounts, validates date column presence.
#
# 2. Restore history note on Data Upload page: after a restore,
#    saves timestamp + filename + row count to user storage.
#    Data Upload page now displays "Last Restore" card showing
#    when restore was done, which file, and how many rows.
#
# 3. Version bump to 9.11.3
#
# ── v9.11.4  ─────────────────────────────────────────────────
# 1. Restore: format detection moved from SHEET headers to
#    UPLOADED FILE columns.  Previous versions checked the
#    sheet's header row, but a prior buggy Replace had
#    overwritten the WIDE headers with TABS defaults (LONG:
#    id, date, type, amount, …).  The file is now the source
#    of truth: if it has 'type' + 'amount' columns → LONG,
#    otherwise → WIDE.
#
# 2. Replace mode now uses write_df_to_sheet() for a single
#    efficient batch write (headers + all data in one API call)
#    instead of clearing first, then row-by-row append.
#    This also restores the correct WIDE headers on the sheet.
#
# 3. Append mode: detects corrupted sheet headers (< 2 column
#    overlap with file) and auto-fixes them by overwriting
#    row 1 with the file's column names before appending.
#
# 4. Both WIDE and LONG files now write data directly (no
#    WIDE→LONG conversion step that was losing rows).
#    NaN values cleaned to '' before write, empty rows dropped.
#
# 5. Version bump to 9.11.4
#
# -- v9.11.5  -------------------------------------------------------
# 1. Restore: added _serialize() to convert ALL cell values BEFORE
#    processing:  pd.Timestamp -> 'YYYY-MM-DD', dt.datetime -> date
#    string, NaN/NaT -> '', float 1234.0 -> '1234', 'nan'/'nat' -> ''.
#    Previous versions passed raw Timestamp objects to append_row /
#    parse_date which caused silent failures and skipped rows.
#
# 2. Removed broken date-validation skip that rejected 71/72 rows.
#    After fillna(''), rows with NaT dates had empty date column
#    excluded from row_dict, so the 'date' key was never found and
#    _has_date stayed False.  Now: all non-empty rows are written.
#
# 3. Append mode: switched from dict-based append_row() (requires
#    header matching) to positional ws.append_row(values_list) using
#    gspread directly.  Values are in file-column order which matches
#    the sheet headers (auto-fixed if corrupted).  This avoids any
#    dict-key vs header-name mismatch.
#
# 4. wide_transactions_to_long() handles categorization on read:
#    maps Credit->type='credit', Debit->'debit', International
#    Transaction->'international', etc.  So writing clean WIDE data
#    to the sheet is sufficient -- the app auto-categorizes on load.
#
# 5. Import summary now shows 'Source rows in file' as the count
#    BEFORE empty-row filtering, and reports header auto-fix status.
#
# 6. Version bump to 9.11.5
#
# -- v9.12  ----------------------------------------------------------
# 1. Add Expense dialog: removed "Save & Next" button and all
#    associated _save_state / save-and-another logic.  Dialog now
#    has just Cancel and Save.  Save always closes the dialog.
#
# 2. Version bump to 9.12
#

