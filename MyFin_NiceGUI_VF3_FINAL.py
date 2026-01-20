"""MYFIN_NICEGUI_P5.1.2.py
Phase 5.1.2 — Bank-style UI Shell (SINGLE FILE, Render-safe)

- NO ui/ folder required
- Uses Render PORT env var if present
- Showcases Phase 5 UI strategy (tokens + shell + primitives + placeholder pages)
"""

from __future__ import annotations

import os
from typing import Callable, Optional, List, Tuple
from nicegui import ui

TOKENS = {
    "color": {
        "bg0": "#070A12",
        "bg1": "#0B1020",
        "surface": "rgba(255,255,255,0.06)",
        "border": "rgba(255,255,255,0.10)",
        "text": "rgba(255,255,255,0.92)",
        "muted": "rgba(255,255,255,0.62)",
        "faint": "rgba(255,255,255,0.42)",
        "accent": "#5B8CFF",
        "accent_soft": "rgba(91,140,255,0.18)",
    },
    "space": {"sm":"10px","md":"14px","lg":"18px","xl":"26px","2xl":"38px"},
    "radius": {"md":"14px","lg":"18px","xl":"22px"},
    "fx": {"blur":"14px","shadow":"0 12px 40px rgba(0,0,0,0.45)","shadow_soft":"0 8px 26px rgba(0,0,0,0.35)"},
    "layout": {"max_w":"1180px","nav_w":"92px","header_h":"64px"},
}

def inject_css() -> None:
    c, s, r, fx, l = TOKENS["color"], TOKENS["space"], TOKENS["radius"], TOKENS["fx"], TOKENS["layout"]
    ui.add_head_html(f"""<style>
    :root {{
      --bg0:{c['bg0']}; --bg1:{c['bg1']}; --surface:{c['surface']}; --border:{c['border']};
      --text:{c['text']}; --muted:{c['muted']}; --faint:{c['faint']};
      --accent:{c['accent']}; --accentSoft:{c['accent_soft']};
      --sm:{s['sm']}; --md:{s['md']}; --lg:{s['lg']}; --xl:{s['xl']}; --2xl:{s['2xl']};
      --rmd:{r['md']}; --rlg:{r['lg']}; --rxl:{r['xl']};
      --blur:{fx['blur']}; --shadow:{fx['shadow']}; --shadowSoft:{fx['shadow_soft']};
      --maxW:{l['max_w']}; --navW:{l['nav_w']}; --headerH:{l['header_h']};
    }}
    body {{
      background:
        radial-gradient(1100px 650px at 18% 10%, rgba(91,140,255,0.22) 0%, rgba(91,140,255,0.00) 55%),
        radial-gradient(900px 600px at 78% 22%, rgba(70,230,166,0.12) 0%, rgba(70,230,166,0.00) 58%),
        linear-gradient(180deg, var(--bg0), var(--bg1));
      color: var(--text);
    }}
    .q-page {{ padding:0 !important; }}
    .mf-shell {{ display:flex; min-height:100vh; }}
    .mf-rail {{ width:var(--navW); position:sticky; top:0; height:100vh; padding:var(--lg); }}
    .mf-rail-card {{
      height:100%; display:flex; flex-direction:column; gap:var(--md);
      border:1px solid var(--border); background:var(--surface);
      backdrop-filter: blur(var(--blur)); -webkit-backdrop-filter: blur(var(--blur));
      border-radius: var(--rlg); box-shadow: var(--shadowSoft); padding: var(--md);
    }}
    .mf-brand {{
      height:44px; display:flex; align-items:center; justify-content:center;
      border-radius: var(--rmd); border:1px solid var(--border);
      background: rgba(255,255,255,0.04); font-weight:900; letter-spacing:0.8px;
    }}
    .mf-navbtn .q-btn__content {{ flex-direction:column !important; gap:6px; }}
    .mf-navbtn {{ width:100%; min-height:58px; border-radius:var(--rmd) !important; border:1px solid transparent !important; }}
    .mf-navbtn.is-active {{ background: var(--accentSoft) !important; border:1px solid rgba(91,140,255,0.35) !important; }}
    .mf-main {{ flex:1; padding: var(--2xl); }}
    .mf-header {{
      height:var(--headerH); display:flex; align-items:center; justify-content:space-between; gap:var(--lg);
      max-width:var(--maxW); margin:0 auto var(--xl) auto;
    }}
    .mf-title .t1 {{ font-size:18px; font-weight:900; }}
    .mf-title .t2 {{ font-size:12px; color:var(--muted); }}
    .mf-canvas {{ max-width:var(--maxW); margin:0 auto; display:flex; flex-direction:column; gap:var(--xl); }}
    .mf-glass {{
      border:1px solid var(--border); background:var(--surface);
      backdrop-filter: blur(var(--blur)); -webkit-backdrop-filter: blur(var(--blur));
      border-radius: var(--rxl); box-shadow: var(--shadow);
    }}
    .mf-card {{ padding: var(--xl); }}
    .mf-hero .h1 {{ font-size:34px; font-weight:950; letter-spacing:-0.6px; line-height:1.05; }}
    .mf-hero .sub {{ color: var(--muted); font-size:13px; }}
    .mf-section-title {{ font-size:13px; letter-spacing:0.18px; color:var(--muted); text-transform:uppercase; }}
    .mf-row {{ display:flex; gap:var(--lg); flex-wrap:wrap; }}
    @media (max-width:900px) {{
      .mf-main {{ padding: var(--xl) var(--lg); }}
      .mf-rail {{ padding: var(--md); }}
      .mf-rail-card {{ padding: var(--sm); border-radius: var(--rmd); }}
      .mf-navbtn .q-btn__content span {{ display:none; }}
      .mf-navbtn {{ min-height:46px; }}
      .mf-hero .h1 {{ font-size:28px; }}
    }}
    </style>""")

def hero(title: str, subtitle: str, right: Optional[Callable[[], None]]=None) -> None:
    with ui.column().classes("mf-glass mf-card mf-hero"):
        with ui.row().classes("w-full items-start justify-between"):
            with ui.column():
                ui.label(title).classes("h1")
                ui.label(subtitle).classes("sub")
            if right:
                right()

def section(title: str, body: Callable[[], None]) -> None:
    with ui.column().classes("mf-glass mf-card"):
        ui.label(title).classes("mf-section-title")
        ui.separator().props("dark").classes("opacity-20 my-2")
        body()

def metric(label: str, value: str, hint: str="") -> None:
    with ui.column().classes("mf-glass").style("padding:16px; border-radius: var(--rlg); box-shadow: var(--shadowSoft); min-width:220px;"):
        ui.label(label).style("color: var(--muted); font-size:12px;")
        ui.label(value).style("font-size:22px; font-weight:950;")
        if hint:
            ui.label(hint).style("color: var(--faint); font-size:12px;")

NAV: List[Tuple[str,str,str]] = [
    ("Dashboard","dashboard","/dashboard"),
    ("Cards","credit_card","/cards"),
    ("Rules","tune","/rules"),
    ("Admin","settings","/admin"),
    ("Transactions","receipt_long","/transactions"),
]

def shell(active_path: str, page_title: str, page_subtitle: str, content: Callable[[], None]) -> None:
    with ui.element("div").classes("mf-shell"):
        with ui.element("div").classes("mf-rail"):
            with ui.element("div").classes("mf-rail-card"):
                ui.label("MYFIN").classes("mf-brand")
                ui.separator().props("dark").classes("opacity-20 my-1")
                for label, icon, href in NAV:
                    btn_cls = "mf-navbtn" + (" is-active" if href == active_path else "")
                    with ui.link(target=href).classes("no-underline w-full"):
                        ui.button(label, icon=icon).props("flat").classes(btn_cls).style("text-transform:none;")
                ui.separator().props("dark").classes("opacity-20 my-1")
                ui.label("Phase 5.1.2").style("color: var(--muted); font-size:11px; text-align:center;")
        with ui.element("main").classes("mf-main"):
            with ui.element("div").classes("mf-header"):
                with ui.element("div").classes("mf-title"):
                    ui.label(page_title).classes("t1")
                    ui.label(page_subtitle).classes("t2")
                with ui.row().classes("items-center gap-2"):
                    ui.button("", icon="search").props("flat round").style("border:1px solid var(--border); background: rgba(255,255,255,0.04);")
                    ui.button("Add", icon="add").props("unelevated").style("background: var(--accent); color:#071022; border-radius:12px; font-weight:950;")
            with ui.element("div").classes("mf-canvas"):
                content()

def boot() -> None:
    inject_css()
    ui.dark_mode().enable()

@ui.page("/")
def _root():
    ui.open("/dashboard")

@ui.page("/dashboard")
def _dashboard():
    def content():
        hero("Am I okay right now?", "Phase 5.1.2 shell live • Phase 5.2 will plug real numbers here",
             right=lambda: ui.button("View details", icon="north_east").props("flat").style("border:1px solid var(--border);"))
        def body():
            with ui.row().classes("mf-row"):
                metric("Available", "$0.00", "Connect Phase 4 data in 5.2")
                metric("Spent (MTD)", "$0.00", "Placeholder")
                metric("Budget health", "—", "Placeholder")
        section("Financial snapshot", body)
    shell("/dashboard", "Dashboard", "Hero-first financial state", content)

@ui.page("/cards")
def _cards():
    def content():
        hero("Cards", "Bank-style widgets • Phase 5.3 will use real balances")
        section("Your cards", lambda: ui.label("Placeholder widgets").style("color: var(--muted);"))
    shell("/cards", "Cards", "Card-as-widget layout", content)

@ui.page("/rules")
def _rules():
    def content():
        hero("Rules", "Correct layout: LEFT list • RIGHT editor (coming in 5.4)")
        section("Rules layout", lambda: ui.label("Placeholder split view").style("color: var(--muted);"))
    shell("/rules", "Rules", "Left list / Right editor", content)

@ui.page("/admin")
def _admin():
    def content():
        hero("Admin", "Control Center layout")
        section("Modules", lambda: ui.label("Placeholder tiles").style("color: var(--muted);"))
    shell("/admin", "Admin", "Productized control center", content)

@ui.page("/transactions")
def _tx():
    def content():
        hero("Transactions", "Clean rows/cards (no tables)")
        section("Recent", lambda: ui.label("Placeholder rows").style("color: var(--muted);"))
    shell("/transactions", "Transactions", "Card rows", content)

boot()
PORT = int(os.environ.get("PORT", "8080"))
ui.run(host="0.0.0.0", port=PORT, title="MyFin — Phase 5.1.2", reload=False)
