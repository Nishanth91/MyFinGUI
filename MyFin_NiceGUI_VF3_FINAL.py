
"""
MYFIN_NICEGUI_P5.1.py
Phase 5.1 — UI Foundation & Bank-Style App Shell (Option A)

This file intentionally contains:
- NO business logic changes
- NO data model changes
- ONLY UI foundation + shell + placeholders

This becomes the baseline for Phase 5.2+
"""

from __future__ import annotations
from nicegui import ui

# ---- UI FOUNDATION ----
from ui.ui_tokens import inject_global_css
from ui.ui_shell import render_shell
from ui.ui_components import (
    HeroCard, SectionCard, MetricTile,
    UsageBar, Chip, SplitView, ControlTile
)


def _boot():
    inject_global_css()
    ui.dark_mode().enable()


@ui.page('/')
def root():
    ui.open('/dashboard')


@ui.page('/dashboard')
def dashboard():
    def content():
        HeroCard(
            title="Financial State",
            subtitle="Phase 5.1 — UI foundation only",
            right_slot=lambda: ui.button(
                'Details',
                icon='north_east'
            ).props('flat').style(
                'border: 1px solid var(--border);'
            )
        )

        def metrics():
            with ui.row().classes('mf-row'):
                MetricTile("Available", "$0.00", "Logic connects in 5.2")
                MetricTile("Spent (MTD)", "$0.00", "Placeholder")
                MetricTile("Budget Health", "—", "Placeholder")

        SectionCard("Quick metrics", metrics)

    render_shell(
        active_path="/dashboard",
        page_title="Dashboard",
        page_subtitle="Hero-first overview",
        content_builder=content,
    )


@ui.page('/cards')
def cards():
    def content():
        HeroCard("Cards", "Bank-style widgets land in Phase 5.3")

        def body():
            with ui.row().classes('mf-row'):
                for name in ["CT Grey", "RBC Card"]:
                    with ui.column().classes('mf-glass').style(
                        'padding: 16px; border-radius: var(--rxl); min-width: 280px; flex:1;'
                    ):
                        ui.label(name).style('font-weight: 800;')
                        ui.label("$0.00 available").style('color: var(--muted);')
                        UsageBar(used=40, total=100, label="Utilization")

        SectionCard("Your cards", body)

    render_shell(
        active_path="/cards",
        page_title="Cards",
        page_subtitle="Card-as-widget design",
        content_builder=content,
    )


@ui.page('/rules')
def rules():
    def content():
        HeroCard("Rules", "Layout fixed in Phase 5.1")

        def left():
            ui.label("Rule list").style('font-weight: 800;')
            ui.input(placeholder="Search rules").props('dense outlined')
            ui.separator().props('dark').classes('opacity-20 my-2')
            for r in ["Costco → Groceries", "Shell → Fuel"]:
                ui.label(r)

        def right():
            ui.label("Rule editor").style('font-weight: 800;')
            with ui.row().classes('gap-2'):
                Chip("costco")
                Chip("wholesale")
            ui.input("Category", value="Groceries").props('dense outlined')

        SplitView(left, right)

    render_shell(
        active_path="/rules",
        page_title="Rules",
        page_subtitle="Left list / Right editor",
        content_builder=content,
    )


@ui.page('/admin')
def admin():
    def content():
        HeroCard("Admin", "Control Center")

        def body():
            with ui.row().classes('mf-row'):
                ControlTile("tune", "Rules", "Manage rules", "/rules")
                ControlTile("savings", "Budgets", "Manage budgets", "/admin")
                ControlTile("autorenew", "Recurrence", "Templates", "/admin")

        SectionCard("Modules", body)

    render_shell(
        active_path="/admin",
        page_title="Admin",
        page_subtitle="Productized control center",
        content_builder=content,
    )


@ui.page('/transactions')
def transactions():
    def content():
        HeroCard("Transactions", "Clean rows (no tables)")

        def body():
            for i in range(3):
                with ui.row().classes(
                    'items-center justify-between'
                ).style(
                    'padding: 12px; border-radius: 14px; '
                    'border: 1px solid rgba(255,255,255,0.10); '
                    'background: rgba(255,255,255,0.04);'
                ):
                    ui.label(f"Merchant {i+1}")
                    ui.label("-$0.00").style('font-weight: 900;')

        SectionCard("Recent activity", body)

    render_shell(
        active_path="/transactions",
        page_title="Transactions",
        page_subtitle="Mobile-first later",
        content_builder=content,
    )


_boot()
ui.run(host="0.0.0.0", port=8080, title="MyFin — Phase 5.1")
