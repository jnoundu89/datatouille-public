#!/usr/bin/env python3
"""Smoke-test every Grafana dashboard panel's SQL against the live Postgres.

Walks `monitoring/grafana/dashboards/**/*.json`, collects every `rawSql` that
targets a postgres datasource, wraps each one in a read-only transaction and
executes it. Categorizes outcomes so broken panels can't hide inside noisy
"template variable" failures.

Categories:
    OK             — query executed, returned rows (or 0 rows but no error)
    EMPTY_OK       — query executed but the relevant condition returned 0 rows;
                     same as OK for SQL health, separate bucket for Grafana UX
    TEMPLATE       — contains unresolved Grafana macros ($__timeFilter, ${var}…);
                     can't evaluate offline without a full Grafana render
    RELATION_MISSING — Postgres complained about a missing table/column. Needs
                       a code fix: the dashboard refers to something that no
                       longer exists.
    SYNTAX_ERROR   — malformed SQL (should not happen after this PR lands)
    OTHER_ERROR    — unexpected Postgres error (permission, type mismatch, etc.)

Exit code is the number of panels in categories worse than TEMPLATE
(RELATION_MISSING + SYNTAX_ERROR + OTHER_ERROR). Wire into CI / `make ci` to
prevent silent dashboard regressions when a source table is renamed or a
column disappears.
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import errors as pg_errors

# Path works in two invocation modes:
#   * from host: scripts/ is at the repo root, dashboards under monitoring/…
#   * from inside airflow-scheduler: dashboards mounted at /opt/airflow/grafana-dashboards
_CANDIDATES = (
    Path(__file__).resolve().parents[1] / "monitoring" / "grafana" / "dashboards",
    Path("/opt/airflow/grafana-dashboards"),
)
DASHBOARDS_ROOT = next((p for p in _CANDIDATES if p.exists()), _CANDIDATES[0])

GRAFANA_MACRO_RE = re.compile(r"\$__\w+|\$\{[^}]+\}")


@dataclass
class PanelResult:
    dashboard: str
    panel_id: str
    title: str
    status: str
    detail: str = ""


def pg_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )


def walk_panels(obj: Any, panels: list[tuple[str, str, str]], title: str = "") -> None:
    """Collect (panel_refId, panel_title, rawSql) tuples from a dashboard tree."""
    if isinstance(obj, dict):
        if "rawSql" in obj and isinstance(obj["rawSql"], str):
            panels.append((str(obj.get("refId", "?")), title, obj["rawSql"]))
        nested_title = obj.get("title", title)
        for val in obj.values():
            walk_panels(val, panels, nested_title)
    elif isinstance(obj, list):
        for item in obj:
            walk_panels(item, panels, title)


def categorize_error(exc: Exception) -> tuple[str, str]:
    msg = str(exc).strip().split("\n")[0]
    if isinstance(exc, pg_errors.UndefinedTable | pg_errors.UndefinedColumn):
        return "RELATION_MISSING", msg
    if isinstance(exc, pg_errors.SyntaxError):
        return "SYNTAX_ERROR", msg
    return "OTHER_ERROR", msg


def check_panel(conn, sql: str) -> tuple[str, str]:
    if GRAFANA_MACRO_RE.search(sql):
        return "TEMPLATE", "unresolved Grafana macro/variable"
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            try:
                rows = cur.fetchall()
            except psycopg2.ProgrammingError:
                rows = []
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            return categorize_error(exc)
        else:
            conn.rollback()
            return ("OK" if rows else "EMPTY_OK", f"{len(rows)} rows")


def main() -> int:
    if not DASHBOARDS_ROOT.exists():
        print(f"ERROR: {DASHBOARDS_ROOT} not found")
        return 2

    results: list[PanelResult] = []
    with pg_connect() as conn:
        conn.autocommit = False
        for dashboard_path in sorted(DASHBOARDS_ROOT.rglob("*.json")):
            try:
                data = json.loads(dashboard_path.read_text())
            except json.JSONDecodeError as exc:
                results.append(
                    PanelResult(
                        dashboard=str(dashboard_path.relative_to(DASHBOARDS_ROOT)),
                        panel_id="-",
                        title="(JSON parse error)",
                        status="OTHER_ERROR",
                        detail=str(exc),
                    )
                )
                continue
            panels: list[tuple[str, str, str]] = []
            walk_panels(data, panels)
            for panel_id, title, sql in panels:
                status, detail = check_panel(conn, sql)
                results.append(
                    PanelResult(
                        dashboard=str(dashboard_path.relative_to(DASHBOARDS_ROOT)),
                        panel_id=panel_id,
                        title=title or "(no title)",
                        status=status,
                        detail=detail,
                    )
                )

    tally: dict[str, int] = defaultdict(int)
    for r in results:
        tally[r.status] += 1

    print("## Grafana panel smoke test\n")
    print(f"Scanned {len({r.dashboard for r in results})} dashboards, {len(results)} panels total.\n")
    print("| status | count |")
    print("|---|---:|")
    for status in ("OK", "EMPTY_OK", "TEMPLATE", "RELATION_MISSING", "SYNTAX_ERROR", "OTHER_ERROR"):
        print(f"| {status} | {tally[status]} |")

    hard_fail = tally["RELATION_MISSING"] + tally["SYNTAX_ERROR"] + tally["OTHER_ERROR"]
    if hard_fail:
        print("\n### Broken panels (fix or delete):\n")
        print("| dashboard | panel | status | error |")
        print("|---|---|---|---|")
        for r in results:
            if r.status in ("RELATION_MISSING", "SYNTAX_ERROR", "OTHER_ERROR"):
                err = r.detail[:80].replace("|", "\\|")
                print(f"| {r.dashboard} | {r.title} ({r.panel_id}) | {r.status} | {err} |")
    else:
        print("\nNo broken panels. OK")

    return hard_fail


if __name__ == "__main__":
    sys.exit(main())
