#!/usr/bin/env python3
"""Detect dashboard panels whose SQL executes but returns mostly-null data.

`check_grafana_panels.py` already catches panels whose SQL errors out or
returns 0 rows. It doesn't catch the more common quiet failure: the query
runs, returns hundreds of rows, but one or more output columns are NULL
for almost every row — so the Grafana table/card shows "Surface: -" for
every listing, even though the SQL is technically "OK".

This script fills that gap by running each `rawSql` against a live
Postgres and measuring per-column NULL density on the returned sample.

Two new buckets:

* SPARSE_COLUMNS — the query returned ≥ `--min-rows` rows AND at least one
  output column is NULL for more than `--max-null-rate` of them.
* EMPTY_KPI     — the query returned a single row with a single column
  whose value is 0 / NULL / empty string (so the "stat" panel shows nothing).

Intentional by default (e.g. `COALESCE(x, '-')` yields "-", which isn't
NULL — the heuristic tolerates it). Configure thresholds via CLI.

Exit code = number of sparse panels. Wire into CI with a grace threshold
once the current fleet is clean.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import errors as pg_errors

_CANDIDATES = (
    Path(__file__).resolve().parents[1] / "monitoring" / "grafana" / "dashboards",
    Path("/opt/airflow/grafana-dashboards"),
)
DASHBOARDS_ROOT = next((p for p in _CANDIDATES if p.exists()), _CANDIDATES[0])

GRAFANA_MACRO_RE = re.compile(r"\$__\w+|\$\{[^}]+\}")
# Column names that are expected to be sparse/nullable and should NOT trip
# the sparse-column heuristic (e.g. COALESCE fallback labels, row markers).
# Case-insensitive match on the output alias.
SPARSE_TOLERATED_COLUMNS = {"score", "points", "url", "note", "rating"}


@dataclass
class PanelResult:
    dashboard: str
    panel_id: str
    title: str
    status: str
    rows: int = 0
    sparse: list[dict[str, Any]] = field(default_factory=list)
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
    """Collect (refId, panel_title, rawSql) tuples from a dashboard tree."""
    if isinstance(obj, dict):
        if "rawSql" in obj and isinstance(obj["rawSql"], str):
            panels.append((str(obj.get("refId", "?")), title, obj["rawSql"]))
        nested_title = obj.get("title", title)
        for val in obj.values():
            walk_panels(val, panels, nested_title)
    elif isinstance(obj, list):
        for item in obj:
            walk_panels(item, panels, title)


def _is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() in ("", "-"):
        return True
    if isinstance(value, (int, float)) and value == 0:
        return True
    return False


def _analyze_rows(
    cur,
    rows: list[tuple],
    min_rows: int,
    max_null_rate: float,
) -> tuple[str, list[dict[str, Any]]]:
    col_names = [c.name for c in cur.description] if cur.description else []
    row_count = len(rows)

    # EMPTY_KPI: single-row, single-column panel that rendered to nothing.
    if row_count == 1 and len(col_names) == 1:
        value = rows[0][0]
        if _is_empty_value(value):
            return "EMPTY_KPI", [{"column": col_names[0], "value": str(value)}]

    if row_count < min_rows:
        return ("OK" if row_count else "EMPTY_OK", [])

    sparse: list[dict[str, Any]] = []
    for idx, name in enumerate(col_names):
        if name.lower() in SPARSE_TOLERATED_COLUMNS:
            continue
        null_count = sum(1 for row in rows if _is_empty_value(row[idx]))
        rate = null_count / row_count
        if rate > max_null_rate:
            sparse.append(
                {
                    "column": name,
                    "null_rate": round(rate, 3),
                    "rows_analyzed": row_count,
                }
            )

    return ("SPARSE_COLUMNS" if sparse else "OK", sparse)


def check_panel(
    conn,
    sql: str,
    min_rows: int,
    max_null_rate: float,
) -> PanelResult:
    if GRAFANA_MACRO_RE.search(sql):
        return PanelResult(dashboard="", panel_id="", title="", status="TEMPLATE", detail="unresolved macro/var")
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            try:
                rows = cur.fetchall()
            except psycopg2.ProgrammingError:
                rows = []
        except (pg_errors.UndefinedTable, pg_errors.UndefinedColumn) as exc:
            conn.rollback()
            return PanelResult("", "", "", "RELATION_MISSING", detail=str(exc).splitlines()[0])
        except pg_errors.SyntaxError as exc:
            conn.rollback()
            return PanelResult("", "", "", "SYNTAX_ERROR", detail=str(exc).splitlines()[0])
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            return PanelResult("", "", "", "OTHER_ERROR", detail=str(exc).splitlines()[0])
        status, sparse = _analyze_rows(cur, rows, min_rows, max_null_rate)
        conn.rollback()
        return PanelResult("", "", "", status, rows=len(rows), sparse=sparse)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--min-rows", type=int, default=10, help="Skip panels returning fewer rows")
    parser.add_argument(
        "--max-null-rate",
        type=float,
        default=0.9,
        help="Flag columns whose NULL/empty rate exceeds this fraction",
    )
    parser.add_argument("--json", type=Path, help="Also write full results to this JSON file")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when any SPARSE_COLUMNS or EMPTY_KPI panel is found (CI mode)",
    )
    args = parser.parse_args()

    if not DASHBOARDS_ROOT.exists():
        print(f"ERROR: {DASHBOARDS_ROOT} not found", file=sys.stderr)
        return 2

    results: list[PanelResult] = []
    with pg_connect() as conn:
        conn.autocommit = False
        for dashboard_path in sorted(DASHBOARDS_ROOT.rglob("*.json")):
            try:
                data = json.loads(dashboard_path.read_text(encoding="utf-8"))
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
                res = check_panel(conn, sql, args.min_rows, args.max_null_rate)
                res.dashboard = str(dashboard_path.relative_to(DASHBOARDS_ROOT))
                res.panel_id = panel_id
                res.title = title or "(no title)"
                results.append(res)

    tally: dict[str, int] = defaultdict(int)
    for r in results:
        tally[r.status] += 1

    print("## Grafana panel column-density scan\n")
    print(
        f"Scanned {len({r.dashboard for r in results})} dashboards, "
        f"{len(results)} panels (min_rows={args.min_rows}, max_null_rate={args.max_null_rate}).\n"
    )
    print("| status | count |")
    print("|---|---:|")
    for status in (
        "OK",
        "EMPTY_OK",
        "SPARSE_COLUMNS",
        "EMPTY_KPI",
        "TEMPLATE",
        "RELATION_MISSING",
        "SYNTAX_ERROR",
        "OTHER_ERROR",
    ):
        print(f"| {status} | {tally[status]} |")

    flagged = [r for r in results if r.status in ("SPARSE_COLUMNS", "EMPTY_KPI")]
    if flagged:
        print(f"\n### {len(flagged)} panels with density issues\n")
        print("| dashboard | panel | rows | finding |")
        print("|---|---|---:|---|")
        for r in flagged:
            if r.status == "SPARSE_COLUMNS":
                detail = ", ".join(f"{c['column']}({int(c['null_rate'] * 100)}% NULL)" for c in r.sparse)
            else:
                detail = f"KPI empty: {r.sparse[0]['column']}={r.sparse[0]['value']}"
            title = r.title.replace("|", "\\|")
            print(f"| {r.dashboard} | {title} | {r.rows} | {detail} |")

    if args.json:
        args.json.write_text(
            json.dumps({"tally": tally, "results": [asdict(r) for r in results]}, indent=2),
            encoding="utf-8",
        )
        print(f"\nWrote full report to {args.json}")

    if args.strict:
        return len(flagged)
    return 0


if __name__ == "__main__":
    sys.exit(main())
