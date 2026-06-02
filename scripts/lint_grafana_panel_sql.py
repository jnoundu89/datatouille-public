#!/usr/bin/env python3
"""Static-analysis linter for Grafana panel rawSql fields.

Catches a small set of mistakes that a live smoke test would also catch —
but this runs in CI without a live Postgres, so regressions are caught
before merge instead of at dashboard-render time:

* Single quotes inside hardcoded identifiers (e.g. 'Baldur's Gate 3')
  — must be dollar-quoted ($...$) or escaped ('').
* pg_stat_user_tables queried against a schema that only contains views
  (dbt_*_staging) — n_live_tup is 0 for views.

Exit code 1 on any finding so CI fails the job.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

DASHBOARDS_DIR = Path(__file__).parent.parent / "monitoring" / "grafana" / "dashboards"

# Schemas known to contain only views (dbt staging). pg_stat_user_tables does
# not track views, so n_live_tup returns 0 rows → the panel is silently empty.
VIEW_ONLY_SCHEMAS = {"dbt_docker_staging", "dbt_dev_staging"}

# Literal "X's Y" pattern inside single-quoted strings — the inner apostrophe
# has to be either escaped ('') or dollar-quoted ($tag$...$tag$). We flag only
# the single-quoted form, since that's what breaks the SQL.
BAD_APOSTROPHE = re.compile(r"'[^']*[a-zA-Z]'[a-zA-Z]")

# Strip PostgreSQL dollar-quoted strings ($tag$...$tag$, including $$...$$)
# before the apostrophe check — apostrophes inside dollar quotes are legal.
DOLLAR_QUOTED = re.compile(r"\$(\w*)\$.*?\$\1\$", re.DOTALL)


def _iter_raw_sql(dashboard: dict) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    def walk(node: object, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "rawSql" and isinstance(value, str):
                    out.append((path, value))
                else:
                    walk(value, f"{path}.{key}")
        elif isinstance(node, list):
            for i, item in enumerate(node):
                walk(item, f"{path}[{i}]")

    walk(dashboard, "$")
    return out


def lint_dashboard(path: Path) -> list[str]:
    try:
        dashboard = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"invalid JSON: {exc}"]

    findings: list[str] = []
    for loc, sql in _iter_raw_sql(dashboard):
        stripped = DOLLAR_QUOTED.sub("", sql)
        if BAD_APOSTROPHE.search(stripped):
            findings.append(
                f"{loc}: unescaped apostrophe inside a quoted literal — "
                f"dollar-quote (`$...$`) or double up the quote (`''`): {sql[:120]}..."
            )
        for schema in VIEW_ONLY_SCHEMAS:
            if "pg_stat_user_tables" in sql and f"'{schema}'" in sql:
                findings.append(
                    f"{loc}: pg_stat_user_tables queried against {schema!r} "
                    f"(views-only schema); use information_schema.views + "
                    f"query_to_xml for live row counts"
                )
    return findings


def main() -> int:
    if not DASHBOARDS_DIR.exists():
        DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)
    failures: dict[str, list[str]] = {}
    for path in sorted(DASHBOARDS_DIR.rglob("*.json")):
        findings = lint_dashboard(path)
        if findings:
            failures[str(path.relative_to(DASHBOARDS_DIR.parent.parent))] = findings

    if not failures:
        total = sum(1 for _ in DASHBOARDS_DIR.rglob("*.json"))
        print(f"OK: {total} dashboards, 0 SQL findings")
        return 0

    for rel, findings in failures.items():
        print(f"\n{rel}")
        for f in findings:
            print(f"  - {f}")
    print(f"\n{sum(len(v) for v in failures.values())} SQL finding(s) across {len(failures)} dashboard(s)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
