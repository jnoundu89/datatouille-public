#!/usr/bin/env python3
"""Validate Grafana dashboard JSON files.

Checks:
- UID length <= 40 characters (Grafana limit)
- UID uniqueness across all dashboards
- Required fields presence
- Datasource UID references
"""

import json
import sys
from pathlib import Path

GRAFANA_UID_MAX_LENGTH = 40
DASHBOARDS_DIR = Path(__file__).parent.parent / "monitoring" / "grafana" / "dashboards"
VALID_DATASOURCE_UIDS = {"postgres", "PBFA97CFB590B2093", "-- Grafana --"}


def validate_dashboard(file_path: Path) -> list[str]:
    """Validate a single dashboard file. Returns list of errors."""
    errors = []

    try:
        with open(file_path, encoding="utf-8") as f:
            dashboard = json.load(f)
    except json.JSONDecodeError as e:
        return [f"Invalid JSON: {e}"]

    # Check UID
    uid = dashboard.get("uid")
    if not uid:
        errors.append("Missing 'uid' field")
    elif len(uid) > GRAFANA_UID_MAX_LENGTH:
        errors.append(f"UID '{uid}' is {len(uid)} chars, max is {GRAFANA_UID_MAX_LENGTH}")

    # Check title
    if not dashboard.get("title"):
        errors.append("Missing 'title' field")

    # Check panels datasource references
    for panel in dashboard.get("panels", []):
        ds = panel.get("datasource", {})
        if isinstance(ds, dict):
            ds_uid = ds.get("uid")
            if ds_uid and ds_uid not in VALID_DATASOURCE_UIDS:
                errors.append(f"Panel '{panel.get('title', 'unknown')}' uses unknown datasource UID '{ds_uid}'")

    return errors


def check_uid_uniqueness(dashboards: dict[Path, dict]) -> list[str]:
    """Check that all UIDs are unique across dashboards."""
    errors = []
    uid_to_file: dict[str, Path] = {}

    for file_path, dashboard in dashboards.items():
        uid = dashboard.get("uid")
        if uid:
            if uid in uid_to_file:
                errors.append(f"Duplicate UID '{uid}' in {file_path.name} and {uid_to_file[uid].name}")
            else:
                uid_to_file[uid] = file_path

    return errors


def main() -> int:
    """Validate all Grafana dashboards."""
    if not DASHBOARDS_DIR.exists():
        DASHBOARDS_DIR.mkdir(parents=True, exist_ok=True)

    dashboard_files = list(DASHBOARDS_DIR.rglob("*.json"))
    if not dashboard_files:
        print("No dashboard files found")
        return 0

    all_errors: dict[Path, list[str]] = {}
    dashboards: dict[Path, dict] = {}

    # Validate each dashboard
    for file_path in dashboard_files:
        errors = validate_dashboard(file_path)
        if errors:
            all_errors[file_path] = errors

        # Load for uniqueness check
        try:
            with open(file_path, encoding="utf-8") as f:
                dashboards[file_path] = json.load(f)
        except json.JSONDecodeError:
            pass

    # Check UID uniqueness
    uniqueness_errors = check_uid_uniqueness(dashboards)
    if uniqueness_errors:
        all_errors[Path("GLOBAL")] = uniqueness_errors

    # Report results
    if all_errors:
        print("Grafana Dashboard Validation FAILED:\n")
        for file_path, errors in all_errors.items():
            print(f"  {file_path.name}:")
            for error in errors:
                print(f"    - {error}")
        print()
        return 1

    print(f"OK: {len(dashboard_files)} Grafana dashboards validated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
