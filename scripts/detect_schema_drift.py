"""Detect schema drift: block CREATE TABLE / ALTER TABLE ADD COLUMN in DAGs.

Schemas must be defined in sql/init/ only. DAGs should never contain inline DDL.
Exit code 1 if violations found, 0 otherwise.
"""

import re
import sys
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"

PATTERNS = [
    (re.compile(r"CREATE\s+TABLE", re.IGNORECASE), "CREATE TABLE"),
    (re.compile(r"ALTER\s+TABLE\s+.*ADD\s+COLUMN", re.IGNORECASE), "ALTER TABLE ADD COLUMN"),
]


def scan() -> list[str]:
    violations = []
    for py_file in sorted(DAGS_DIR.rglob("*.py")):
        for line_no, line in enumerate(py_file.read_text(encoding="utf-8").splitlines(), 1):
            for pattern, label in PATTERNS:
                if pattern.search(line):
                    rel = py_file.relative_to(DAGS_DIR.parent)
                    violations.append(f"{rel}:{line_no}  {label}")
    return violations


def main() -> int:
    violations = scan()
    if violations:
        print(f"Schema drift detected! {len(violations)} violation(s):")
        for v in violations:
            print(f"  {v}")
        print("\nSchemas must live in sql/init/ — not in DAG code.")
        return 1
    print("No schema drift detected.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
