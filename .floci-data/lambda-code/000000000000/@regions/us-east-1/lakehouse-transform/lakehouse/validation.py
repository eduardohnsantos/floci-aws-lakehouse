"""Data-quality rules applied between bronze and silver."""
import re

from .schema import NUMERIC, REQUIRED, to_float

PERIOD_RE = re.compile(r"^(\d{4}[/-]\d{2}|\d{2}[/-]\d{4})$")


def validate(row: dict) -> list:
    errors = []
    for field in REQUIRED:
        if row.get(field) in (None, ""):
            errors.append(f"missing_{field}")
    period = row.get("period")
    if period and not PERIOD_RE.match(str(period)):
        errors.append("invalid_period")
    for field in NUMERIC:
        if row.get(field) in (None, ""):
            continue
        number = to_float(row[field])
        if number is None:
            errors.append(f"invalid_{field}")
        elif number < 0:
            errors.append(f"negative_{field}")
    return errors
