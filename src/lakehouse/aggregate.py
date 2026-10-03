"""Lambda: silver (JSON lines) -> gold tables (JSON lines).

Full refresh on every run (idempotent): reads all of silver, then writes two tables:

  well_production/well_production.jsonl   one record per (well, period), latest ingestion wins
  field_monthly/field_monthly.jsonl       per (field, period): total oil, wells, average per well

Each table lives under its own prefix so Glue/Athena can point a table at it later.
TODO: write Parquet instead of JSON lines.
"""
import json
import os
from collections import defaultdict

from .aws import client

SILVER_BUCKET = os.getenv("SILVER_BUCKET", "lakehouse-silver")
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "lakehouse-gold")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "anp/")


def normalize_period(value) -> str:
    """'2021/05' or '05/2021' -> '2021-05'."""
    parts = str(value).strip().replace("/", "-").split("-")
    if len(parts[0]) == 4:
        return f"{parts[0]}-{parts[1]}"
    return f"{parts[1]}-{parts[0]}"


def dedupe_latest(records):
    """One record per (well, period); the most recent ingested_at wins (ties: last seen)."""
    latest = {}
    for rec in records:
        period = normalize_period(rec["period"])
        key = (rec["well"], period)
        current = latest.get(key)
        if current is None or (rec.get("ingested_at") or "") >= (current.get("ingested_at") or ""):
            latest[key] = {**rec, "period": period}
    return sorted(latest.values(), key=lambda r: (r["period"], r["well"]))


def aggregate_fields(wells):
    """Per (field, period): total oil, number of wells and average per well."""
    groups = defaultdict(list)
    for rec in wells:
        groups[(rec.get("field") or "UNKNOWN", rec["period"])].append(rec)
    rows = []
    for (field, period), items in sorted(groups.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        total = sum(float(i["oil_volume"]) for i in items)
        rows.append(
            {
                "field": field,
                "period": period,
                "total_oil": round(total, 4),
                "wells": len(items),
                "avg_oil_per_well": round(total / len(items), 4),
            }
        )
    return rows


def _jsonl(items):
    return ("\n".join(json.dumps(i, ensure_ascii=False) for i in items) + "\n").encode("utf-8")


def read_silver(s3, bucket, prefix):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8")
            for line in body.splitlines():
                if line.strip():
                    yield json.loads(line)


def handler(event, context=None):
    s3 = client("s3")
    records = list(read_silver(s3, SILVER_BUCKET, SILVER_PREFIX))
    if not records:
        raise RuntimeError("silver is empty: nothing to aggregate")
    wells = dedupe_latest(records)
    fields = aggregate_fields(wells)
    s3.put_object(Bucket=GOLD_BUCKET, Key="well_production/well_production.jsonl", Body=_jsonl(wells))
    s3.put_object(Bucket=GOLD_BUCKET, Key="field_monthly/field_monthly.jsonl", Body=_jsonl(fields))
    return {"silver_records": len(records), "wells": len(wells), "field_rows": len(fields)}
