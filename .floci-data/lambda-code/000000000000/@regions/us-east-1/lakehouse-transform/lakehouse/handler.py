"""Lambda: bronze (JSON lines) -> silver (valid) + quarantine (rejected).

Invoked by Step Functions with {"bucket": "...", "prefix": "anp/"} or {"bucket": "...", "key": "..."}.
Dedup here is per batch file only; cross-file dedup is on the roadmap.
"""
import json
import os
from datetime import datetime, timezone

from .aws import client
from .schema import canonicalize, to_float
from .validation import validate

SILVER_BUCKET = os.getenv("SILVER_BUCKET", "lakehouse-silver")
QUARANTINE_BUCKET = os.getenv("QUARANTINE_BUCKET", "lakehouse-quarantine")


def process_lines(lines):
    valid, rejected, seen, duplicates = [], [], set(), 0
    for line in lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            rejected.append({"errors": ["invalid_json"], "raw_line": line})
            continue
        event_id = record.get("event_id")
        if event_id in seen:
            duplicates += 1
            continue
        seen.add(event_id)
        row = canonicalize(record.get("raw", {}))
        errors = validate(row)
        if errors:
            rejected.append({"event_id": event_id, "errors": errors, "record": record})
        else:
            row["oil_volume"] = to_float(row["oil_volume"])
            valid.append({"event_id": event_id, "ingested_at": record.get("ingested_at"), **row})
    return valid, rejected, duplicates


def _jsonl(items):
    return ("\n".join(json.dumps(i, ensure_ascii=False) for i in items) + "\n").encode("utf-8")


def _keys(s3, bucket, key=None, prefix=""):
    if key:
        yield key
        return
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def handler(event, context=None):
    s3 = client("s3")
    bucket = event["bucket"]
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    totals = {"valid": 0, "rejected": 0, "duplicates": 0}
    for key in _keys(s3, bucket, event.get("key"), event.get("prefix", "")):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        valid, rejected, duplicates = process_lines(body.splitlines())
        name = key.rsplit("/", 1)[-1]
        # TODO: write Parquet (pyarrow layer or PySpark job) instead of JSON lines
        if valid:
            s3.put_object(Bucket=SILVER_BUCKET, Key=f"anp/dt={day}/{name}.jsonl", Body=_jsonl(valid))
        if rejected:
            s3.put_object(Bucket=QUARANTINE_BUCKET, Key=f"anp/dt={day}/{name}.jsonl", Body=_jsonl(rejected))
        totals["valid"] += len(valid)
        totals["rejected"] += len(rejected)
        totals["duplicates"] += duplicates
    if sum(totals.values()) == 0:
        raise RuntimeError("bronze is empty: no records to process yet")
    return totals