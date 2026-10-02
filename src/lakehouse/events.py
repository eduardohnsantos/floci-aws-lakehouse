"""Event building, fault injection and Kinesis publishing (shared by the Lambda and the local CLI)."""
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone

from .schema import raw_key_for

FAULTS = {
    "missing_field": ("well", ""),
    "bad_number": ("oil_volume", "n/a"),
    "negative": ("oil_volume", "-1"),
}


def make_event(row, source="anp"):
    fingerprint = hashlib.sha256(
        json.dumps(row, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return {
        "event_id": fingerprint[:32],
        "source": source,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "raw": row,
    }


def events_for_row(row, fault_rate, rng):
    """Events to send for one CSV row: normally one; faults may duplicate, delay or corrupt it."""
    if rng.random() >= fault_rate:
        return [make_event(row)]
    kind = rng.choice(["duplicate", "late", *FAULTS])
    if kind == "duplicate":
        event = make_event(row)
        return [event, event]
    if kind == "late":
        event = make_event(row)
        event["ingested_at"] = (
            datetime.now(timezone.utc) - timedelta(days=rng.randint(1, 7))
        ).isoformat()
        return [event]
    canonical, bad_value = FAULTS[kind]
    key = raw_key_for(row, canonical)
    if key is None:
        return [make_event(row)]
    corrupted = dict(row)
    corrupted[key] = bad_value
    return [make_event(corrupted)]


def send(kinesis, stream, events):
    for start in range(0, len(events), 500):  # PutRecords limit
        batch = events[start:start + 500]
        response = kinesis.put_records(
            StreamName=stream,
            Records=[
                {
                    "Data": (json.dumps(e, ensure_ascii=False) + "\n").encode("utf-8"),
                    "PartitionKey": e["event_id"],
                }
                for e in batch
            ],
        )
        if response.get("FailedRecordCount"):
            print(f"warning: {response['FailedRecordCount']} records failed", file=sys.stderr)
