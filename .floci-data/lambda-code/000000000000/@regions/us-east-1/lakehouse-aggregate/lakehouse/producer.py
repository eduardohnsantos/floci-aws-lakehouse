"""Local CLI: replay an ANP CSV into Kinesis from your machine (development and dry-runs).

In the deployed pipeline this job is done by the extraction Lambda (lakehouse.extract).

    PYTHONPATH=src python -m lakehouse.producer --csv data/sample/anp_sample.csv --dry-run
"""
import argparse
import csv
import json
import os
import random
import sys
import time
from itertools import islice

from .events import events_for_row, send


def read_rows(path, sep, encoding):
    with open(path, newline="", encoding=encoding) as fh:
        yield from csv.DictReader(fh, delimiter=sep)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--csv", required=True)
    parser.add_argument("--sep", default=";")
    parser.add_argument("--encoding", default="utf-8-sig")
    parser.add_argument("--stream", default=os.getenv("KINESIS_STREAM", "anp-events"))
    parser.add_argument("--rate", type=float, default=200, help="events per second")
    parser.add_argument("--fault-rate", type=float, default=0.05)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--dry-run", action="store_true", help="print events instead of sending")
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    kinesis = None
    if not args.dry_run:
        from .aws import client

        kinesis = client("kinesis")

    rows = read_rows(args.csv, args.sep, args.encoding)
    if args.limit:
        rows = islice(rows, args.limit)

    sent = 0
    while True:
        chunk = list(islice(rows, args.batch_size))
        if not chunk:
            break
        events = [e for row in chunk for e in events_for_row(row, args.fault_rate, rng)]
        if args.dry_run:
            for event in events:
                print(json.dumps(event, ensure_ascii=False))
        else:
            send(kinesis, args.stream, events)
            time.sleep(len(events) / args.rate)
        sent += len(events)
    print(f"done: {sent} events", file=sys.stderr)


if __name__ == "__main__":
    main()
