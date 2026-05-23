#!/usr/bin/env python3
"""Profile companion raw CSV distribution.

Input rows are `vid,loc,unix_ts`. The tool is intentionally streaming for row
ingest, then keeps exact counters for the requested top-N distribution tables.
It is meant for mini/1d/7d profiling; for 31d, run it on a sampled stream or on
the cluster host with enough memory.
"""

import argparse
import csv
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_T0 = 1420041600
DEFAULT_SLOT_SIZE = 300


def parse_args():
    parser = argparse.ArgumentParser(description="Profile raw companion CSV")
    parser.add_argument("--input", required=True, help="CSV file path, or '-' for stdin")
    parser.add_argument("--output-dir", required=True, help="Directory for profile outputs")
    parser.add_argument("--top", type=int, default=50, help="Rows in top distribution CSVs")
    parser.add_argument("--t0", type=int, default=DEFAULT_T0, help="Reference timestamp")
    parser.add_argument("--slot-size", type=int, default=DEFAULT_SLOT_SIZE, help="Slot width in seconds")
    return parser.parse_args()


def open_input(path):
    if path == "-":
        return sys.stdin
    return open(path, "r", encoding="utf-8", newline="")


def parse_row(line):
    first = line.find(",")
    if first < 0:
        return None
    second = line.find(",", first + 1)
    if second < 0 or line.find(",", second + 1) >= 0:
        return None
    try:
        vid = int(line[:first].strip())
        loc = int(line[first + 1:second].strip())
        ts = int(line[second + 1:].strip())
    except ValueError:
        return None
    if vid < 0 or loc < 0 or ts < 0:
        return None
    return vid, loc, ts


def day_key(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def write_counter_csv(path, header, rows):
    with open(path, "w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(header)
        writer.writerows(rows)


def main():
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    vid_counts = Counter()
    loc_counts = Counter()
    loc_slot_counts = Counter()
    day_counts = Counter()
    raw_records = 0
    parse_fail = 0
    min_ts = None
    max_ts = None

    with open_input(args.input) as src:
        for line in src:
            raw_records += 1
            parsed = parse_row(line)
            if parsed is None:
                parse_fail += 1
                continue
            vid, loc, ts = parsed
            t_norm = ts - args.t0
            if t_norm < 0 or t_norm > 2_147_483_647:
                parse_fail += 1
                continue
            slot = math.floor(t_norm / args.slot_size)
            vid_counts[vid] += 1
            loc_counts[loc] += 1
            loc_slot_counts[(loc, slot)] += 1
            day_counts[day_key(ts)] += 1
            min_ts = ts if min_ts is None else min(min_ts, ts)
            max_ts = ts if max_ts is None else max(max_ts, ts)

    valid_records = sum(vid_counts.values())
    singleton_vids = sum(1 for count in vid_counts.values() if count == 1)
    summary = {
        "raw_records": raw_records,
        "valid_records": valid_records,
        "parse_fail": parse_fail,
        "parse_fail_ratio": parse_fail / raw_records if raw_records else 0.0,
        "distinct_vids": len(vid_counts),
        "singleton_vids": singleton_vids,
        "singleton_vid_ratio": singleton_vids / len(vid_counts) if vid_counts else 0.0,
        "distinct_locs": len(loc_counts),
        "distinct_loc_slots": len(loc_slot_counts),
        "min_ts": min_ts,
        "max_ts": max_ts,
        "t0": args.t0,
        "slot_size": args.slot_size,
    }

    with open(out_dir / "summary.json", "w", encoding="utf-8") as out:
        json.dump(summary, out, ensure_ascii=False, indent=2, sort_keys=True)
        out.write("\n")

    write_counter_csv(out_dir / "top_vid.csv", ["vid", "records"], vid_counts.most_common(args.top))
    write_counter_csv(out_dir / "top_loc.csv", ["loc", "records"], loc_counts.most_common(args.top))
    write_counter_csv(out_dir / "day_counts.csv", ["day", "records"], sorted(day_counts.items()))
    write_counter_csv(
        out_dir / "top_loc_slot.csv",
        ["loc", "slot", "records"],
        [(loc, slot, count) for (loc, slot), count in loc_slot_counts.most_common(args.top)],
    )


if __name__ == "__main__":
    main()
