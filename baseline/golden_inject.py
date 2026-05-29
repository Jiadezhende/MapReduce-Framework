#!/usr/bin/env python3
import argparse
import json
import os
import xml.etree.ElementTree as ET


def load_config(path):
    tree = ET.parse(path)
    root = tree.getroot()
    conf = {}
    for prop in root.findall("property"):
        name = prop.findtext("name")
        value = prop.findtext("value")
        if name and value is not None:
            conf[name] = value.strip()
    return conf


def parse_args():
    parser = argparse.ArgumentParser(description="Inject a golden pair into CSV data")
    parser.add_argument("--input", required=True, help="input CSV: vid,loc,ts")
    parser.add_argument("--output", required=True, help="output CSV with injected records")
    parser.add_argument("--config", default="common/src/main/resources/companion-conf.xml")
    parser.add_argument("--vid-a", type=int, default=90000001)
    parser.add_argument("--vid-b", type=int, default=90000002)
    parser.add_argument("--loc", type=int, default=999999)
    parser.add_argument("--start-ts", type=int)
    parser.add_argument("--slots", type=int, default=3)
    parser.add_argument("--delta-t", type=int)
    parser.add_argument("--slot-size", type=int)
    parser.add_argument("--emit-summary", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    conf = load_config(args.config)

    t0 = int(conf.get("companion.t0", "1420041600"))
    delta_t = args.delta_t if args.delta_t is not None else int(conf.get("companion.delta.t", "300"))
    slot_size = args.slot_size if args.slot_size is not None else int(conf.get("companion.slot.size", "300"))
    start_ts = args.start_ts if args.start_ts is not None else t0 + 60

    offset = min(delta_t - 1, slot_size - 1)
    if offset < 0:
        offset = 0

    records = []
    for i in range(args.slots):
        ts_a = start_ts + i * slot_size
        ts_b = ts_a + offset
        records.append(f"{args.vid_a},{args.loc},{ts_a}")
        records.append(f"{args.vid_b},{args.loc},{ts_b}")

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.input, "r", encoding="utf-8") as src, \
            open(args.output, "w", encoding="utf-8") as out:
        for line in src:
            out.write(line)
        for line in records:
            out.write(line)
            out.write("\n")

    if args.emit_summary:
        summary = {
            "vid_a": args.vid_a,
            "vid_b": args.vid_b,
            "loc": args.loc,
            "start_ts": start_ts,
            "slots": args.slots,
            "delta_t": delta_t,
            "slot_size": slot_size,
            "records_added": len(records)
        }
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
