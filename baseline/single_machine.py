#!/usr/bin/env python3
import argparse
import json
import os
import xml.etree.ElementTree as ET
from collections import defaultdict

import pandas as pd


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
    parser = argparse.ArgumentParser(description="Pandas baseline for companion pairs")
    parser.add_argument("--input", required=True, help="input CSV: vid,loc,ts")
    parser.add_argument("--output", required=True, help="output CSV: vidA,vidB,count")
    parser.add_argument("--config", default="common/src/main/resources/companion-conf.xml")
    parser.add_argument("--t0", type=int)
    parser.add_argument("--delta-t", type=int)
    parser.add_argument("--k-min", type=int)
    parser.add_argument("--slot-size", type=int)
    parser.add_argument("--emit-config", action="store_true")
    return parser.parse_args()


def pair_key(a, b):
    if a == b:
        return None
    if a < b:
        return (a << 32) | (b & 0xFFFFFFFF)
    return (b << 32) | (a & 0xFFFFFFFF)


def decode_pair(key):
    return key >> 32, key & 0xFFFFFFFF


def witness_key(loc, slot):
    return (loc << 32) | (slot & 0xFFFFFFFF)


def build_pairs(df, delta_t, slot_size):
    pair_witnesses = defaultdict(set)
    for loc, group in df.groupby("loc", sort=True):
        group = group.sort_values("t_norm")
        vids = group["vid"].to_numpy()
        ts = group["t_norm"].to_numpy()
        slots = group["slot"].to_numpy()

        start = 0
        for i in range(len(vids)):
            cur_ts = int(ts[i])
            cur_vid = int(vids[i])
            cur_slot = int(slots[i])
            while start < i and int(ts[start]) < cur_ts - delta_t:
                start += 1
            for j in range(start, i):
                prev_vid = int(vids[j])
                key = pair_key(prev_vid, cur_vid)
                if key is None:
                    continue
                pair_witnesses[key].add(witness_key(int(loc), cur_slot))
    return pair_witnesses


def main():
    args = parse_args()
    conf = load_config(args.config)

    t0 = args.t0 if args.t0 is not None else int(conf.get("companion.t0", "1420041600"))
    delta_t = args.delta_t if args.delta_t is not None else int(conf.get("companion.delta.t", "300"))
    k_min = args.k_min if args.k_min is not None else int(conf.get("companion.k.min", "3"))
    slot_size = args.slot_size if args.slot_size is not None else int(conf.get("companion.slot.size", "300"))

    if args.emit_config:
        print(json.dumps({
            "t0": t0,
            "delta_t": delta_t,
            "k_min": k_min,
            "slot_size": slot_size
        }, indent=2))
        return

    df = pd.read_csv(
        args.input,
        header=None,
        names=["vid", "loc", "ts"],
        dtype={"vid": "int64", "loc": "int64", "ts": "int64"}
    )

    counts = df["vid"].value_counts()
    keep = counts[counts >= 2].index
    df = df[df["vid"].isin(keep)].copy()

    df["t_norm"] = df["ts"] - t0
    df["slot"] = (df["t_norm"] // slot_size).astype("int64")

    pair_witnesses = build_pairs(df[["vid", "loc", "t_norm", "slot"]], delta_t, slot_size)

    rows = []
    for key, witnesses in pair_witnesses.items():
        count = len(witnesses)
        if count >= k_min:
            vid_a, vid_b = decode_pair(key)
            rows.append((vid_a, vid_b, count))

    rows.sort(key=lambda r: (-r[2], r[0], r[1]))

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as out:
        for vid_a, vid_b, count in rows:
            out.write(f"{vid_a},{vid_b},{count}\n")


if __name__ == "__main__":
    main()
