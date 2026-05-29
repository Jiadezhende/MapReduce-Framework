#!/usr/bin/env python3
import argparse
import json
import os


def parse_args():
    parser = argparse.ArgumentParser(description="Diff MR output vs baseline")
    parser.add_argument("--mr-output", required=True, help="MR output file or dir")
    parser.add_argument("--baseline-output", required=True, help="Baseline output file or dir")
    parser.add_argument("--report", required=True, help="JSON report path")
    parser.add_argument("--phase", default="")
    parser.add_argument("--hll-pairs", default="", help="HLL pairs file or dir")
    parser.add_argument("--hll-rel-error", type=float, default=0.02)
    parser.add_argument("--stage1-input", default="", help="Optional raw input CSV to simulate Stage1 semantics for structural-missing classification")
    parser.add_argument("--stage1-lines", type=int, default=10000, help="When simulating Stage1, limit to this many lines (default 10000) to match FixtureGenerator)")
    parser.add_argument("--t0", type=int, default=1420041600)
    parser.add_argument("--delta-t", type=int, default=300)
    parser.add_argument("--slot-size", type=int, default=300)
    parser.add_argument("--k-min", type=int, default=3)
    return parser.parse_args()


def pair_key(a, b):
    if a < b:
        return (a << 32) | (b & 0xFFFFFFFF)
    return (b << 32) | (a & 0xFFFFFFFF)


def decode_pair(key):
    return key >> 32, key & 0xFFFFFFFF


def list_files(path):
    if os.path.isdir(path):
        files = []
        for name in os.listdir(path):
            if name.startswith("part-"):
                files.append(os.path.join(path, name))
        return sorted(files)
    return [path]


def read_pairs(path):
    pairs = {}
    for file_path in list_files(path):
        with open(file_path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",")
                if len(parts) < 3:
                    continue
                try:
                    vid_a = int(parts[0])
                    vid_b = int(parts[1])
                    count = int(parts[2])
                except ValueError:
                    continue
                key = pair_key(vid_a, vid_b)
                pairs[key] = count
    return pairs


def read_hll_pairs(path):
    if not path:
        return set()
    if not os.path.exists(path):
        return set()

    pairs = read_pairs(path)
    return set(pairs.keys())


def top_pairs(pairs, n):
    items = [(key, count) for key, count in pairs]
    items.sort(key=lambda x: (-x[1], decode_pair(x[0])[0], decode_pair(x[0])[1]))
    out = []
    for key, count in items[:n]:
        vid_a, vid_b = decode_pair(key)
        out.append(f"{vid_a},{vid_b},{count}")
    return out


def simulate_stage1_pairs(input_csv, lines_limit, delta_t, slot_size, k_min):
    # lightweight single-process Stage1+Stage2 simulation (match FixtureGenerator semantics)
    # returns a set of pair keys that Stage1 could emit (before Stage2 thresholding)
    import csv
    from collections import defaultdict

    # Stage0: read prefix and keep vids with freq>=2
    counts = defaultdict(int)
    rows = []
    with open(input_csv, 'r', encoding='utf-8') as fh:
        reader = csv.reader(fh)
        for i, parts in enumerate(reader):
            if lines_limit and i >= lines_limit:
                break
            if not parts or parts[0].strip().startswith('#'):
                continue
            try:
                vid = int(parts[0].strip())
                loc = int(parts[1].strip())
                ts = int(parts[2].strip())
            except Exception:
                continue
            rows.append((vid, loc, ts))
            counts[vid] += 1

    kept = [r for r in rows if counts.get(r[0], 0) >= 2]

    # Stage1: group by (loc, partition=slot//2) and sliding-window
    groups = defaultdict(list)
    for vid, loc, ts in kept:
        slot = (ts - 1420041600) // slot_size
        partition = slot // 2
        key = (loc, partition)
        groups[key].append((vid, loc, ts))

    witnesses = []  # list of (vidA, vidB, loc, slot)
    for key, group in groups.items():
        group.sort(key=lambda x: x[2])
        window = []
        start = 0
        for cur in group:
            cur_vid, cur_loc, cur_ts = cur
            cur_slot = (cur_ts - 1420041600) // slot_size
            while start < len(window) and window[start][2] < cur_ts - delta_t:
                start += 1
            for j in range(start, len(window)):
                prev = window[j]
                if prev[0] == cur_vid:
                    continue
                va = min(prev[0], cur_vid)
                vb = max(prev[0], cur_vid)
                witnesses.append((va, vb, cur_loc, cur_slot))
            window.append(cur)

    # Stage2: count distinct (loc, slot) per pair, keep >= k_min
    pair_to_cells = defaultdict(set)
    for va, vb, loc, slot in witnesses:
        pair = pair_key(va, vb)
        cell = (loc << 32) | (slot & 0xFFFFFFFF)
        pair_to_cells[pair].add(cell)

    out = set()
    for pair, cells in pair_to_cells.items():
        if len(cells) >= k_min:
            out.add(pair)
    return out


def diff_pairs(mr, baseline, hll_pairs, hll_rel_error):
    mr_keys = set(mr.keys())
    baseline_keys = set(baseline.keys())

    common = mr_keys & baseline_keys
    missing = baseline_keys - mr_keys
    extra = mr_keys - baseline_keys

    mismatches = []
    abs_errors = []
    for key in common:
        mr_count = mr[key]
        base_count = baseline[key]
        if key in hll_pairs and base_count > 0:
            rel = abs(mr_count - base_count) / float(base_count)
            if rel <= hll_rel_error:
                continue
        if mr_count != base_count:
            mismatches.append((key, base_count, mr_count))
        if key not in hll_pairs:
            abs_errors.append(abs(mr_count - base_count))

    if abs_errors:
        count_mae = sum(abs_errors) / float(len(abs_errors))
    else:
        count_mae = 0.0

    precision = len(common) / float(len(mr_keys)) if mr_keys else 0.0
    recall = len(common) / float(len(baseline_keys)) if baseline_keys else 0.0

    return {
        "precision": precision,
        "recall": recall,
        "count_mae": count_mae,
        "missing": missing,
        "extra": extra,
        "mismatches": mismatches
    }


def main():
    args = parse_args()

    mr = read_pairs(args.mr_output)
    baseline = read_pairs(args.baseline_output)
    hll_pairs = read_hll_pairs(args.hll_pairs)

    diff = diff_pairs(mr, baseline, hll_pairs, args.hll_rel_error)

    structural_missing = set()
    real_missing = set()
    if args.stage1_input:
        stage1_pairs = simulate_stage1_pairs(args.stage1_input, args.stage1_lines, args.delta_t, args.slot_size, args.k_min)
        missing_keys = diff["missing"]
        # diff['missing'] is a set of keys from baseline not in mr
        for k in missing_keys:
            if k in stage1_pairs:
                real_missing.add(k)
            else:
                structural_missing.add(k)

    # If we classified missing keys, expose structural vs real missing; otherwise report missing as before
    if args.stage1_input:
        missing_list = top_pairs(((key, baseline[key]) for key in real_missing), 100)
        structural_list = top_pairs(((key, baseline[key]) for key in structural_missing), 100)
    else:
        missing_list = top_pairs(((key, baseline[key]) for key in diff["missing"]), 100)
        structural_list = []
    extra_list = top_pairs(((key, mr[key]) for key in diff["extra"]), 100)

    mismatch_items = []
    for key, base_count, mr_count in diff["mismatches"]:
        mismatch_items.append((key, abs(mr_count - base_count), base_count, mr_count))
    mismatch_items.sort(key=lambda x: (-x[1], decode_pair(x[0])[0], decode_pair(x[0])[1]))

    mismatch_list = []
    for key, _, base_count, mr_count in mismatch_items[:100]:
        vid_a, vid_b = decode_pair(key)
        mismatch_list.append(f"{vid_a},{vid_b},{base_count},{mr_count}")

    report = {
        "phase": args.phase,
        "mr_pairs": len(mr),
        "baseline_pairs": len(baseline),
        "precision": diff["precision"],
        "recall": diff["recall"],
        "count_mae": diff["count_mae"],
        "missing_in_mr": missing_list,
        "structural_missing_in_baseline": structural_list,
        "extra_in_mr": extra_list,
        "count_mismatches": mismatch_list
    }

    os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as out:
        json.dump(report, out, indent=2)
        out.write("\n")


if __name__ == "__main__":
    main()
