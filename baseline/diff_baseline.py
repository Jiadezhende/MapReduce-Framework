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

    missing_list = top_pairs(((key, baseline[key]) for key in diff["missing"]), 100)
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
        "extra_in_mr": extra_list,
        "count_mismatches": mismatch_list
    }

    os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as out:
        json.dump(report, out, indent=2)
        out.write("\n")


if __name__ == "__main__":
    main()
