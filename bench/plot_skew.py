#!/usr/bin/env python3
"""把 `reducer_skew.py` 产出的 `skew.csv` 渲染成报告用 PNG。

输入列必须匹配 R6 文档中的 reducer 明细接口：records、shuffle_bytes、
wall_ms。图里左侧用箱线图看离群 reducer，右侧用 CDF 看整体分布。
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="绘制 reducer skew 图")
    parser.add_argument("--input", required=True, help="reducer_skew.py 输出的 skew.csv")
    parser.add_argument("--output", required=True, help="输出 PNG 路径")
    return parser.parse_args()


def read_values(path: str) -> dict[str, list[int]]:
    metrics = {"records": [], "shuffle_bytes": [], "wall_ms": []}
    with open(path, "r", encoding="utf-8", newline="") as src:
        reader = csv.DictReader(src)
        for row in reader:
            for metric in metrics:
                value = row.get(metric, "")
                if value:
                    metrics[metric].append(int(value))
    return metrics


def cdf(values: list[int]) -> tuple[list[int], list[float]]:
    ordered = sorted(values)
    if not ordered:
        return [], []
    n = len(ordered)
    return ordered, [(idx + 1) / n for idx in range(n)]


def main() -> int:
    args = parse_args()
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise SystemExit(
            "ERROR: plot_skew.py requires matplotlib. Install it or skip plot generation."
        ) from exc

    metrics = read_values(args.input)
    non_empty = {name: values for name, values in metrics.items() if values}
    if not non_empty:
        raise SystemExit(f"ERROR: no reducer metrics found in {args.input}")

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    box_values = list(non_empty.values())
    box_labels = list(non_empty.keys())
    try:
        axes[0].boxplot(box_values, tick_labels=box_labels, showfliers=True)
    except TypeError:
        axes[0].boxplot(box_values, labels=box_labels, showfliers=True)
    axes[0].set_title("Reducer metric distribution")
    axes[0].tick_params(axis="x", rotation=20)

    for name, values in non_empty.items():
        xs, ys = cdf(values)
        axes[1].plot(xs, ys, label=name)
    axes[1].set_title("Reducer metric CDF")
    axes[1].set_xlabel("value")
    axes[1].set_ylabel("fraction <= value")
    axes[1].legend()

    fig.tight_layout()
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=160)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
