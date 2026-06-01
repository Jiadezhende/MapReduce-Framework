#!/usr/bin/env python3
"""按 R6 文档接口汇总 reducer 倾斜情况。

输入是每个 reduce attempt 的 CSV，至少要能提供 reducer id、输出记录数、
shuffle 字节数、运行耗时和状态；输出拆成两份：`skew.csv` 保留成功
reducer 明细，`skew_summary.csv` 给报告直接消费 mean/p50/p95/max/max_mean_ratio。
"""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


STATUS_SUCCESS = "SUCCEEDED"


@dataclass
class ReducerAttempt:
    reducer_id: str
    records: int
    shuffle_bytes: int
    wall_ms: int
    status: str
    attempt_id: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="汇总 reducer skew")
    parser.add_argument("--input", required=True, help="输入 reducer attempt CSV")
    parser.add_argument("--detail-out", required=True, help="输出成功 reducer 明细 CSV，即 skew.csv")
    parser.add_argument("--summary-out", required=True, help="输出 reducer 汇总 CSV，即 skew_summary.csv")
    parser.add_argument(
        "--include-status",
        default=STATUS_SUCCESS,
        help="只统计该状态的 attempt，默认 SUCCEEDED，避免失败重试污染 skew",
    )
    return parser.parse_args()


def parse_int(value: str | None) -> int:
    if value is None or value == "":
        return 0
    return int(str(value).replace(",", "").strip())


def first_present(row: dict[str, str], names: Iterable[str]) -> str:
    lowered = {key.lower(): value for key, value in row.items()}
    for name in names:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return ""


def load_attempts(path: str, include_status: str = STATUS_SUCCESS) -> list[ReducerAttempt]:
    attempts: list[ReducerAttempt] = []
    with open(path, "r", encoding="utf-8", newline="") as src:
        reader = csv.DictReader(src)
        if reader.fieldnames is None:
            return attempts
        for row in reader:
            # Hadoop / 导出脚本字段名可能不同，这里统一归一成 R6 文档里的
            # reducer_id, records, shuffle_bytes, wall_ms, status。
            status = first_present(row, ["status", "state", "attempt_status"]).strip().upper()
            if status != include_status.upper():
                continue
            reducer_id = first_present(row, ["reducer_id", "task_id", "taskid", "id"]).strip()
            attempt_id = first_present(row, ["attempt_id", "attemptid"]).strip()
            attempts.append(
                ReducerAttempt(
                    reducer_id=reducer_id,
                    records=parse_int(first_present(row, ["records", "reduce_output_records", "record_count"])),
                    shuffle_bytes=parse_int(first_present(row, ["shuffle_bytes", "reduce_shuffle_bytes"])),
                    wall_ms=parse_int(first_present(row, ["wall_ms", "wall_time_ms", "elapsed_ms", "duration_ms"])),
                    status=status,
                    attempt_id=attempt_id,
                )
            )
    return attempts


def nearest_rank(values: list[int], percentile: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile / 100.0 * len(ordered)))
    return ordered[rank - 1]


def metric_summary(values: list[int]) -> dict[str, float | int]:
    if not values:
        return {
            "count": 0,
            "mean": 0.0,
            "p50": 0,
            "p95": 0,
            "max": 0,
            "max_mean_ratio": 0.0,
        }
    mean = sum(values) / len(values)
    max_value = max(values)
    return {
        "count": len(values),
        "mean": mean,
        "p50": nearest_rank(values, 50),
        "p95": nearest_rank(values, 95),
        "max": max_value,
        "max_mean_ratio": (max_value / mean) if mean else 0.0,
    }


def write_detail(path: str, attempts: Iterable[ReducerAttempt]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["reducer_id", "records", "shuffle_bytes", "wall_ms", "status", "attempt_id"])
        for attempt in attempts:
            writer.writerow(
                [
                    attempt.reducer_id,
                    attempt.records,
                    attempt.shuffle_bytes,
                    attempt.wall_ms,
                    attempt.status,
                    attempt.attempt_id,
                ]
            )


def write_summary(path: str, attempts: list[ReducerAttempt]) -> None:
    metrics = {
        "records": [attempt.records for attempt in attempts],
        "shuffle_bytes": [attempt.shuffle_bytes for attempt in attempts],
        "wall_ms": [attempt.wall_ms for attempt in attempts],
    }
    with open(path, "w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["metric", "count", "mean", "p50", "p95", "max", "max_mean_ratio"])
        for metric, values in metrics.items():
            summary = metric_summary(values)
            writer.writerow(
                [
                    metric,
                    summary["count"],
                    f"{summary['mean']:.6f}",
                    summary["p50"],
                    summary["p95"],
                    summary["max"],
                    f"{summary['max_mean_ratio']:.6f}",
                ]
            )


def main() -> int:
    args = parse_args()
    Path(args.detail_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)
    attempts = load_attempts(args.input, args.include_status)
    write_detail(args.detail_out, attempts)
    write_summary(args.summary_out, attempts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
