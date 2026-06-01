#!/usr/bin/env python3
"""把 Hadoop JobHistory 解析成 R6 文档约定的 counters / wall-clock CSV。

输入接口来自 bench/README.md：优先支持真实集群落盘的 Avro `.jhist`
文件；同时保留 fake 文本文档格式，方便没有集群时做单元测试。输出接口固定为
`counters.csv` 和可选的 `wall_clock.csv`，列名不能随意变更，否则下游报告脚本
无法稳定消费。
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


STAGE_GROUPS = {"STAGE0", "STAGE1", "STAGE2", "STAGE3"}
AVRO_HISTORY_PREFIX = b"Avro-Binary\n"


class AvroDecodeError(ValueError):
    """真实 `.jhist` 解码失败时抛出，错误信息里保留文件偏移。"""


GROUP_ALIASES = {
    "FILE SYSTEM COUNTERS": "org.apache.hadoop.mapreduce.FileSystemCounter",
    "FILESYSTEMCOUNTER": "org.apache.hadoop.mapreduce.FileSystemCounter",
    "FILE_SYSTEM_COUNTER": "org.apache.hadoop.mapreduce.FileSystemCounter",
    "TASK COUNTERS": "org.apache.hadoop.mapreduce.TaskCounter",
    "TASKCOUNTER": "org.apache.hadoop.mapreduce.TaskCounter",
    "TASK_COUNTER": "org.apache.hadoop.mapreduce.TaskCounter",
}

COUNTER_ALIASES = {
    "HDFS: NUMBER OF BYTES READ": "HDFS_BYTES_READ",
    "HDFS_BYTES_READ": "HDFS_BYTES_READ",
    "FILE_BYTES_READ": "FILE_BYTES_READ",
    "HDFS: NUMBER OF BYTES WRITTEN": "HDFS_BYTES_WRITTEN",
    "HDFS_BYTES_WRITTEN": "HDFS_BYTES_WRITTEN",
    "BYTES_WRITTEN": "BYTES_WRITTEN",
    "MAP_OUTPUT_BYTES": "MAP_OUTPUT_BYTES",
    "REDUCE_SHUFFLE_BYTES": "REDUCE_SHUFFLE_BYTES",
    "CPU_MILLISECONDS": "CPU_MILLISECONDS",
    "GC_TIME_MILLIS": "GC_TIME_MILLIS",
}


@dataclass
class CounterRow:
    job_id: str
    job_name: str
    stage: str
    counter_group: str
    counter_name: str
    value: int


@dataclass
class ReducerAttemptRow:
    reducer_id: str
    records: int
    shuffle_bytes: int
    wall_ms: int
    status: str
    attempt_id: str


@dataclass
class JobBlock:
    source: str
    job_id: str = ""
    job_name: str = ""
    submit_time_ms: int | None = None
    launch_time_ms: int | None = None
    finish_time_ms: int | None = None
    counters: list[tuple[str, str, int]] = field(default_factory=list)

    def has_data(self) -> bool:
        return bool(
            self.job_id
            or self.job_name
            or self.counters
            or self.submit_time_ms is not None
            or self.launch_time_ms is not None
            or self.finish_time_ms is not None
        )

    def inferred_stage(self) -> str:
        text = self.job_name or self.job_id
        match = re.search(r"stage\s*([0-3])", text, re.IGNORECASE)
        if match:
            return f"stage{match.group(1)}"
        for group, _, _ in self.counters:
            if group in STAGE_GROUPS:
                return group.lower()
        return ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="解析 Hadoop JobHistory `.jhist` 或文本 counter")
    parser.add_argument(
        "--input",
        required=True,
        action="append",
        help="JobHistory 输入文件；真实集群通常是 Avro `.jhist`，测试可传文本 fixture。可重复传入。",
    )
    parser.add_argument("--counters-out", required=True, help="输出 counters.csv")
    parser.add_argument("--wall-out", help="可选输出 wall_clock.csv")
    parser.add_argument(
        "--reducer-attempts-out",
        help="可选输出 reducer_attempts.csv，数据从真实 `.jhist` 的 reduce attempt 事件派生。",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="任一输入文件没有解析出 counter 时直接失败，适合集群真实验证。",
    )
    return parser.parse_args()


def clean_group(value: str) -> str:
    group = value.strip().strip(":")
    upper = re.sub(r"[^A-Za-z0-9]+", "_", group).strip("_").upper()
    return GROUP_ALIASES.get(group.upper(), GROUP_ALIASES.get(upper, group))


def clean_counter(value: str) -> str:
    counter = value.strip().strip(":")
    upper = re.sub(r"\s+", " ", counter).upper()
    if upper in COUNTER_ALIASES:
        return COUNTER_ALIASES[upper]
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", counter).strip("_").upper()
    return COUNTER_ALIASES.get(normalized, normalized)


def parse_int(value: str) -> int:
    return int(value.replace(",", "").strip())


def parse_time_value(value: str) -> int | None:
    match = re.search(r"-?\d[\d,]*", value)
    if not match:
        return None
    return parse_int(match.group(0))


def split_inline_group_counter(name: str) -> tuple[str, str] | None:
    stripped = name.strip()
    for group in STAGE_GROUPS:
        prefix = group + "."
        if stripped.upper().startswith(prefix):
            return group, clean_counter(stripped[len(prefix) :])

    match = re.match(r"(.+?)[./]([A-Z][A-Z0-9_ ]+)$", stripped)
    if match:
        group = clean_group(match.group(1))
        counter = clean_counter(match.group(2))
        return group, counter
    return None


def read_avro_long(data: bytes, pos: int) -> tuple[int, int]:
    """读取 Avro zig-zag long/int；`.jhist` 里的时间戳、数组长度都走这里。"""
    shift = 0
    raw = 0
    start = pos
    while True:
        if pos >= len(data):
            raise AvroDecodeError(f"Avro long 在偏移 {start} 处截断")
        byte = data[pos]
        pos += 1
        raw |= (byte & 0x7F) << shift
        if not byte & 0x80:
            value = (raw >> 1) ^ -(raw & 1)
            return value, pos
        shift += 7
        if shift > 70:
            raise AvroDecodeError(f"Avro long 在偏移 {start} 处过长")


def read_avro_bytes(data: bytes, pos: int) -> tuple[bytes, int]:
    """读取 Avro bytes/string 的长度前缀和正文。"""
    length, pos = read_avro_long(data, pos)
    if length < 0:
        raise AvroDecodeError(f"Avro bytes 在偏移 {pos} 处出现负长度 {length}")
    end = pos + length
    if end > len(data):
        raise AvroDecodeError(f"Avro bytes 在偏移 {pos} 处长度越界: {length}")
    return data[pos:end], end


def read_avro_string(data: bytes, pos: int) -> tuple[str, int]:
    """读取 Avro UTF-8 string；解码失败说明 `.jhist` 文件本身或偏移不对。"""
    raw, pos = read_avro_bytes(data, pos)
    return raw.decode("utf-8"), pos


def register_named_schemas(schema: Any, names: dict[str, dict[str, Any]]) -> None:
    """收集 Avro record/enum/fixed 的名字，供 `JhCounters` 这类引用复用。"""
    if isinstance(schema, list):
        for item in schema:
            register_named_schemas(item, names)
        return
    if not isinstance(schema, dict):
        return

    schema_type = schema.get("type")
    if schema_type in {"record", "enum", "fixed"} and "name" in schema:
        name = str(schema["name"])
        namespace = schema.get("namespace")
        full_name = name if "." in name or not namespace else f"{namespace}.{name}"
        names[name] = schema
        names[full_name] = schema

    register_named_schemas(schema_type, names)
    for field_schema in schema.get("fields", []):
        register_named_schemas(field_schema.get("type"), names)
    if "items" in schema:
        register_named_schemas(schema["items"], names)
    if "values" in schema:
        register_named_schemas(schema["values"], names)


def decode_avro(schema: Any, data: bytes, pos: int, names: dict[str, dict[str, Any]]) -> tuple[Any, int]:
    """按 `.jhist` 自带 schema 解一段 Avro 数据，只实现 JobHistory 用到的类型。"""
    if isinstance(schema, str):
        if schema in names:
            return decode_avro(names[schema], data, pos, names)
        if schema == "null":
            return None, pos
        if schema == "boolean":
            if pos >= len(data):
                raise AvroDecodeError(f"Avro boolean 在偏移 {pos} 处截断")
            return data[pos] != 0, pos + 1
        if schema in {"int", "long"}:
            return read_avro_long(data, pos)
        if schema == "string":
            return read_avro_string(data, pos)
        if schema == "bytes":
            return read_avro_bytes(data, pos)
        raise AvroDecodeError(f"不支持的 Avro 类型引用: {schema}")

    if isinstance(schema, list):
        branch_index, pos = read_avro_long(data, pos)
        if branch_index < 0 or branch_index >= len(schema):
            raise AvroDecodeError(f"Avro union 分支越界: {branch_index}")
        return decode_avro(schema[branch_index], data, pos, names)

    if not isinstance(schema, dict):
        raise AvroDecodeError(f"非法 Avro schema 片段: {schema!r}")

    schema_type = schema["type"]
    if isinstance(schema_type, (dict, list)):
        return decode_avro(schema_type, data, pos, names)
    if isinstance(schema_type, str) and schema_type in names and schema_type not in {
        "record",
        "enum",
        "array",
        "map",
        "fixed",
    }:
        return decode_avro(names[schema_type], data, pos, names)

    if schema_type == "record":
        record: dict[str, Any] = {}
        for field_schema in schema["fields"]:
            record[field_schema["name"]], pos = decode_avro(field_schema["type"], data, pos, names)
        return record, pos

    if schema_type == "enum":
        symbol_index, pos = read_avro_long(data, pos)
        symbols = schema["symbols"]
        if symbol_index < 0 or symbol_index >= len(symbols):
            raise AvroDecodeError(f"Avro enum 分支越界: {symbol_index}")
        return symbols[symbol_index], pos

    if schema_type == "array":
        values: list[Any] = []
        while True:
            block_count, pos = read_avro_long(data, pos)
            if block_count == 0:
                return values, pos
            if block_count < 0:
                block_count = -block_count
                _block_bytes, pos = read_avro_long(data, pos)
            for _ in range(block_count):
                value, pos = decode_avro(schema["items"], data, pos, names)
                values.append(value)

    if schema_type == "map":
        values: dict[str, Any] = {}
        while True:
            block_count, pos = read_avro_long(data, pos)
            if block_count == 0:
                return values, pos
            if block_count < 0:
                block_count = -block_count
                _block_bytes, pos = read_avro_long(data, pos)
            for _ in range(block_count):
                key, pos = read_avro_string(data, pos)
                value, pos = decode_avro(schema["values"], data, pos, names)
                values[key] = value

    if schema_type == "fixed":
        size = int(schema["size"])
        end = pos + size
        if end > len(data):
            raise AvroDecodeError(f"Avro fixed 在偏移 {pos} 处长度越界: {size}")
        return data[pos:end], end

    raise AvroDecodeError(f"不支持的 Avro 类型: {schema_type}")


def avro_schema_and_offset(data: bytes) -> tuple[dict[str, Any], int]:
    """解析 Hadoop HistoryEventWriter 写在文件头里的 JSON schema 和首条事件偏移。"""
    if not data.startswith(AVRO_HISTORY_PREFIX):
        raise AvroDecodeError("文件头不是 Hadoop Avro JobHistory")

    # Hadoop `.jhist` 不是标准 Avro Object Container File；它先写固定魔数，
    # 再写一段 JSON schema，之后直接连续写 Event record，所以这里要手动定位
    # JSON 结束位置，而不是找 avro/fastavro 依赖。
    text = data[len(AVRO_HISTORY_PREFIX) :].decode("utf-8", errors="replace")
    schema, end_char = json.JSONDecoder().raw_decode(text)
    offset = len(AVRO_HISTORY_PREFIX) + end_char
    while offset < len(data) and data[offset] in b" \t\r\n":
        offset += 1
    return schema, offset


def append_jh_counters(job: JobBlock, counters: dict[str, Any]) -> None:
    """把 JobFinished.totalCounters 展平成 `(group, counter, value)`。"""
    job.counters.extend(flatten_jh_counters(counters))


def flatten_jh_counters(counters: dict[str, Any]) -> list[tuple[str, str, int]]:
    """把 Hadoop `JhCounters` 统一转成三元组，供 job 汇总和 attempt 派生复用。"""
    rows: list[tuple[str, str, int]] = []
    for group in counters.get("groups", []):
        group_name = clean_group(group.get("name") or group.get("displayName") or "")
        for counter in group.get("counts", []):
            counter_name = clean_counter(counter.get("name") or counter.get("displayName") or "")
            rows.append((group_name, counter_name, int(counter.get("value", 0))))
    return rows


def jh_counter_value(counters: dict[str, Any], group_name: str, counter_name: str) -> int:
    """从 attempt counter 里按组名和 counter 名取值，缺失时返回 0。"""
    wanted_group = clean_group(group_name)
    wanted_counter = clean_counter(counter_name)
    for group, counter, value in flatten_jh_counters(counters):
        if group == wanted_group and counter == wanted_counter:
            return value
    return 0


def maybe_rotate_job(jobs: list[JobBlock], current: JobBlock, source: str, job_id: str) -> JobBlock:
    """理论上一个 `.jhist` 只有一个 job；这里兼容拼接文件，避免 job 串行污染。"""
    if current.job_id and current.job_id != job_id:
        append_if_nonempty(jobs, current)
        return new_job(source, len(jobs) + 1)
    return current


def decode_history_events(data: bytes, source: str) -> list[dict[str, Any]]:
    """把真实 Hadoop `.jhist` 解成 Event 列表，后续按用途提取 job 或 attempt。"""
    schema, pos = avro_schema_and_offset(data)
    names: dict[str, dict[str, Any]] = {}
    register_named_schemas(schema, names)

    events: list[dict[str, Any]] = []
    while pos < len(data):
        event_start = pos
        try:
            decoded, pos = decode_avro(schema, data, pos, names)
        except AvroDecodeError as exc:
            raise AvroDecodeError(f"{source}: 偏移 {event_start} 附近解码失败: {exc}") from exc
        events.append(decoded)
    return events


def parse_history_avro(data: bytes, source: str) -> list[JobBlock]:
    """解析真实 Hadoop `.jhist`，只取 R6 报告需要的 job 级汇总指标。"""
    jobs: list[JobBlock] = []
    current = new_job(source, 1)
    for decoded in decode_history_events(data, source):

        event_type = decoded.get("type", "")
        event = decoded.get("event") or {}
        job_id = event.get("jobid", "")
        if job_id:
            current = maybe_rotate_job(jobs, current, source, job_id)
            current.job_id = job_id

        if event_type == "JOB_SUBMITTED":
            current.job_name = event.get("jobName", "")
            current.submit_time_ms = event.get("submitTime")
        elif event_type == "JOB_INFO_CHANGED":
            current.submit_time_ms = event.get("submitTime", current.submit_time_ms)
            current.launch_time_ms = event.get("launchTime", current.launch_time_ms)
        elif event_type == "JOB_INITED":
            current.launch_time_ms = event.get("launchTime", current.launch_time_ms)
        elif event_type == "JOB_FINISHED":
            current.finish_time_ms = event.get("finishTime", current.finish_time_ms)
            append_jh_counters(current, event.get("totalCounters") or {})
        elif event_type in {"JOB_FAILED", "JOB_KILLED", "JOB_ERROR"}:
            current.finish_time_ms = event.get("finishTime", current.finish_time_ms)

    append_if_nonempty(jobs, current)
    return jobs


def reducer_id_from_taskid(taskid: str) -> str:
    """把 Hadoop reduce task id 压成稳定数字；无法识别时保留原始 taskid。"""
    match = re.search(r"_r_(\d+)$", taskid)
    if match:
        return str(int(match.group(1)))
    return taskid


def reducer_attempt_rows_from_avro(data: bytes, source: str) -> list[ReducerAttemptRow]:
    """从 `.jhist` 的 reduce attempt 事件派生 reducer_skew.py 的真实输入。"""
    starts: dict[str, int] = {}
    rows: list[ReducerAttemptRow] = []
    for decoded in decode_history_events(data, source):
        event_type = decoded.get("type", "")
        event = decoded.get("event") or {}
        attempt_id = event.get("attemptId", "")

        if event_type in {"TASK_ATTEMPT_STARTED", "REDUCE_ATTEMPT_STARTED"} and attempt_id:
            starts[attempt_id] = int(event.get("startTime", 0))
            continue

        if event_type != "REDUCE_ATTEMPT_FINISHED":
            continue

        finish_time = int(event.get("finishTime", 0))
        start_time = starts.get(attempt_id, 0)
        wall_ms = finish_time - start_time if start_time and finish_time >= start_time else 0
        counters = event.get("counters") or {}
        rows.append(
            ReducerAttemptRow(
                reducer_id=reducer_id_from_taskid(event.get("taskid", "")),
                records=jh_counter_value(
                    counters,
                    "org.apache.hadoop.mapreduce.TaskCounter",
                    "REDUCE_OUTPUT_RECORDS",
                ),
                shuffle_bytes=jh_counter_value(
                    counters,
                    "org.apache.hadoop.mapreduce.TaskCounter",
                    "REDUCE_SHUFFLE_BYTES",
                ),
                wall_ms=wall_ms,
                status=(event.get("taskStatus") or event.get("state") or "").upper(),
                attempt_id=attempt_id,
            )
        )
    return rows


def reducer_attempt_rows_from_history_file(path: Path) -> list[ReducerAttemptRow]:
    """读取单个 history 文件；只有真实 Avro `.jhist` 能派生 reducer attempt。"""
    data = path.read_bytes()
    if not data.startswith(AVRO_HISTORY_PREFIX):
        return []
    return reducer_attempt_rows_from_avro(data, str(path))


def parse_history_file(path: Path) -> list[JobBlock]:
    """根据文件头选择真实 `.jhist` Avro 解析或 fake 文本解析。"""
    data = path.read_bytes()
    if data.startswith(AVRO_HISTORY_PREFIX):
        return parse_history_avro(data, str(path))
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = data.decode("utf-8", errors="replace")
    return parse_history_text(text, str(path))


def new_job(source: str, index: int) -> JobBlock:
    return JobBlock(source=source)


def append_if_nonempty(jobs: list[JobBlock], job: JobBlock) -> None:
    if job.has_data():
        if not job.job_id:
            job.job_id = f"{Path(job.source).stem}#{len(jobs) + 1}"
        jobs.append(job)


def parse_history_text(text: str, source: str) -> list[JobBlock]:
    jobs: list[JobBlock] = []
    current = new_job(source, 1)
    current_group = ""
    job_index = 1

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        job_match = re.match(r"(?i)^(?:job(?:\s+id)?|jobid)\s*[:=]\s*(job_[A-Za-z0-9_]+)", stripped)
        if job_match:
            found_job_id = job_match.group(1)
            if current.has_data() and current.job_id and current.job_id != found_job_id:
                append_if_nonempty(jobs, current)
                job_index += 1
                current = new_job(source, job_index)
                current_group = ""
            current.job_id = found_job_id
            continue

        bare_job = re.match(r"(?i)^job\s+[:=]\s*(job_[A-Za-z0-9_]+)", stripped)
        if bare_job:
            current.job_id = bare_job.group(1)
            continue

        name_match = re.match(r"(?i)^job\s*name\s*[:=]\s*(.+)$", stripped)
        if name_match:
            current.job_name = name_match.group(1).strip()
            continue
        camel_name_match = re.match(r"(?i)^jobname\s*[:=]\s*(.+)$", stripped)
        if camel_name_match:
            current.job_name = camel_name_match.group(1).strip()
            continue

        time_match = re.match(r"(?i)^(submit|launch|start|finish|finished)\s*time\s*[:=]\s*(.+)$", stripped)
        if time_match:
            label = time_match.group(1).lower()
            parsed_time = parse_time_value(time_match.group(2))
            if parsed_time is not None:
                if label == "submit":
                    current.submit_time_ms = parsed_time
                elif label in {"launch", "start"}:
                    current.launch_time_ms = parsed_time
                else:
                    current.finish_time_ms = parsed_time
            continue

        camel_time_match = re.match(r"(?i)^(submitTime|launchTime|startTime|finishTime)\s*[:=]\s*(.+)$", stripped)
        if camel_time_match:
            label = camel_time_match.group(1).lower()
            parsed_time = parse_time_value(camel_time_match.group(2))
            if parsed_time is not None:
                if label == "submittime":
                    current.submit_time_ms = parsed_time
                elif label in {"launchtime", "starttime"}:
                    current.launch_time_ms = parsed_time
                else:
                    current.finish_time_ms = parsed_time
            continue

        counter_match = re.match(r"^(.+?)\s*(?:=|:)\s*(-?\d[\d,]*)\s*$", stripped)
        if counter_match:
            name = counter_match.group(1).strip()
            value = parse_int(counter_match.group(2))
            inline = split_inline_group_counter(name)
            if inline is not None:
                group, counter = inline
            else:
                group = current_group
                counter = clean_counter(name)
            if group:
                current.counters.append((group, counter, value))
            continue

        group_candidate = stripped.strip(":")
        if (
            group_candidate.upper() in STAGE_GROUPS
            or "COUNTER" in group_candidate.upper()
            or group_candidate.upper() in GROUP_ALIASES
            or group_candidate.startswith("org.apache.hadoop.")
            or re.match(r"^[A-Z][A-Z0-9_.-]*$", group_candidate)
        ):
            current_group = clean_group(group_candidate)
            continue

    append_if_nonempty(jobs, current)
    return jobs


def counter_rows(jobs: Iterable[JobBlock]) -> list[CounterRow]:
    rows: list[CounterRow] = []
    for job in jobs:
        job_stage = job.inferred_stage()
        for group, counter, value in job.counters:
            stage = group.lower() if group in STAGE_GROUPS else job_stage
            rows.append(
                CounterRow(
                    job_id=job.job_id,
                    job_name=job.job_name,
                    stage=stage,
                    counter_group=group,
                    counter_name=counter,
                    value=value,
                )
            )
    return rows


def write_counters(path: str, rows: Iterable[CounterRow]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["job_id", "job_name", "stage", "counter_group", "counter_name", "value"])
        for row in rows:
            writer.writerow(
                [
                    row.job_id,
                    row.job_name,
                    row.stage,
                    row.counter_group,
                    row.counter_name,
                    row.value,
                ]
            )


def write_wall_clock(path: str, jobs: Iterable[JobBlock]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(
            [
                "job_id",
                "job_name",
                "stage",
                "submit_time_ms",
                "launch_time_ms",
                "finish_time_ms",
                "wall_clock_ms",
            ]
        )
        for job in jobs:
            start = job.launch_time_ms if job.launch_time_ms is not None else job.submit_time_ms
            wall = ""
            if start is not None and job.finish_time_ms is not None:
                wall = job.finish_time_ms - start
            writer.writerow(
                [
                    job.job_id,
                    job.job_name,
                    job.inferred_stage(),
                    "" if job.submit_time_ms is None else job.submit_time_ms,
                    "" if job.launch_time_ms is None else job.launch_time_ms,
                    "" if job.finish_time_ms is None else job.finish_time_ms,
                    wall,
                ]
            )


def write_reducer_attempts(path: str, rows: Iterable[ReducerAttemptRow]) -> None:
    """写出 reducer_skew.py 的输入 CSV；列名和 bench/README.md 保持一致。"""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["reducer_id", "records", "shuffle_bytes", "wall_ms", "status", "attempt_id"])
        for row in rows:
            writer.writerow(
                [
                    row.reducer_id,
                    row.records,
                    row.shuffle_bytes,
                    row.wall_ms,
                    row.status,
                    row.attempt_id,
                ]
            )


def main() -> int:
    args = parse_args()
    jobs: list[JobBlock] = []
    reducer_attempts: list[ReducerAttemptRow] = []

    for input_path in args.input:
        path = Path(input_path)
        parsed = parse_history_file(path)
        if args.strict and not any(job.counters for job in parsed):
            print(f"ERROR: no counters parsed from {path}", file=sys.stderr)
            return 1
        jobs.extend(parsed)
        if args.reducer_attempts_out:
            reducer_attempts.extend(reducer_attempt_rows_from_history_file(path))

    rows = counter_rows(jobs)
    write_counters(args.counters_out, rows)
    if args.wall_out:
        write_wall_clock(args.wall_out, jobs)
    if args.reducer_attempts_out:
        write_reducer_attempts(args.reducer_attempts_out, reducer_attempts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
