# Stage 1 - 滑动窗口配对

**Owner**: R3（详见 [docs/roles.md](../docs/roles.md)）

Stage 1 的职责是把“车辆出现记录”转换成“车辆对见证”。只要两辆车在同一地点、时间差不超过 `companion.delta.t`，就输出一次该车辆对在某个 `(loc, slot)` 中共同出现的见证。

## 本阶段要解决什么

输入来自 Stage 0：

```text
RecordWritable(vid, loc, ts_norm)
```

其中 `ts_norm` 已经是 `ts - companion.t0` 后的相对秒数。

Stage 1 输出：

```text
PairKey(vidA, vidB) -> LocSlotWritable(loc, slot)
```

其中：

- `vidA < vidB`。
- `slot = ts_norm / companion.slot.size`。
- `PairKey -> LocSlotWritable` 表示这对车在该 `(loc, slot)` 中有一次共同出现的见证。

Stage 1 不负责最终计数，也不负责过滤 `count < k.min`。这些由 Stage 2 完成。

## 输入输出

| 类型 | 路径 | 格式 |
|---|---|---|
| 输入 | `hdfs:///companion/filtered/{phase}/` | `SequenceFile<NullWritable, RecordWritable>` |
| 输出 | `hdfs:///companion/pair_loc_slot/{phase}/part-*` | `SequenceFile<PairKey, LocSlotWritable>` |

输出使用 Snappy block 压缩。

## 推荐实现流程

```text
Mapper
  读取 RecordWritable(vid, loc, ts_norm)
  计算 slot = ts_norm / slot.size
  输出 CompositeKey(loc, slot, ts_norm) -> vid

Shuffle / Sort
  按 loc、slot、ts_norm 排序
  按 loc、slot 分组
  使用 hash(loc, slot / 2) 分区，让相邻的一对 slot 尽量进入同一 reducer

Reducer
  对同一 loc、同一 slot 内的记录按时间滑窗
  对窗口内车辆两两配对
  输出 PairKey(vidA, vidB) -> LocSlotWritable(loc, slot)
```

## 跨 slot 边界

时间窗口可能跨越相邻 slot。例如一条记录在 slot 0 末尾，另一条在 slot 1 开头，它们仍可能满足 `delta.t`。

本阶段用 `hash(loc, slot / 2)` 把 `2k` 和 `2k+1` 这对相邻 slot 分到同一 reducer。Reducer 处理 `slot=2k` 后，保留末尾 `delta.t` 秒内的记录作为 tail buffer；处理 `slot=2k+1` 时，先让当前记录和 tail buffer 配对，再进入本 slot 的滑窗。

已知限制：`2k+1` 和 `2k+2` 的边界不会在同一轮里处理。如果 M3 recall 不达标，需要用第二轮 J1b 或其他补偿方案修复。

## 必须保持的契约

- `PairKey.set(v1, v2)` 会强制 `vidA < vidB`，不要绕过它。
- `CompositeKey` 排序顺序固定为 `(loc asc, slot asc, ts asc)`。
- GroupComparator 固定按 `(loc, slot)` 分组。
- Partitioner 使用 `hash(loc, slot / 2)`，不是 `hash(loc, slot)`。
- Reducer 的滑窗判断使用闭区间：`|tsA - tsB| <= companion.delta.t`。
- 当单个 `(loc, slot)` 过热时，热点切片方案需要和 profiler / bench 的热点统计口径一致。

## Counter

Counter 组名固定为 `STAGE1`。

| Counter | 含义 |
|---|---|
| `INPUT_RECORDS` | mapper 每读入一条 Stage0 记录加 1 |
| `PAIRS_EMITTED` | reducer 每输出一个 pair witness 加 1 |
| `CROSS_SLOT_PAIRS` | 来自 tail buffer 的跨 slot 配对加 1 |
| `SKEW_DROP` | deque 达到 `loc.skew.cap` 后丢弃记录加 1 |
| `HOT_LOCS_SLICED` | 触发热点切片的 `(loc, slot)` 桶数 |

`SKEW_DROP` 应为 0 或极小；如果它持续出现，说明热点切片或阈值设置需要重新评估。

## 性能要求

- 滑窗中只保留当前窗口需要的记录，避免把整个 `(loc, slot)` 全量装入内存。
- Mapper 和 reducer 尽量复用 Writable 对象，减少 pair 爆发时的 GC 压力。
- `companion.loc.skew.cap` 是保护阈值，不是正常限流策略。
- 默认 reducer 数：1d 为 8，7d 为 32，31d 为 128，可通过 `-Dmapreduce.job.reduces` 覆盖。
- 若 max reducer wall time / mean > 3，优先分析热点分布和 salt 策略，再单纯增加 reducer。

## 验收信号

- `STAGE1.INPUT_RECORDS` 约等于 `STAGE0.KEPT_RECORDS`。
- `STAGE1.PAIRS_EMITTED` 与 Stage 2 的 `PAIRS_INPUT` 规模接近。
- `CROSS_SLOT_PAIRS` 非零时，说明跨 slot tail buffer 生效。
- `SKEW_DROP` 不应成为常态。
