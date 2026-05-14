# Stage 2 - 共现计数与阈值过滤

**Owner**: R4（详见 [docs/roles.md](../docs/roles.md)）

Stage 2 把 Stage 1 产生的“车辆对见证流”汇总成最终候选伴随车对。它负责按车辆对去重 `(loc, slot)`，计算 `count`，并过滤掉 `count < companion.k.min` 的 pair。

## 本阶段要解决什么

Stage 1 可能多次输出同一个：

```text
PairKey(vidA, vidB) -> LocSlotWritable(loc, slot)
```

Stage 2 的计数语义是：

```text
count = distinct number of (loc, slot) witnesses for this pair
```

同一个 pair 在同一个 `(loc, slot)` 内出现多次，只能计 1 次。

输出主结果：

```text
vidA\tvidB\tcount
```

只输出 `count >= companion.k.min` 的 pair。

## 输入输出

| 类型 | 路径 | 格式 |
|---|---|---|
| 输入 | `hdfs:///companion/pair_loc_slot/{phase}/` | `SequenceFile<PairKey, LocSlotWritable>` |
| 主输出 | `hdfs:///companion/companions/{phase}/part-*` | 文本 `vidA\tvidB\tcount` |
| 副输出 | `hdfs:///companion/companions/{phase}/_hll_pairs/` | 使用 HLL 估算的 pair 列表 |

主输出 schema 是 Stage 3 和 baseline diff 的契约，字段顺序和分隔符不能改。

## 推荐实现流程

```text
Mapper
  读取 PairKey -> LocSlotWritable
  原样输出

Combiner
  在本地对同一个 pair 的 (loc, slot) 去重
  仍输出 PairKey -> LocSlotWritable

Reducer
  收到同一个 pair 的所有 witness
  对 (loc, slot) 去重
  计算 count
  count >= k.min 时输出 vidA\tvidB\tcount
```

Combiner 只能减少重复 witness，不能提前输出最终 count。因为同一个 pair 的 witness 可能分布在多个 mapper 或 spill 中，下游 reducer 仍需要做全局去重。

## 关于 salt 和热点 pair

如果实现 pair salt，必须保证同一个 pair 的所有 witness 最终被汇总成一个全局 count。不能把同一个 pair 拆到多个 reducer 后，直接把每个 reducer 的局部 count 当作最终结果。

可选实现方式需要在代码和文档中明确：

- 单阶段：partitioner 只按 pair 分区，保证同一个 pair 到同一个 reducer。实现简单，但热点 pair 可能导致 reducer 倾斜。
- 两阶段：第一阶段按 `(pair, salt)` 分散热点并局部去重，第二阶段再按 pair 合并局部结果。实现复杂，但能缓解极热 pair。

当前验收以“最终 count 语义正确”为第一优先级。`companion.pair.salt.n` 的具体使用方式需要和 `docs/architecture.md`、bench 指标保持一致。

## HLL fallback

普通 pair 使用精确 `HashSet` 保存 `(loc, slot)`，例如编码为一个 `long`。当单个 pair 的 witness 数超过 `companion.hll.threshold` 时，可以切换到 HyperLogLog 估算。

要求：

- HLL 切换是单向的。
- 切到 HLL 的 pair 必须写入 `_hll_pairs/` 副输出。
- HLL pair 的 count 允许在 baseline diff 中使用相对误差判断；非 HLL pair 必须精确。
- 如果新增 `companion.hll.threshold` 配置，需同步补到 `companion-conf.xml`、`CompanionConf` 和 `docs/architecture.md`。

## 必须保持的契约

- 输入 `PairKey` 已保证 `vidA < vidB`，不要重新排序导致对象语义混乱。
- 主输出固定为 tab 分隔：`vidA\tvidB\tcount`。
- `count` 的含义固定为 distinct `(loc, slot)` 数。
- `count >= companion.k.min` 才能输出。
- `_hll_pairs/` 只能作为副输出，不要污染主输出 schema。

## Counter

Counter 组名固定为 `STAGE2`。

| Counter | 含义 |
|---|---|
| `PAIRS_INPUT` | mapper 每读入一条 witness 加 1 |
| `PAIRS_OUTPUT` | 每输出一个过阈值 pair 加 1 |
| `HLL_FALLBACK_COUNT` | 每个切换到 HLL 的 pair 加 1 |

`PAIRS_INPUT` 应和 Stage 1 的 `PAIRS_EMITTED` 接近。`HLL_FALLBACK_COUNT / PAIRS_OUTPUT > 1%` 时需要重新评估阈值和热点分布。

## 性能要求

- Combiner 必须有效，目标是显著减少 Stage 2 shuffle。
- 精确路径优先使用 `HashSet<Long>`，不要默认给所有 pair 建 HLL。
- Reducer 流式处理 pair，处理完一个 pair 后释放 witness 集合。
- 默认 reducer 数：1d 为 8，7d 为 32，31d 为 128。
- 如果 reducer max/mean > 2，需要分析热点 pair、salt 策略或 HLL 提前切换阈值。

## 验收信号

- `STAGE2.PAIRS_INPUT` 约等于 `STAGE1.PAIRS_EMITTED`。
- 输出文件能被 Stage 3 按 `vidA\tvidB\tcount` 解析。
- 非 HLL pair 与 baseline 的 count 精确一致。
- `PAIRS_OUTPUT` 明显小于输入 witness 规模，说明阈值过滤生效。
