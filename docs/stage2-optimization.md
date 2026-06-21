# Stage2 性能优化需求

> 状态：**问题定位 / 待优化**。本文档记录 Stage2 shuffle 爆盘根因与候选优化项（S2-A* 编号）。

## TL;DR

1d 数据下 Stage2 wall-time ≥ 30 min，input 约 ~1.92 B 条 pair-witness（Stage1 双趟之和）。最大瓶颈是**算法实现层面**：`PairPartitioner` 没有用 `PAIR_SALT_N`，热点 vehicle pair 把整条 reducer 拖死。其次是 reducer 并行度 8 太小、HLL 阈值过高导致堆抖动。

参考截图：`application_1515238638289_0083`，状态 RUNNING，Running Containers=7，Allocated 7 vCore / 16 GB，30+ min 未结束。

---

## 1. 现状基线（1d 输入，Stage1 双趟输出之和）

| 指标 | 估算值 | 备注 |
|---|---|---|
| Input pair-witness | ~1.92 B 条 | Stage1 单趟 958 M × 2（两趟） |
| Input 物化大小 | ~5.5 GB（压缩） | Stage1 单趟 2.76 GB × 2 |
| Reduce 任务数 | 8 | `stage2.reducers` 默认 |
| 集群并发上限 | 7 容器 | 队列资源限制 |
| Wall-time | **>30 min（未完成）** | YARN 截图 RUNNING |

---

## 2. 问题分类

### 2.1 配置类

| ID | 问题 | 现值 | 期望 |
|---|---|---|---|
| S2-C1 | Reducer 偏少 | `companion.stage2.reducers=8` | 32~48（配合 S2-A1 salt 后才有意义） |
| S2-C2 | YARN 队列并发容量 7 | 集群配置 | 与集群侧确认能否放到 16+ |
| S2-C3 | HLL fallback 阈值过高，HashSet 撑爆堆 | `companion.hll.threshold=1_000_000` | 50,000~100,000 |
| S2-C4 | Reducer 容器内存 | `mapreduce.reduce.memory.mb` 默认 | 6144，`-Xmx4500m` |
| S2-C5 | Map split 默认，Stage2 mapper 数等于 Stage1 输出文件数 | 未设 | 设 `split.maxsize=32m` 拉高并行度 |

### 2.2 算法实现类

| ID | 问题 | 文件:行 |
|---|---|---|
| S2-A1 | **`PairPartitioner` 仅按 `(vidA, vidB)` 哈希**，`PAIR_SALT_N` 配置在 `CompanionConf` 里但 Stage2 完全没用 | `Stage2Job.java:99-108` |
| S2-A2 | Reducer 无二级聚合，热 pair 单 reducer 串行处理几千万行 witness | `Stage2Job.java:127-160` |
| S2-A3 | `DedupCombiner` 内 HashSet 无上限，热 pair 在 combiner 阶段就 GC | `Stage2Job.java:79-95` |
| S2-A4 | Mapper 纯 pass-through，浪费一次 spill；可在 mapper 端做 in-memory dedup | `Stage2Job.java:68-77` |
| S2-A5 | HLL precision=14 写死，1M HashSet → HLL 切换瞬间一次性 add 1M long | `Stage2Job.java:31, 137-141` |

> 已核：`PairKey.set` 在 vidA==vidB 时抛异常、否则按 vidA<vidB 归一（`PairKey.java:31-41`），无方向重复问题。

---

## 3. 优化需求

### 3.1 算法实现类（必须先做，否则配置调了也没用）

**REQ-S2-A1 / PairPartitioner 引入 salt（最高优先级）**
- 现状：`PairPartitioner.getPartition` 用 `HashUtil.mix(vidA, vidB) % numPartitions`，单个热 pair 的所有 witness 必然落到同一 reducer。即使加到 48 个 reducer，热 pair 也只占 1 个，30 min wall-time 不会变。
- 改造方案（两阶段聚合）：
  1. **Stage2 改双轮**：J2a 用 `mix(vidA, vidB, witness_salt)` 分区，salt = `loc * 31 + slot) % PAIR_SALT_N`；reducer 做局部 dedup/HLL，输出 `(pair, partial_witness_set 或 partial_HLL)`。J2b 用 `mix(vidA, vidB) % numPartitions` 分区，reducer 做最终合并（HashSet 求并集或 HLL register 取 max），过 `kMin` 后输出。
  2. **或单轮多 reducer 分片 + Stage3 合并**：J2 partition = `mix(vidA, vidB, salt)`，输出文件保留 salt 维度；Stage3 已经按 pair 聚合，可顺带做最终 dedup（需评估 Stage3 改动成本）。
- 推荐方案 1（双轮 J2），与 Stage1 双趟模型对称。
- `PAIR_SALT_N` 默认 16，1d 数据下 J2a 可放 64~128 个 reducer，J2b 16~32。
- 验收：单 reducer wall-time 标准差 / 平均值 < 0.3；wall-time ≤ 8 min。

**REQ-S2-A2 / Reducer 二级聚合（与 A1 配套）**
- 当 J2a 输出 partial HLL register 时，J2b reducer 不再持有完整 HashSet/HLL，仅做 register-wise max。
- 数据结构：`LocSlotWritable` 之外新增 `PartialAggWritable`，含 `byte[1<<HLL_PRECISION]` 或紧凑的 sparse 表示。
- 验收：J2b reducer 内存峰值 < 1 GB，无 OOM。

**REQ-S2-A3 / DedupCombiner 限上限**
- 现状：`DedupCombiner.reduce` 把 `Iterable<LocSlotWritable>` 全塞 HashSet。
- 改造：HashSet 达到上限（如 200,000）后直接 flush 已有 witness、清空、继续接收。combiner 本来就是 best-effort，不必保证全局唯一。
- 验收：combiner 阶段 GC time 下降，热 pair 不再卡 combiner。

**REQ-S2-A4 / Mapper 本地 dedup（可选，A1 落地后再评估）**
- 改造：在 `Stage2Mapper` 内维护 LRU `Map<PairKey, Set<Long>>`（容量如 10 万 pair），命中则吸收重复 witness 不下发。
- 风险：mapper 内存占用上升，需评估收益是否值得；A1 + combiner 已经能消化大部分重复，A4 优先级低。

**REQ-S2-A5 / HLL 切换路径优化**
- 现状：切换瞬间循环 `hll.add(exactWitness)` 把已有 HashSet 全部 hash 一遍。
- 改造：把 HashSet 替换为**直接维护 HLL register 的 sparse 表示**，达到稠密阈值后转 dense；避免一次性 1M add。
- 优先级低，配合 S2-C3 把阈值降到 100K 后这次性 add 也只剩 10 万次，可接受。

### 3.2 配置类（A1 落地后再调）

**REQ-S2-C1 / 提高 Reducer 并行度**
- J2a：64；J2b：32。`CompanionConf.STAGE2_REDUCERS_DEFAULT` 拆成 `STAGE2A` / `STAGE2B` 两个值。
- 验收：在集群容量允许下，平均 reducer wall-time < 3 min。

**REQ-S2-C2 / 集群队列容量**
- 与集群运维确认 `default` 队列能不能开到 16+ 容器；如不能，A1 之后即使并行度高，也会因排队失去意义。
- 备选：提交时 `-D mapreduce.job.queuename=<高优先队列>`。

**REQ-S2-C3 / 降低 HLL 阈值**
- `companion.hll.threshold` 从 1,000,000 改为 100,000。
- 影响：HLL 误差 ~0.81%（precision=14 下 typical），对 `kMin` 判定可接受。
- 验收：reducer 物理内存峰值下降，无 long-tail 单 pair 阻塞。

**REQ-S2-C4 / Reducer 内存兜底**
- `mapreduce.reduce.memory.mb=6144 -Xmx4500m`。
- 验收：A1 + A2 + C3 都落地后预期不再触发，但留兜底。

**REQ-S2-C5 / Map split 拉高并行度**
- `-D mapreduce.input.fileinputformat.split.maxsize=33554432`。
- 验收：Stage2 mapper 数从 ~8（Stage1 reducer 数）升到 ~30+，map 阶段时间缩短。

---

## 4. 上线顺序

1. **第 1 批（算法 PR，核心）**：S2-A1 + S2-A2（双轮 J2a/J2b + salt）。这是把 30 min 压回 10 min 以内的唯一路径。
2. **第 2 批（配置）**：S2-C1、S2-C3、S2-C4、S2-C5 一起上。
3. **第 3 批（按需）**：S2-A3 combiner 上限；S2-A5 HLL sparse；S2-A4 mapper LRU。

> 注意联动：本文档的提升幅度依赖 [stage1-optimization](stage1-optimization.md) 的 **REQ-S1-A3**（补偿不重复 within-slot pair），它直接把 Stage2 输入量砍 ~34%。两份文档建议同 sprint 推进。

---

## 5. 验收口径

- **wall-time**：1d 输入下 Stage2 端到端 ≤ 8 min（J2a + J2b 合计）。
- **资源**：单 reducer wall-time 最大值 / 平均值 < 1.5（无明显倾斜）。
- **正确性**：
  - `Stage2Counter.PAIRS_OUTPUT` 与改造前差异 < HLL 误差范围（±1%）。
  - `tests/cluster_test.sh stage2` 与基线 diff 行数维持。
  - HLL fallback 计数（`HLL_FALLBACK_COUNT`）显著上升属预期（C3 把阈值降了）。
