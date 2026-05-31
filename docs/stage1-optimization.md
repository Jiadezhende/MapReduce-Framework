# Stage1 性能优化需求

## TL;DR

1d 数据下 Stage1 **单趟** wall-time ≈ 10 min，双趟串行 ≈ 20 min。最大异常点是单趟内 reduce 平均 task time 仅 3.3 min（197 s），wall-time 却拖到 10 min——**倾斜系数 ≈ 3×**，叠加 1 个 killed reducer 的重试。先治倾斜，再谈双趟并行。

参考计数器：`log/stage1_counter`（单趟）。算法语义无硬错（见 [stage1-boundary-gap](stage1-boundary-gap.md)），本文不涉及正确性变更。

---

## 1. 现状基线（单趟，1d 输入 57 MB / 7.97 M 条）

| 指标 | 值 | 备注 |
|---|---|---|
| Map 任务数 | 2 | 57 MB ÷ 默认 128 MB block |
| Reduce 任务数 | 8（含 1 个 killed） | `stage1.reducers` 默认 |
| Map slot time | 87 s | 健康 |
| Reduce task time（总） | 1,575,864 ms ≈ 26 min | 8 reducer 累计 CPU 时间 |
| Reduce slot time（总） | 6,303 s | slot 含 4× memory 因子 |
| **Reduce 平均 task time** | **197 s ≈ 3.3 min** | 平均每 reducer 实际跑 |
| **单趟 wall-time** | **~10 min** | 最慢 reducer ≈ 3× 平均 → 严重倾斜 |
| **双趟 wall-time（compensation on）** | **~20 min** | j1a + j1b **串行** |
| Map spilled records | 15.94 M = 2× map output | 多一轮 spill |
| Reduce input groups | 182,214 | 不同 `(loc, slot)` |
| PAIRS_EMITTED | 958.9 M | within 647.6 M + cross 311.3 M |
| HDFS 写入 | 2.76 GB（压缩） | 单趟 |

**关键异常**：平均 task time 3.3 min vs wall-time 10 min，意味着某个 reducer 单独跑了 ~10 min（其他几乎闲置），再加上 killed reducer 的失败 + 重启周期。这是单趟性能的主要矛盾。

---

## 2. 问题分类

### 2.1 配置类（不动代码，改 conf）

| ID | 问题 | 现值 | 期望 | 对单趟 10min 的预期收益 |
|---|---|---|---|---|
| S1-C1 | Reducer 偏少，倾斜后单点更突出 | `companion.stage1.reducers=8` | 24~32 | 中（拉平倾斜需配合 S1-A3） |
| S1-C2 | 热 loc 跑满 reducer 但 cap 形同虚设 | `companion.loc.skew.cap=200000` | 5,000~20,000 | **高**（直接砍掉拖尾 reducer） |
| S1-C3 | Reducer 容器内存偏小（killed 1 个，疑似 OOM） | `mapreduce.reduce.memory.mb` 默认 | 4096，`-Xmx3072m` | **高**（消除 killed→重试） |
| S1-C4 | Map split 过大，并行度=2 | 未设 | `split.maxsize=16m` → map 数 ~8 | 低（map 本来就只 44s） |
| S1-C5 | Map spill 2 次 | `mapreduce.task.io.sort.mb=100` | 512；`spill.percent=0.85` | 低 |

### 2.2 算法实现类（改代码）

| ID | 问题 | 文件:行 | 影响 |
|---|---|---|---|
| **S1-A1** | **`SkewAwarePartitioner` 仅 `hash(loc, slot/2)`，热 loc 全压一个 reducer**——单趟 10 min 的主因 | `Stage1Job.java:186-209` | **高** |
| S1-A2 | 补偿两趟 j1a/j1b **串行**提交，整体 2× | `Stage1Job.java:104-111` | 高（单趟治好后才显现） |
| S1-A3 | 补偿两趟重复发射 within-slot pair，把 Stage2 入口翻倍 | `Stage1Job.java:286-294`（within-slot 在两趟都跑） | 中（主要利好 Stage2） |
| S1-A4 | Reducer 每条 `new SeenRecord(...)`，单趟 ~8 M 次分配，加剧热 reducer GC | `Stage1Job.java:249` | 中 |

---

## 3. 优化需求

### 3.1 算法实现类（先做，是单趟 10 min 的根因）

**REQ-S1-A1 / 热 loc salt 拆分（最高优先级）**
- 现状：`SkewAwarePartitioner.getPartition` 用 `HashUtil.mix(loc, (slot+offset)/2)`，单 loc 的所有 slot 配对永远去同一个 reducer。1d 数据下平均 reduce task 3.3 min，wall-time 却 10 min——单点拖尾 3×。
- 改造：
  1. 引入 `companion.stage1.loc.salt.threshold`（默认 50,000 record/loc）。Mapper 阶段对每 `(loc)` 维持一个计数 sketch（轻量 CMS 或简单 HashMap，按 loc 滚动估算），超过阈值的 loc 在 partitioner 里追加 salt 维度：`mix(loc, (slot+offset)/2, vid % saltN)`，否则保持原 key。
  2. 配套修改 grouping comparator：salt 维度只作为分区辅助键，reduce 端 grouping 仍按 `(loc, slot)`。
- 难点：salt 把同 `(loc, slot)` 的记录分到不同 reducer 后，单 reducer 看不到完整人群，配对会少。因此**salt 仅对超阈值 loc 启用**，并需在 reducer 端针对 salted loc 用近似 dedup（或接受少量漏配，由 S1-A3 的两遍补偿覆盖）。如复杂度过高，退化到 REQ-S1-C2（仅收紧 locSkewCap），代价是丢一些热 loc 内的配对。
- 验收：单 reducer wall-time 最大 / 平均 < 1.5；单趟 wall-time 从 10 min 降到 4~5 min。

**REQ-S1-A2 / 补偿两趟改并发提交**
- 现状：`Stage1Job.run()` 顺序调 `runSinglePass(A)` → `runSinglePass(B)`，两 Job 间无数据依赖。单趟治好后双趟串行成为新瓶颈。
- 改造：用 `Job.submit()` + 双 `isComplete()` 轮询并发提交，两 Job 共享 YARN 队列资源。
- 前置条件：队列容量 ≥ 2× 单 Job 占用，否则两 Job 互相排队反而更慢。
- 验收：双趟 wall-time ≈ max(j1a, j1b) 而非 sum；预期从 ~20 min → ~5 min（叠加 A1 后）。

**REQ-S1-A3 / 补偿两趟去除重复 within-slot pair**
- 现状：j1a 和 j1b 都对每个 `(loc, slot)` 完整发射 within-slot pair；Stage2 端靠 dedup 吃掉。
- 改造：在 `Stage1Reducer.setup` 读 `slotOffset`，offset==1 时 `emitWithinSlotPairs` 直接返回；j1a 负责所有 within-slot + cross-slot(2k→2k+1)，j1b 只发射 cross-slot(2k+1→2k+2)。
- 影响：j1b 数据量从 ~958 M 降到 ~311 M，j1b 单趟 wall-time 也会随之缩短 ~2/3；Stage2 输入量整体砍 ~34%。
- 验收：j1b 的 `PAIRS_EMITTED` ≈ `CROSS_SLOT_PAIRS`（不再含 within-slot）。

**REQ-S1-A4 / Reducer 对象复用**
- 现状：`new SeenRecord(value.getVid(), value.getTNorm())` 每条新建，~8 M 次/趟，加剧热 reducer GC（也是 killed task 的可能诱因之一）。
- 改造：`window` 和 `tailBuffer` 改为两个 `int[] vids` + `int[] ts`，环形队列实现；`SeenRecord` 类删除。
- 验收：reduce 阶段 GC time 下降；CPU time 持平或下降。

### 3.2 配置类（先上 C2/C3 吃快收益，A1 落地后再调 C1）

**REQ-S1-C2 / 让 locSkewCap 实际生效（最快见效）**
- `companion.loc.skew.cap` 从 200,000 改为 8,000；观察 `HOT_LOCS_SLICED` 计数器，>100 再回调。
- 与 A1 的关系：cap 是兜底丢弃策略——丢掉过热 loc 的尾部 record，避免单 reducer 拖死；A1 是分担策略——把热 loc 拆给多 reducer 算。cap 见效快但**会损失正确性**（丢 record），A1 复杂但无损。
- **临时方案**（A1 没就绪时）：先调 cap 救场；A1 上线后回调 cap 到 50,000 仅作兜底。
- 验收：单 reducer wall-time 抖动收敛；正确性副作用以 `HOT_LOCS_SLICED` / `SKEW_DROP` 计数监控。

**REQ-S1-C3 / Reducer 内存兜底**
- 提交参数加 `-D mapreduce.reduce.memory.mb=4096 -D mapreduce.reduce.java.opts=-Xmx3072m`。
- 验收：`Killed reduce tasks=0`，单点重试不再吞 wall-time。

**REQ-S1-C1 / 提高 Reduce 并行度**
- `companion.stage1.reducers` 默认从 8 改为 32。**前置**：必须 A1 已落地，否则单点没拆分，加再多 reducer 也只压一个。
- 验收：reduce slot time 总量持平，单 reducer wall-time 减半。

**REQ-S1-C4 / 提高 Map 并行度**
- `-D mapreduce.input.fileinputformat.split.maxsize=16777216`。
- 验收：map 任务数从 2 升到 7~8。**这一项几乎不影响单趟 wall-time**（map 本来就 44s），主要是为了让 shuffle 早开始。优先级最低。

**REQ-S1-C5 / 降低 Map spill 次数**
- `-D mapreduce.task.io.sort.mb=512 -D mapreduce.task.io.sort.spill.percent=0.85`。
- 验收：`Spilled Records (Map) ≈ Map output records`。优先级最低。

---

## 4. 上线顺序与预期收益

| 批次 | 内容 | 单趟 wall-time | 双趟 wall-time |
|---|---|---|---|
| 现状 | — | 10 min | 20 min |
| 第 1 批 | C2（cap=8000）+ C3（reducer 内存）救场 | 6~7 min | 12~14 min |
| 第 2 批 | A1（热 loc salt）+ A4（对象池） | 3~4 min | 6~8 min |
| 第 3 批 | A2（双趟并发）+ A3（j1b 跳过 within-slot） | 3~4 min | **3~5 min** |
| 第 4 批（按需） | C1（reducer=32）+ C4/C5（map 调参） | 同上 | 微调 |

---

## 5. 验收口径

- **单趟 wall-time**：1d 输入下 ≤ 4 min；最慢 reducer / 平均 < 1.5。
- **双趟 wall-time**：≤ 5 min（compensation 仍启用）。
- **资源**：`Killed reduce tasks=0`；GC time 占比 < 5%。
- **正确性**：`tests/cluster_test.sh stage1` 与基线 diff 行数比例保持现状（本次优化不变更语义）；A1 的 salt 路径需用小规模 fixture 单独验证不漏配。
- **Stage2 联动**：Stage2 输入量下降 ≥ 34%（来自 A3）。
