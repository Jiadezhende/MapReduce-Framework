# Stage1 桶内 Pair 去重 — 问题定位与方案评审

> 状态：**待评审**。本文档不直接合并进 `docs/stage1-optimization.md`，作为独立评审材料；评审通过后再以 REQ-S1-A0 形式登记进主优化文档。

## TL;DR

`Stage1Reducer` 在单个 `(loc, slot)` 桶内对同一对车 `(V1, V2)` 重复发射 pair-witness，重复倍数约等于 `nA × nB`（V1、V2 在该桶内的记录数乘积）。`PAIRS_EMITTED=958,942,040` 中相当大一部分是这类重复，Stage2 端靠 `HashSet<Long>` 去重吃掉。改造方案是在 reducer 内加 per-bucket `Set<Long> emittedPairs`，within-slot 与 cross-slot 两条发射路径统一查表 first-seen 才下发。预计 Stage1 输出量可下降至少 50%，直接缓解 Stage2 磁盘崩盘问题。

但**改造涉及计数器语义、内存上界、与补偿趟和倾斜 cap 的交互**，需要评审定夺。

---

## 1. 现状与症状

### 1.1 观测到的现象

`log/stage1_counter`（1d 输入，单趟）：

```
INPUT_RECORDS        = 7,971,686
Reduce input records = 7,971,686
Reduce input groups  = 182,214       （distinct (loc, slot) 桶数）
PAIRS_EMITTED        = 958,942,040
  within-slot 部分   ≈ 647,641,435   （= PAIRS_EMITTED - CROSS_SLOT_PAIRS）
  cross-slot 部分    = 311,300,605
HDFS bytes written   = 2,765,600,382  （2.76 GB，压缩）
```

衍生：

- 平均 emit/桶 = 958 M / 182 K ≈ **5,257 emit/桶**
- 平均 record/桶 = 7.97 M / 182 K ≈ **44 record/桶**
- 桶内若无任何重复发射，理论上限 `C(44, 2) = 946 emit/桶`；观测 5,257 是它的 ~5.5×

仅靠平均无法区分这 5.5× 是来自**长尾热桶**还是**桶内重复发射**——但代码分析表明两者都贡献，且重复发射可证伪、可消除。

### 1.2 下游表现

- Stage1 → HDFS 中间产物 2.76 GB（双趟 5.5 GB），Stage2 mapper 几乎原样落本地盘
- Stage2 单 worker shuffle/spill 量峰值 ~15 GB → 50 GB 单盘集群触发 NM UNHEALTHY → AM lost（参见本次 application_1515238638289_0083 故障）
- Stage2 reducer 端 `HashSet<Long>` 长期承担去重压力，HLL fallback 阈值高，热 pair 单 reducer 易 OOM

---

## 2. 根因分析

### 2.1 代码定位

`stage1/src/main/java/companion/stage1/Stage1Job.java`：

**within-slot 发射路径**（行 286–294）：

```java
private void emitWithinSlotPairs(int loc, int slot, SeenRecord cur, Context context) ... {
    while (!window.isEmpty() && window.peekFirst().ts < cur.ts - deltaT) {
        window.pollFirst();
    }
    for (SeenRecord prev : window) {          // window 可包含同一 vid 的多条记录
        emitPair(prev.vid, cur.vid, loc, slot, context);
    }
}
```

**cross-slot 发射路径**（行 270–284）：

```java
private void emitCrossSlotPairs(int loc, int slot, SeenRecord cur, boolean canUseTail, ...) {
    if (!canUseTail) return;
    for (SeenRecord prev : tailBuffer) {      // tailBuffer 同样可含同 vid 多条
        if (cur.ts - prev.ts > deltaT) continue;
        if (emitPair(prev.vid, cur.vid, loc, slot, context)) {
            ...CROSS_SLOT_PAIRS++;
        }
    }
}
```

**核心 emit**（行 296–306）：

```java
private boolean emitPair(int vid1, int vid2, int loc, int slot, Context context) ... {
    if (vid1 == vid2) return false;        // 只跳过 self-pair
    outKey.set(vid1, vid2);                 // PairKey.set 已归一 vidA<vidB
    outValue.set(loc, slot);
    context.write(outKey, outValue);
    ...PAIRS_EMITTED++;
    return true;
}
```

**`rebuildTailBuffer`**（行 308–320）：把上一个 `(loc, slot)` 桶尾部 deltaT 内的全部记录搬入 tailBuffer，**不按 vid 去重**。

### 2.2 重复发射的形成机制

#### within-slot

设 `(loc=A, slot=k)` 桶内：
- V1 有 nA 条记录，时间 t11 < t12 < … < t1nA
- V2 有 nB 条记录，时间 t21 < t22 < … < t2nB
- 全部在彼此 deltaT 窗口内

按 `FullKeyComparator` 时间序处理，window 是先进先出的时间窗。对每个新到来的 `cur`，遍历当前 window 中 vid 不同的 prev 发射。可以证明对于每一对 (V1@t1i, V2@t2j)，只要它们之间时序与窗口约束允许，就会被发射 1 次。总发射次数 = `nA × nB`，**全部 tagged 为 `(loc=A, slot=k)`**，Stage2 dedup → 1 个 witness。

#### cross-slot

补偿分区把 `(slot=k, slot=k+1)` 放进同一 reducer，`tailBuffer` 在 reducer 处理完 slot=k 时被 rebuild 为该桶尾部 deltaT 内的全部记录（按时间）。处理 slot=k+1 时，对每个 cur 遍历整个 tailBuffer 发射。

设 tailBuffer 中 V1 有 nA' 条、slot=k+1 中 V2 有 nB' 条且对落 deltaT 内：发射 `nA' × nB'` 次，**全部 tagged 为 `(loc, slot=k+1)`**（cross-slot emit 用当前槽作为标签，见行 280）。

#### 两条路径共享同一 witness 标签

`emitCrossSlotPairs` 与 `emitWithinSlotPairs` 在同一 reduce 调用内、对同一 `slot` 发射。一个 `(V1, V2)` 既可能通过 tail 跨进来（slot=k 的 V1 + slot=k+1 的 V2），也可能再通过 within（slot=k+1 内的另一条 V1 + 另一条 V2）相遇——两路全部 tagged `(loc, k+1)`，Stage2 视作同一 witness。**所以可以用一个 dedup set 同时压缩两路。**

### 2.3 重复倍数的数据依赖

- 若每个 `(vid, loc, slot)` 平均仅 1 条记录（GPS 帧粒度 ≥ slot size，或 loc 空间粒度非常细），则 `nA = nB = 1`，无重复
- 若每个 `(vid, loc, slot)` 平均 3 条（GPS ~30s 一帧、slot=300s、loc 较粗），则 nA×nB ≈ 9，重复倍数 9×
- 实际值取决于业务数据，**评审需要先跑一次小规模 sample 测出真实分布**（见 §6.1）

---

## 3. 改造方案

### 3.1 核心改动

在 `Stage1Reducer` 中加入桶级 dedup set，within/cross 两条路径统一查表：

```java
public static class Stage1Reducer extends Reducer<...> {
    private final HashSet<Long> emittedPairs = new HashSet<>();    // 新增
    // ... 其余字段不变 ...

    @Override
    protected void reduce(CompositeKey key, Iterable<RecordWritable> values, Context context) ... {
        int loc = key.getLoc();
        int slot = key.getSlot();
        boolean canUseTail = ...;
        if (!canUseTail) tailBuffer.clear();

        window.clear();
        emittedPairs.clear();          // 新增：每个 (loc, slot) 桶开始时清空
        // ... 其余逻辑不变 ...
    }

    private boolean emitPair(int vid1, int vid2, int loc, int slot, Context context) ... {
        if (vid1 == vid2) return false;
        int a = Math.min(vid1, vid2);
        int b = Math.max(vid1, vid2);
        long packed = ((long) a << 32) | (b & 0xffffffffL);
        if (!emittedPairs.add(packed)) {       // 新增：first-seen 才下发
            return false;
        }
        outKey.set(vid1, vid2);
        outValue.set(loc, slot);
        context.write(outKey, outValue);
        ...PAIRS_EMITTED++;
        return true;
    }
}
```

### 3.2 改动范围

| 文件 | 改动 |
|---|---|
| `Stage1Job.java` | `Stage1Reducer` 新增 `emittedPairs` 字段；`reduce` 顶部 `clear()`；`emitPair` 加查表 |
| `Stage1JobTest.java` | 新增 fixture：同 (vid, loc, slot) 多帧的输入，期望 Stage1 输出 distinct (pair, loc, slot) |
| `cluster_test.sh` 期望基线 | Stage1 输出行数会下降；需要与改造后 baseline 重新对齐 |
| 主优化文档 `stage1-optimization.md` | 评审通过后追加 REQ-S1-A0 章节 |

### 3.3 预期收益

- **Stage1 PAIRS_EMITTED**：从 958 M 降到 distinct (pair, loc, slot) 数。下界由真实 vid-loc-slot 分布决定，先以**保守估 50%** 衡量。
- **Stage1 HDFS 写**：成比例下降。
- **Stage2 输入 / shuffle / 落盘**：成比例下降。**这是当前集群磁盘崩盘的直接缓解**。
- **Stage2 reducer 内存峰值**：HashSet/HLL 处理量同步下降。
- **Stage1 reducer CPU**：少了大量重复 `context.write` 调用 + 序列化。

---

## 4. 风险与边界情况（评审重点）

### 4.1 dedup set 的内存上界

- 最坏情况：单桶 distinct vid 数 = D，distinct pair 数 = `C(D, 2)`
- `HashSet<Long>` 单元素开销 ~48 B（Long 装箱 24 B + HashMap.Node 32 B + table 摊销）
- D=1,000 → 500K pair → 24 MB；D=5,000 → 12.5 M pair → 600 MB
- 当前 `locSkewCap=200,000`（record 数，不是 vid 数）形同虚设；若热桶 vid 数破万，set 直接打爆 reducer 堆

**评审建议**：
- 加 `emittedPairs.size()` 上限保护（如 5,000,000，约 240 MB），超过则**关闭 dedup 改回原始模式**继续处理（牺牲此桶的去重收益，保住任务存活）；新增计数器 `DEDUP_DISABLED_BUCKETS`
- 或者：复用 `locSkewCap` 的丢弃逻辑，把"vid 数太多"也纳入 SKEW_DROP
- 评审需明确：set 满了之后是**退化模式继续跑** vs **触发与 SKEW_DROP 同等的丢弃**

### 4.2 计数器语义变更

| 计数器 | 改造前 | 改造后 |
|---|---|---|
| `PAIRS_EMITTED` | 原始发射次数（含重复） | distinct (pair, loc, slot) 数 |
| `CROSS_SLOT_PAIRS` | cross 路径原始发射次数 | cross 路径**首次**成功发射的 distinct (pair, loc, slot) 数（即"非 cross 不会被发现"的真实贡献量） |

`CROSS_SLOT_PAIRS` 的语义变化尤其需要注意：当前实现下 cross emit 在 within emit **之前**调用，所以如果同一 pair 既能跨槽又能槽内见到，`CROSS_SLOT_PAIRS` 会捕获（因为是首发）；within emit 会被 dedup 拒掉。**这意味着 `CROSS_SLOT_PAIRS` 数会增大其在 `PAIRS_EMITTED` 中的占比**，但对应的语义解释也变了——它代表的是"该 distinct witness 是否依赖 cross 路径"。

**评审需明确**：
- 是否保留旧计数器口径？如要保留，需要新增 `PAIRS_EMITTED_RAW` 单独统计未去重的发射次数
- 历史 baseline / `cluster_test.sh` 期望值需重新对齐

### 4.3 与 SKEW_DROP / locSkewCap 的交互

当前 `locSkewCap` 检查在 emit 之后（行 253–260）：

```java
emitCrossSlotPairs(...);
emitWithinSlotPairs(...);

if (window.size() >= locSkewCap) {
    SKEW_DROP++;
    HOT_LOCS_SLICED++;
    continue;            // 不加入 window，但 emit 已经发了
}
window.addLast(cur);
```

加入 dedup set 后：
- `cur` 即使被 SKEW_DROP 不加 window，它对应的 pair（之前 emit 出去的）已经在 set 里；后续不会再重复发——**这部分语义和原来的"emit 已发"是一致的**
- set 本身的内存可能在 SKEW_DROP 触发前就已经爆掉

**评审需明确**：dedup set 容量上限是否应该与 `locSkewCap` 联动（例如 set 上限 = `locSkewCap × avg_pairs_per_record`）

### 4.4 cross-slot dedup 的正确性

需要确认以下断言：

> 同一对车 (V1, V2) 通过 cross-slot 路径或 within-slot 路径在同一 reduce 调用内发射时，witness tag 必然相同（即 `(loc, current_slot)`）。

代码确认（行 280 和行 292）：

```java
// cross-slot
if (emitPair(prev.vid, cur.vid, loc, slot, context)) { ... }
//                              ^^^^^^^ 当前 reduce 调用的 slot
// within-slot
emitPair(prev.vid, cur.vid, loc, slot, context);
//                          ^^^^^^^ 同样是当前 slot
```

确认成立。dedup 不会跨 `(loc, slot)` 桶错误合并。

### 4.5 与补偿趟 (j1a / j1b) 的交互

- j1a 与 j1b 是两个独立 Job，各自 reducer 各自维护 `emittedPairs`，**互不干扰**
- 同一 `(loc, slot=k)` 桶可能在 j1a 和 j1b 各自的某次 reduce 调用中都被处理（取决于 partition 落入哪一对槽），各自 emit 1 个 `(V1, V2, loc, k)`，两趟合计 2 条记录写出
- Stage2 dedup 把这 2 条合并为 1 个 distinct witness——和当前行为一致，**没有正确性回归**

如果同步落地 [REQ-S1-A3](stage1-optimization.md)（j1b 跳过 within-slot），则 j1b 只处理 cross 路径，dedup set 只装 cross 路径 emit 的 pair，内存压力更小。

**评审建议**：两项一起评，落地顺序 A0 先于 A3。

### 4.6 Set 实现选择

- `HashSet<Long>` 装箱开销大，可以换 **open-addressing 的 long-keyed set**（如 eclipse-collections `LongHashSet`，或者手写 linear-probe `long[]` + 占用位）
- 单元素开销可从 48 B 降到 ~12–16 B，相同内存能装 3–4× 元素
- 评审需决定是否引入第三方依赖；如果不想加依赖，手写 `LongOpenHashSet` 工作量约半天

### 4.7 emit 失败时 SKEW_DROP 的计数

当前 `emitCrossSlotPairs` 用 `if (emitPair(...) returns true) CROSS_SLOT_PAIRS++`。改造后 dedup 失败也会让 emitPair 返回 false——**不影响 CROSS_SLOT_PAIRS 的新语义**（它就是该数），但要确认这是评审接受的解释。

---

## 5. 与其他优化项的关系

| 项 | 关系 |
|---|---|
| REQ-S1-A1（热 loc salt 拆分） | **正交**，可同时落地。salt 把热 loc 跨 reducer 拆分后，每个 reducer 看到的桶 distinct vid 数变小，dedup set 内存上界同步下降，互相利好 |
| REQ-S1-A3（j1b 跳过 within-slot） | **协同**，建议一起评审。A3 把 j1b 数据量再砍 ~2/3，dedup 收益叠加 |
| REQ-S1-A4（reducer 对象池） | **协同**。A4 消除 `new SeenRecord(...)` 的分配，A0 消除冗余 emit；都减轻 reducer GC |
| Stage2 优化（PairPartitioner salt） | **下游受益**。A0 把 Stage2 输入砍掉一半左右，salt 拆分的收益基础更大 |
| 集群磁盘崩盘问题 | **直接缓解**。Stage2 落盘量随 A0 输入下降同比例下降，单 worker 峰值从 ~15 GB 估降到 5–8 GB，留出健康检查阈值的缓冲 |

---

## 6. 验证计划

### 6.1 改造前先测真实重复倍数（验证 ROI）

不改代码，仅加临时计数器跑一次小样本（如 1h 数据）：

- 在 `emitPair` 顶部加 `emittedPairsProbe.add(packed)` 但**仍照常发射**
- 在 `reduce` cleanup 时把 `emittedPairsProbe.size()` 写 counter `PROBE_DISTINCT_PAIRS`
- 与 `PAIRS_EMITTED` 对比：**比值就是 A0 落地后的压缩倍数**
- 比值 < 1.5 → A0 收益小，不必投入；比值 > 3 → A0 收益巨大，优先做
- **预期评审先看这个数再决定是否上**

### 6.2 单元 / fixture 测试

新增 fixture：
- 输入：单 `(loc=1, slot=0)` 内 V1=10、V2=20 各 5 条不同 ts 的 record，全部在 deltaT 内
- 期望（改造前）：Stage1 输出 5×5 = 25 条 `(10, 20, 1, 0)`
- 期望（改造后）：Stage1 输出 1 条 `(10, 20, 1, 0)`
- Stage2 最终输出对**两种情况都应为同一结果**（如 kMin=1 时输出 1 对，count=1）

跨 slot fixture：
- 输入：V1 在 slot=0 有 3 条尾部记录，V2 在 slot=1 有 3 条头部记录，跨 deltaT 全部覆盖
- 期望（改造后）：1 条 `(V1, V2, loc, 1)`（cross 路径首发）

### 6.3 cluster_test 端到端

- Stage2/Stage3 最终输出应**逐字节一致**（dedup 不变更语义）
- Stage1 输出行数下降是预期；需要更新 `tests/cluster_test.sh` 的 Stage1 期望或改为比较 distinct witness 数而非原始行数

### 6.4 性能验收

| 指标 | 现状 | A0 落地后目标 |
|---|---|---|
| Stage1 PAIRS_EMITTED（单趟） | 958 M | ≤ 500 M（实测真实重复倍数后定值） |
| Stage1 HDFS 写（单趟） | 2.76 GB | ≤ 1.4 GB |
| Stage1 reducer 单 task time（平均） | 197 s | ≤ 120 s |
| Stage1 reducer 内存峰值 | 未测 | ≤ 1 GB（含 dedup set） |
| Stage2 单 worker 本地盘峰值 | ~15 GB（推测） | ≤ 8 GB |
| 1d Stage2 wall-time | 跑不完 | ≤ 15 min（与 PairPartitioner salt 配合可再降） |

---

## 7. 待评审 Open Questions

1. **§6.1 的 probe 测试谁来跑、什么时候跑？** A0 的 ROI 完全取决于真实重复倍数，先有这个数再决定是否投入工程量。
2. **dedup set 内存上限**：定 5 M 还是 10 M？超限后退化模式还是 SKEW_DROP？
3. **计数器是否保留旧口径？** 是否新增 `PAIRS_EMITTED_RAW`？
4. **HashSet 实现是否换 primitive long set？** 是否引入第三方依赖（如 eclipse-collections）？
5. **A0 与 A3 的落地顺序？** 建议 A0 先（无语义变化），A3 后（涉及补偿语义边界）。
6. **fixture 与 cluster_test 基线如何对齐？** 是改 fixture 期望，还是改 cluster_test 的比对粒度（按 distinct witness）？
7. **是否在改造同时把 `emitWithinSlotPairs` 的 window 改成按 vid 去重**？即 window 内同 vid 只保留最新一条，与 dedup set 配合可省 emit 调用，但语义需仔细推敲（pair 之间时间差判定可能改变）——倾向**不做**，保持 dedup 单点改造。

---

## 8. 评审通过后的落地任务清单

1. 跑 §6.1 的 probe 测真实重复倍数（先决条件）
2. 实现 §3.1 代码改动 + §4 的内存上限保护
3. 添加 §6.2 fixture 测试
4. 更新 `Stage1Counter` 注释 / 新增 `PAIRS_EMITTED_RAW`（按评审决定）
5. 更新 `tests/cluster_test.sh` 期望
6. 在 `docs/stage1-optimization.md` 追加 REQ-S1-A0 章节并链回本文档
7. 1d 数据回归 + 性能指标记录
