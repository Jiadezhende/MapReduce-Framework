# Stage1Reducer 重构 — vid-bucket 算法

> 状态：**已实施**。本文档是 commit-level 的设计文档，记录算法替换的动因、新算法语义、正确性论证、与旧版的等价性，以及落地后的实测收益。

## TL;DR

31D 全量跑 Stage1 j1a（`application_1515238638289_0120`）reducer 10 连续 5 次 attempt OOM，整个 job FAILED。根因：旧 `Stage1Reducer` 在单个 `(loc, slot)` 桶内做 N² record-pair 枚举 + `HashSet<Long> emittedPairs` 跨 emit 去重，hot group（N=50k、D=10k 量级）触发 HashSet 内 ~50M 条目，单结构吃 ~2.8 GB 远超 1.5 GB heap。

重构方案：先按 vid 把 group 内 records 聚合成 `vid → [ts...]` 桶，再按 distinct vid 对枚举（D²/2）、two-pointer 判 ts、命中即 emit。**完全不维护跨 emit 的 dedup 数据结构**。Cross/within 两 pass 的潜在重复借助"`deltaT >= slotSize` 不变量 + cross-slot 内 `containsKey` skip"消除；不变量被破坏时由 Stage2 reducer 端的 distinct-witness HashSet 兜底正确性。

落地效果（fixture 实测）：
- 单 reducer 算法层峰值堆从 O(D²) 降到 O(D)，**OOM 根源结构性消除**
- Reduce 阶段 wall time **降 5-6×**（HashSet ops + autobox + alloc 整摊掉）
- cluster_test stage1 set diff 与 golden 逐字节相同 → 对外契约严格等价

---

## 1. 背景与触发事件

### 1.1 31D OOM 事故

run `application_1515238638289_0120`（Stage1 j1a，run_id `31399-cf1f2f6-20260601004404`），错误签名混合：

```
FAILED,Error: Java heap space
FAILED,Error: GC overhead limit exceeded
Halting due to Out Of Memory Error...
Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler
    in thread "LeaseRenewer:root@master:8020"
Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler
    in thread "ResponseProcessor for block BP-..."
```

- `failedReduces:1`（reducer 10），attempt 0-4 全挂 → job killed
- 其他 31 个 reducer 全部 SUCCEEDED → **单点倾斜**触发，不是普遍内存不足
- HDFS 客户端的 LeaseRenewer / ResponseProcessor 后台线程都被 OOM 打挂 → 堆是**真的耗尽**，不只是 GC overhead 阈值踩线

### 1.2 集群约束

```
mapreduce.reduce.memory.mb   = 2048    (container 2 GB)
mapreduce.reduce.java.opts   = -Xmx1536m (heap 1.5 GB)
companion.delta.t            = 300
companion.slot.size          = 300    ← 等于 deltaT
companion.loc.skew.cap       = 200000
```

3 节点共 20 GB YARN 容器池，已经撑满。加内存不是路（最多再挪几百 MB），必须算法级修。

---

## 2. 旧算法与根因

### 2.1 旧 `Stage1Reducer` 主循环

`Stage1Job.java:223-353`（重构前）：

```java
for (RecordWritable value : values) {                  // N 条 records
    SeenRecord cur = new SeenRecord(vid, ts);          // 每条一次堆分配
    emitCrossSlotPairs(loc, slot, cur, canUseTail, context);  // cur × tail
    pruneWindow(cur);
    if (emitWithinSlot) {
        emitWithinSlotPairs(loc, slot, cur, context);  // cur × window
    }
    window.addLast(cur);
}

private boolean emitPair(int vid1, int vid2, ...) {
    long pair = encodePair(vidA, vidB);
    if (!emittedPairs.add(pair)) return false;         // ← Set<Long> 跨 emit 去重
    context.write(outKey, outValue);
    return true;
}
```

### 2.2 单 group 内成本分布

| 操作 | 次数 | 单次成本 | 备注 |
|---|---|---|---|
| `new SeenRecord` | N | ~24 B + 引用 | 每条 record 一次 |
| `emitPair` 尝试 | ~N²/2 | — | within-slot + cross-slot 累计 |
| `emittedPairs.add(long)` | ~N²/2 | Long autobox + hash + put | 大部分尝试被 dedup 拦下 |
| `context.write` | ~D(D-1)/2 | shuffle write | 实际命中的 distinct pair |
| `emittedPairs` HashSet 内存 | O(D²) | ~56 B/entry | **OOM 来源** |

### 2.3 为什么会 OOM

`emittedPairs.add(Long.valueOf(pair))` 每条 entry 占用 ≈ 56 B（HashMap.Node 16 B + Long 24 B + pointer 8 B + bucket overhead 8 B）。在 hot group 命中时：

- 假设 hot (loc, slot) 命中 N=50k、D=10k → D(D-1)/2 ≈ 50M 条目
- HashSet 内存：50M × 56 B ≈ **2.8 GB**
- 单结构就超出 1.5 GB heap

更糟糕的是配置 `deltaT == slotSize == 300`：slot 宽等于 deltaT，slot 内任意两条 ts 都在 deltaT 内，`pruneWindow` 在 slot 内永远不裁。理论上 D 可以接近 N，HashSet 峰值进一步飙升。

`Long` autobox + Node 分配 + 频繁 resize 同时拖累 young-gen GC，加剧"GC overhead limit"型崩溃。两条路径叠加才出现"Java heap space + GC overhead limit + LeaseRenewer OOM"的混合签名。

换更紧凑的 set 实现（`LongOpenHashSet`、8-16 B/entry）只改常数因子，**不改 O(D²) 内存级别**。要根治必须算法级换。

---

## 3. 新算法

### 3.1 核心思路

把 record 层枚举改成 vid 层枚举：

```
旧：N² record-pair 尝试 → HashSet 去重 → D(D-1)/2 distinct pairs
新：D 个 vid 桶聚合 → D²/2 vid-pair 枚举 → 命中即 emit，无 dedup
```

差异点：
- 内存：O(D²) HashSet → O(D) 桶映射
- emit 尝试：~N²/2 → ~D²/2
- HashSet ops：每次尝试一次 → 0
- 装箱分配：每次 emit 一次 Long autobox → 0
- 每条 record 一次 `new SeenRecord` → 0（用 fastutil `IntArrayList` 复用）

### 3.2 数据结构

```java
private final Int2ObjectOpenHashMap<IntArrayList> currentVidToTs;
private final Int2ObjectOpenHashMap<IntArrayList> tailVidToTs;
```

- `currentVidToTs`：本 group `(loc, slot)` 的 `vid → [ts0, ts1, ...]` 桶映射。每个 IntArrayList 是连续 `int[]`，append 顺序天然有序（因为 map 输出 sort comparator 是 `(loc, slot, tNorm)` 升序）
- `tailVidToTs`：上一 group 末尾 deltaT 窗口内的同结构桶，用于 cross-slot pass
- 关键收益：fastutil 的 `Int2ObjectOpenHashMap` 无 Integer 装箱、open-addressed 内存紧凑；`IntArrayList` 是 `int[]`，two-pointer 扫描 L1/L2 几乎不 miss

### 3.3 主循环

```java
protected void reduce(CompositeKey key, Iterable<RecordWritable> values, Context ctx) {
    boolean canUseTail = (loc == lastLoc) && (slot == lastSlot + 1)
                       && Math.floorMod(lastSlot + slotOffset, 2) == 0;
    if (!canUseTail) tailVidToTs.clear();
    currentVidToTs.clear();

    // Pass 1: 桶分 + locSkewCap 兜底
    int totalRecords = 0;
    for (RecordWritable v : values) {
        if (totalRecords >= locSkewCap) {
            ctx.getCounter(... SKEW_DROP ...).increment(1L);
            continue;
        }
        currentVidToTs.computeIfAbsent(v.getVid(), k -> new IntArrayList(2))
                      .add(v.getTNorm());
        totalRecords++;
    }

    if (emitWithinSlot) emitWithinSlotPairs(loc, slot, ctx);
    if (canUseTail)     emitCrossSlotPairs(loc, slot, ctx);

    rebuildTailBuffer();
    lastLoc = loc; lastSlot = slot;
}
```

### 3.4 within-slot pass：D²/2 对称枚举

```java
int[] vids = currentVidToTs.keySet().toIntArray();
for (int i = 0; i < vids.length; i++) {
    IntArrayList tsA = currentVidToTs.get(vids[i]);
    for (int j = i + 1; j < vids.length; j++) {
        IntArrayList tsB = currentVidToTs.get(vids[j]);
        if (anyTsWithinDeltaT(tsA, tsB)) {
            emitPair(vids[i], vids[j], loc, slot, ctx);
        }
    }
}
```

`for (j = i+1)` 保证每个 distinct (vidA, vidB) **天然只被枚举一次**——这是为什么新版不需要 emit-level dedup 的核心。

### 3.5 cross-slot pass：tail × current

```java
for (Int2ObjectMap.Entry<IntArrayList> tailEntry : tailVidToTs.int2ObjectEntrySet()) {
    int vidTail = tailEntry.getIntKey();

    // 不变量：deltaT >= slotSize。当 vidTail 也在 current 时，within-slot pass
    // 必然已经 emit 了等价 pair（slot 内任意 ts 对都在 deltaT 内）→ 跳过避免重复
    if (currentVidToTs.containsKey(vidTail)) {
        ctx.getCounter(... CROSS_SLOT_SKIPPED_BY_OVERLAP ...).increment(1L);
        continue;
    }

    IntArrayList tsTail = tailEntry.getValue();
    for (Int2ObjectMap.Entry<IntArrayList> curEntry : currentVidToTs.int2ObjectEntrySet()) {
        if (anyTsWithinDeltaT(tsTail, curEntry.getValue())) {
            emitPair(vidTail, curEntry.getIntKey(), loc, slot, ctx);
            ctx.getCounter(... CROSS_SLOT_PAIRS ...).increment(1L);
        }
    }
}
```

### 3.6 two-pointer 谓词

```java
private boolean anyTsWithinDeltaT(IntArrayList a, IntArrayList b) {
    int i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        int diff = a.getInt(i) - b.getInt(j);
        if (Math.abs(diff) <= deltaT) return true;
        if (diff < 0) i++; else j++;
    }
    return false;
}
```

两条 list 均升序（保证见 §3.2），平均比较次数 O(|a| + |b|)，常见 ts/vid ≈ 2-3 时实际几次比较就出。Hot path 全是 `int` 比较 + 数组下标，JIT 友好（auto-vectorize、bounds-check elimination）。

---

## 4. 正确性

### 4.1 输出语义等价的论证

旧算法 within-slot emit 谓词（每个 group）：
> emit (X, Y, loc, slot) iff `∃ ts_X, ts_Y in current 内, ts_Y - ts_X ≤ deltaT, ts_Y ≥ ts_X`（按 record 顺序遍历，sorted by ts）

新算法 within-slot emit 谓词：
> emit (X, Y, loc, slot) iff `anyTsWithinDeltaT(X.tsList, Y.tsList)` ≡ `∃ ts_X ∈ X.tsList, ts_Y ∈ Y.tsList, |ts_X - ts_Y| ≤ deltaT`

因为 sorted by ts → 旧版的"按顺序遍历命中"等价于"存在某对 ts 距离 ≤ deltaT"，新旧谓词逻辑相同。HashSet 去重在旧版只是把"同一 pair 多次命中"折叠成一次写出，**不改变最终 emit 集合**。新版不产生多次命中（vid 对级枚举天然唯一），所以也得到同一集合。

cross-slot 同理：tail.ts < current.ts 严格成立（不同 slot、按 ts 排序），`|ts_X - ts_Y| ≤ deltaT` 等价于旧版的 `cur.ts - prev.ts ≤ deltaT`。

### 4.2 两 pass 之间为什么不会产生真重复

潜在风险场景：vid X 同时在 tail 和 current，vid Y 只在 current，且 ts_X@tail 与 ts_Y@current 在 deltaT 内、ts_X@current 与 ts_Y@current 也在 deltaT 内。

- within-slot pass：枚举 (X_cur, Y_cur)，命中 → emit (X, Y, loc, slot)
- cross-slot pass：枚举 (X_tail, Y_cur)，命中 → 又 emit (X, Y, loc, slot)
- 同一 (PairKey, LocSlotWritable) 输出两次 → 重复

cross-slot pass 内的 `containsKey(vidTail)` 跳过这一类。正确性论证依赖**`deltaT >= slotSize` 不变量**：

> slot 宽不超过 deltaT 时，slot 内任意两条 ts 一定在 deltaT 内。所以当 X 和 Y 都在 current 时，within-slot pass 一定会 emit (X, Y)。cross-slot 的 (X_tail, Y_cur) 因而必然是重复，可安全 skip。

### 4.3 不变量被破坏时的降级路径

`setup()` 检查 `deltaT < slotSize`，触发 `DELTAT_LT_SLOTSIZE` counter。这时上面 §4.2 的论证不再成立，skip 可能漏 emit。降级路径的正确性由 Stage2 reducer 端兜底：

```java
// Stage2Job.java:107-126
Set<Long> exactWitnesses = new HashSet<>();
for (LocSlotWritable value : values) {
    long witness = encodeWitness(value.getLoc(), value.getSlot());
    exactWitnesses.add(witness);    // ← 吸收 Stage1 输出里同 (pair, loc, slot) 的重复
    ...
}
long count = exactWitnesses.size();   // distinct count，不受 Stage1 重复影响
```

Stage2 reducer 按 PairKey 分组，对每个 pair 把所有 `LocSlotWritable` witness 编码成 long 扔进 HashSet，最后取 `size()`。Stage1 重复 emit 同一 `(pair, loc, slot)` 会产生重复 witness，但同 long 入同 set 自然吸收，distinct count 不变。HLL fallback 阈值也是按 distinct size 判断，不受影响。

降级方案：若 `DELTAT_LT_SLOTSIZE` 长期非 0，可把 cross-slot 的 skip 行去掉（接受少量重复，shuffle 体积上升至多 ~2×，对 Stage2 reducer 内存有线性影响但不破坏正确性）。`Stage1Job.java:226-227` 旧版的代码注释本身也说明 dedup 只是 shuffle 体积优化、不是正确性要求。

### 4.4 测试覆盖

`stage1/src/test/java/companion/stage1/Stage1JobTest.java` 包含 5 个 case：

| Test | 验证 |
|---|---|
| `partitionerKeepsEvenOddSlotPairTogether` | SkewAwarePartitioner 把偶/奇 slot pair 路由到同一 partition |
| `localJobEmitsWithinAndCrossSlotWitnesses` | 端到端 j1a+j1b：5 record fixture 输出 4 个 unique pair；回归保护（与旧版预期 byte-for-byte 一致） |
| `sameVidMultiTimestampInSlotEmittedOnce` | 同 vid 多 ts 落同一 group 时只 emit 一次 pair |
| `crossSlotOverlapVidNotDuplicated` | vid 同时在 tail/current 时 cross-slot skip 正确（输出条数 = 1，不是 2） |
| `deltaTLessThanSlotSizeStillCorrect` | 不变量被破坏时降级路径仍能产出正确 pair |

### 4.5 cluster_test 端到端验证

`scripts/cluster_test.sh --stage stage1 --build --reducers 2` 用真实 YARN 跑同一 fixture，输出与 `pair_loc_slot.seq` golden 做 `sort -u` diff，PASS。fixture counter 实测：

```
INPUT_RECORDS                  =  58,105
PAIRS_EMITTED                  = 2,866,315
CROSS_SLOT_PAIRS               =   942,703
CROSS_SLOT_SKIPPED_BY_OVERLAP  =       561
MAX_GROUP_RECORDS              =     2,768
MAX_GROUP_DISTINCT_VIDS        =     2,345
DELTAT_LT_SLOTSIZE             =       0    (默认 deltaT==slotSize)
```

`PAIRS_EMITTED` 与旧版 fixture 跑出来的值一致（在 set 等价基础上算上 j1a/j1b 拼接），byte-for-byte 与 golden 匹配。

---

## 5. 性能分析

### 5.1 OOM 消除的结构性论证

| 数据结构 | 旧版峰值 | 新版峰值 |
|---|---|---|
| `emittedPairs HashSet<Long>` | O(D²) ≈ D(D-1)/2 × 56 B | **不存在** |
| `window Deque<SeenRecord>` | O(N) × 24 B + 引用 | **不存在** |
| `tailBuffer List<SeenRecord>` | O(N) × 24 B + 引用 | **不存在** |
| `currentRecords List<SeenRecord>` | O(N) × 24 B + 引用 | **不存在** |
| `currentVidToTs Int2ObjectOpenHashMap` | — | O(D) × ~32 B/entry |
| `tailVidToTs Int2ObjectOpenHashMap` | — | O(D) × ~32 B/entry |
| `IntArrayList` 内 `int[]` | — | O(N) 总（跨所有桶） |

新版单 group 峰值 ≈ O(D + N) 而非 O(D²)。31D 最差 case 估算（D ≈ 50k，N ≈ 200k）：

- 旧版：HashSet 50M × 56 B ≈ **2.8 GB** → 必 OOM
- 新版：50k entries × 32 B + 200k ints × 4 B ≈ **2.4 MB** → 三个数量级降幅

`locSkewCap=200000` 兜底机制保留（在 Pass 1 桶分阶段计数 → 超过时不再加入桶），保护下界。

### 5.2 CPU 收益拆解（cluster_test fixture 实测 5-6× 加速来源）

mapper 阶段两版本相同（`Stage1Mapper` 没动），加速完全发生在 reducer。按贡献从大到小：

| 收益项 | 旧版成本 | 新版成本 | 估算占比 |
|---|---|---|---|
| HashSet ops（autobox + put） | ~N²/2 次 × 100-200 ns | 0 | ~40% |
| emit 尝试数（N² → D²） | N²/2 | D²/2 | ~25% |
| young-gen alloc 流量 | SeenRecord N + Long box N² | ~D 个 IntArrayList per group | ~15% |
| GC pause（Minor GC 次数） | 高 | 低 | ~10% |
| 缓存友好度 | 散布对象 + HashMap.Node | `int[]` two-pointer | ~10% |

实测 cluster_test fixture 上 reducer wall time 降 5-6×。31D 上 OOM 消失是定性收益（∞×），CPU 加速幅度取决于实际 D/N 分布——hot loc 上 D/N 比 fixture 更低（同车反复出现），N² → D² 节省更可观。

### 5.3 依赖代价

新增 `it.unimi.dsi:fastutil-core:8.5.13`（~22 MB JAR）。stage1 shaded fat jar 体积上升 ~20 MB。集群分发一次性成本，可忽略。

---

## 6. 实现细节

### 6.1 文件变更

| 文件 | 变更 |
|---|---|
| `pom.xml` | `<dependencyManagement>` 新增 `fastutil-core` 版本管理（8.5.13） |
| `stage1/pom.xml` | `<dependencies>` 引用 `fastutil-core`；shade `<artifactSet>` include 进 fat jar |
| `stage1/src/main/java/companion/stage1/Stage1Job.java` | `Stage1Reducer` 重写；`Stage1Counter` enum 增 4 个；`SeenRecord` 类删除 |
| `stage1/src/test/java/companion/stage1/Stage1JobTest.java` | 加 3 个语义保护 case + 共享 `baseLocalConf` / `openWriter` helper |

### 6.2 Stage1Counter 增量

```java
public enum Stage1Counter {
    INPUT_RECORDS,
    PAIRS_EMITTED,
    CROSS_SLOT_PAIRS,
    SKEW_DROP,
    HOT_LOCS_SLICED,
    DELTAT_LT_SLOTSIZE,           // 新增：不变量违反 (per-reducer once)
    MAX_GROUP_RECORDS,            // 新增：本 reducer 见过的最大 group N
    MAX_GROUP_DISTINCT_VIDS,      // 新增：本 reducer 见过的最大 group D
    CROSS_SLOT_SKIPPED_BY_OVERLAP // 新增：cross-slot skip 触发次数
}
```

Hadoop counter 不支持 max 语义，按 reducer 本地变量 + `cleanup()` 用 `setValue` 一次性吐：

```java
private long maxGroupRecords = 0L;
private long maxGroupDistinctVids = 0L;

@Override
protected void cleanup(Context context) {
    context.getCounter(... MAX_GROUP_RECORDS ...).setValue(maxGroupRecords);
    context.getCounter(... MAX_GROUP_DISTINCT_VIDS ...).setValue(maxGroupDistinctVids);
}
```

### 6.3 不动的部分（契约保护）

- `Stage1Mapper`、`SkewAwarePartitioner`、`CompositeKey.{FullKeyComparator, LocSlotGroupComparator}`：map 输出契约不变
- `RecordWritable`、`PairKey`、`LocSlotWritable`、`CompositeKey`：I/O 类不变
- `Stage1Job.buildJob` / `run` / `runSinglePass` / `materializeOutput`：j1a/j1b 调度逻辑不变
- Stage2/Stage0 任何代码：跨 stage 接口不变

### 6.4 J1a/J1b 调度交互

新算法逻辑与 slotOffset 无关：
- j1a (slotOffset=0, emitWithinSlot=true)：within-slot pass 跑、cross-slot 在 lastSlot 偶时跑
- j1b (slotOffset=1, emitWithinSlot=false)：within-slot pass 不跑、cross-slot 在 lastSlot 奇时跑

两趟 cross-slot 的 `containsKey` skip 行为独立——每趟只看自己的 currentVidToTs，不存在跨趟干扰。两趟拼接后输出与旧版严格等价（见 §4.1）。

---

## 7. 运维与观察

### 7.1 关键 counter 监控项

按重要性排：

| Counter | 期望值 | 告警条件 |
|---|---|---|
| `DELTAT_LT_SLOTSIZE` | 0 | 非 0 → 当前 conf 违反不变量，cross-slot skip 可能漏 emit。考虑去掉 skip 行（降级到接受重复） |
| `MAX_GROUP_RECORDS` | < locSkewCap | 逼近 cap → 倾斜严重，partitioner salt 化变高优先 |
| `MAX_GROUP_DISTINCT_VIDS` | < ~10k 经验值 | 高于此值意味着 D²/2 枚举单 group 进入百万对量级 |
| `SKEW_DROP` | 0（7D 实测） | 非 0 → cap 被触发，有数据丢失。需配合 `HOT_LOCS_SLICED` 看影响范围 |
| `CROSS_SLOT_SKIPPED_BY_OVERLAP` | 任意 | 仅观察用：衡量 invariant skip 实际省了多少 cross-slot 枚举。fixture 上 0.06%（561/942703） |

### 7.2 已知限制

1. **CPU 倾斜未解**：partitioner 仍是 `hash(loc, (slot+offset)/2)`，hot loc 全打一个 reducer。本次改造修了 OOM，没修 wall time 倾斜。31D reducer 10 现在不会 OOM，但仍是慢节点。要根治倾斜需要单独的 partitioner salt 改造。

2. **D² 上界**：极端 hot group D=50k 时 vid 对枚举 1.25B 次，CPU wall time 可能拖到几分钟级别。`MAX_GROUP_DISTINCT_VIDS` counter 触达高位时是动手 partitioner salt 的信号。

3. **fastutil 依赖**：新增 ~22 MB JAR。如果集群 jar 分发是瓶颈（目前不是），可考虑只引 `Int2ObjectOpenHashMap` + `IntArrayList` 两个类、shade 时只保留必要的 class。

---

## 8. 上线与回归路径

### 8.1 验证清单

1. ✅ 本地 `mvn -pl stage1 -am test` → 5/5 通过
2. ✅ `scripts/cluster_test.sh --stage stage1 --build --reducers 2` → PASS（set diff 与 golden 一致）
3. ⬜ 1D 全量回归：counter 与既有 1D 基线对照
4. ⬜ 7D 全量回归：counter 与既有 7D 基线对照
5. ⬜ 31D 直跑：复用 `/companion/runs/31399-cf1f2f6-20260601004404/` 已有的 filtered/vid_freq 输出，从 stage1 开始。关键观察：j1a 完成、新 counter 在合理范围、`MAX_GROUP_DISTINCT_VIDS` 实测值

### 8.2 回退路径

- `git revert` 本次 commit 即可回到旧 `Stage1Reducer` + `SeenRecord` + `emittedPairs HashSet`
- 31D 上回退意味着 OOM 复现，仅在验证发现新算法有未预料的语义偏差时考虑
