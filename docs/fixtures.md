# Fixtures — 跨 stage 数据边界字节合约

本文档是 `filtered.seq`、`pair_loc_slot.seq`、`companions.csv` 三种跨 stage 数据边界的字节级权威。R3 / R4 / R5 手写单测 fixture 时以本文档为准；任何 fixture 与本文档不一致都按"本文档赢"处理。

字节布局以 [common/src/main/java/companion/io/](../common/src/main/java/companion/io/) 下的 Writable 源码为唯一真理。源码变更必须在同一个 PR 内同步更新本文档；反过来，本文档先行变更但未改源码无效。文档维护人：R1（详见 [roles.md](roles.md)）。

**范围**：只规范应用层 `(key, value)` 字节布局与文本行格式。SequenceFile 容器头、sync marker、压缩块边界由 Hadoop 自行处理，不在本文档范畴；跨 stage diff 在解压、剥离容器层之后做应用层比较。

---

## §1 `filtered.seq` — Stage 0 输出 / Stage 1 输入

- 容器：`SequenceFile<NullWritable, RecordWritable>`，block 压缩；codec 由集群 Hadoop 默认配置决定。
- 路径：`${COMPANION_ROOT}/filtered/{phase}/part-*`，`{phase} ∈ {1d, 7d, 31d}`。
- Key：`NullWritable`，零字节，不参与排序。
- Value：[`RecordWritable`](../common/src/main/java/companion/io/RecordWritable.java) 定长 12 字节。

### Value 字节布局（big-endian）

| Offset | 字节 | 类型 | 字段 | 语义 |
|---|---|---|---|---|
| `[0, 4)` | 4 | int32 BE | `vid` | 车辆 ID |
| `[4, 8)` | 4 | int32 BE | `loc` | 地点 ID |
| `[8, 12)` | 4 | int32 BE | `tNorm` | `ts - companion.t0`（秒），默认 `t0 = 1420041600` |

### 关键不变量

- `tNorm` **已经**扣除 `companion.t0`；Stage 1 mapper 拿到后**禁止再减一次**。
- 输出按 Hadoop SequenceFile 规则写出，单条记录可由 `NullWritable.readFields` + `RecordWritable.readFields` 顺序反序列化。
- `RecordWritable` 字段顺序固定为 `(vid, loc, tNorm)`，不能调换。

### 最小有效记录（十六进制）

```
# value=RecordWritable, hex bytes, big-endian, no key (NullWritable)
# vid=1, loc=0, tNorm=2     → 00 00 00 01  00 00 00 00  00 00 00 02
# vid=2, loc=0, tNorm=26    → 00 00 00 02  00 00 00 00  00 00 00 1A
# vid=3, loc=1, tNorm=0     → 00 00 00 03  00 00 00 01  00 00 00 00
```

`hdfs dfs -text` 解码后形如：

```
1,0,2
2,0,26
3,1,0
```

（来自 `RecordWritable.toString()`，仅用于人工巡检，**不是**任何 stage 的真实输入格式。）

---

## §2 `pair_loc_slot.seq` — Stage 1 输出 / Stage 2 输入

- 容器：`SequenceFile<PairKey, LocSlotWritable>`，Snappy block 压缩。
- 路径：`${COMPANION_ROOT}/pair_loc_slot/{phase}/part-*`。
- Key：[`PairKey`](../common/src/main/java/companion/io/PairKey.java) 定长 8 字节。
- Value：[`LocSlotWritable`](../common/src/main/java/companion/io/LocSlotWritable.java) 变长，2 个 VInt。

### Key 字节布局（big-endian）

| Offset | 字节 | 类型 | 字段 | 语义 |
|---|---|---|---|---|
| `[0, 4)` | 4 | int32 BE | `vidA` | 较小的车辆 ID |
| `[4, 8)` | 4 | int32 BE | `vidB` | 较大的车辆 ID |

**强约束**：`vidA < vidB`，由 `PairKey.set` 强制（[PairKey.java:31-40](../common/src/main/java/companion/io/PairKey.java#L31-L40)）。`vidA == vidB` 或 `vidA > vidB` 的字节模式在 fixture 中**禁止出现**，否则下游反序列化通过、但语义已经被破坏。

排序：`PairKey` 的 byte-wise lexicographic 比较即等价于 `(vidA asc, vidB asc)`，因为字段都是 big-endian 且 `vidA` 在前。

### Value 字节布局（Hadoop VInt × 2）

| 顺序 | 类型 | 字段 |
|---|---|---|
| 1 | VInt | `loc` |
| 2 | VInt | `slot` |

Hadoop VInt 编码（`WritableUtils.writeVInt` / `readVInt`）摘要：

- 值落在 `[-112, 127]` 内：占 **1 字节**，直接以补码写入。常见 `loc`、`slot` 多在这个区间，因而每条 value 通常 2 字节。
- 否则首字节是 length tag（负值表示位数与符号），后跟 1–8 字节大端 magnitude；最坏情况 9 字节，整个 value 最长 10 字节。

### 关键不变量

- 同一对 `(vidA, vidB)` 在不同 `(loc, slot)` 上可以出现多条，每条是一个独立 witness；Stage 2 负责按 `(loc, slot)` 去重再计数。
- `slot = tNorm / companion.slot.size`（整数除法），`slot.size` 默认 300（[architecture.md:34](architecture.md#L34)）。

### 最小有效记录（十六进制）

```
# Record 1
# key=PairKey(vidA=1, vidB=2): 00 00 00 01  00 00 00 02
# value=LocSlotWritable(loc=0, slot=0): 00 00
# ---
# Record 2 — same pair, next slot
# key=PairKey(1, 2)           : 00 00 00 01  00 00 00 02
# value=LocSlotWritable(0, 1) : 00 01
# ---
# Record 3 — different pair, larger loc
# key=PairKey(1, 3)           : 00 00 00 01  00 00 00 03
# value=LocSlotWritable(5, 2) : 05 02
```

反例（**不允许**出现在 fixture）：

- `PairKey(2, 1)`：`set()` 会强制重排为 `(1, 2)`；直接 `write` 已倒序的 PairKey 是绕过 API、违反合约的行为。
- `PairKey(1, 1)`：`set()` 抛 `IllegalArgumentException`。

---

## §3 `companions.csv` — Stage 2 主输出 / Stage 3 输入 / Stage 3 重整后

- 路径：
  - Stage 2 主输出：`${COMPANION_ROOT}/companions/{phase}/part-*`
  - Stage 3 重整：`${COMPANION_ROOT}/final/{phase}/companions.csv/part-*` 和 `${COMPANION_ROOT}/final/{phase}/top_n.csv`
- 格式：UTF-8 纯文本。
- 行格式：`vidA,vidB,count\n`。
- **分隔符：逗号**，与原始 mini.csv、[baseline](../baseline/README.md) 主输出保持一致。
- 无表头，无空行，行尾 `\n`。

### 字段语义

| 字段 | 类型 | 语义 |
|---|---|---|
| `vidA` | int | 较小的车辆 ID，与 `PairKey.vidA` 一致 |
| `vidB` | int | 较大的车辆 ID，`vidB > vidA` |
| `count` | long | 该 pair 的 distinct `(loc, slot)` witness 数（[stage2/README.md:17-19](../stage2/README.md#L17-L19)）；`long` 语义，不允许溢出截断 |

### 关键不变量

- `vidA < vidB`：文本输出仍然保持顺序约束，便于下游 dedup 和 diff。
- `count >= companion.k.min`（默认 3）：Stage 2 已过滤；Stage 3 不再过滤。
- 排序：
  - **Stage 2** `companion/companions/{phase}/part-*`：不保证全局有序，仅保证同一 reducer 内 PairKey 升序。
  - **Stage 3** `final/{phase}/companions.csv/part-*`：按 `count` 全局**降序**有序（[stage3/README.md:29](../stage3/README.md#L29)）。Stage 3 单测用的手写 fixture 必须降序排列。
- HLL 副输出 `companion/companions/{phase}/_hll_pairs/`（[stage2/README.md:37](../stage2/README.md#L37)）不在本契约范畴，由 R4 自定义 schema，但不能污染 `part-*` 主输出。

### 最小有效记录

```
1,2,3
1,3,5
2,3,12
```

第一行 `count = 3` 恰好等于 `k.min` 默认值，是允许保留的边界。真实 Stage 2/3 输出**绝不**包含注释行或空行；下游解析器禁止跳过任何以 `#` 开头的行。

---

## §4 跨 stage 对账机制

[roles.md:68](roles.md#L68) 规定的对账流程：每个 stage owner 在自己模块的 `src/test/resources/` 手写本 stage **输入**的二进制 fixture，单测先绿；上游 stage 完工后，`mvn verify` 把上游真实输出与下游 fixture 做 diff，**字节级一致才算 pass**。

diff 比较的对象是**应用层 `(key, value)` 序列**：

- 上游 SequenceFile 解 Snappy 块、剥离 sync marker 后，按 `keyClass + valueClass` 反序列化得到记录流。
- 同样反序列化下游 fixture。
- 逐记录字段比对。

SequenceFile 容器头里的元数据（class 名、压缩 codec、sync 间隔）不参与 diff——只要 keyClass / valueClass 与本文档一致即可。

---

## §5 变更流程

任何对 Writable 字节字段（数量、顺序、类型、编码方式）的变更，按 [architecture.md §4 末尾](architecture.md#L75) "破坏性变更"流程：

1. 源码与本文档同 PR 修改。
2. 通知 R1 + 所有下游 stage owner。
3. `companion-parent` minor 版本号 +1。
4. 跑 [scripts/regenerate_fixtures.sh](../scripts/regenerate_fixtures.sh) 重生 `tests/data/fixtures/*.seq` 与 `companions.csv` 并入库。

`companions.csv` 文本 schema 变更（增列、改分隔符、改字段顺序、改 EOL 约定）同样按破坏性变更走。

---

## §6 真实 golden fixtures

[tests/data/fixtures/](../tests/data/fixtures/) 下入库了从 [tests/data/mini.csv](../tests/data/mini.csv) 前 10 000 行经参考 Stage 0/1/2 实现派生的真实 fixture，供 R3 / R4 / R5 单测做最小正确性 diff：

| 文件 | 容器 | 行数 | 大小 |
|---|---|---|---|
| `filtered.seq` | `SequenceFile<NullWritable, RecordWritable>`, NONE | 2 191 | ~44 KB |
| `pair_loc_slot.seq` | `SequenceFile<PairKey, LocSlotWritable>`, NONE | 25 361 | ~458 KB |
| `companions.csv` | 文本 `vidA,vidB,count`, count desc + pair asc | 37 | ~442 B |

三个文件互相一致：把 `filtered.seq` 喂入正确的 Stage 1 实现，去重 `(loc, slot)` 后与 `pair_loc_slot.seq` 等价；继续跑 Stage 2 + Stage 3 排序后等于 `companions.csv`。

参考实现位于 [common/src/test/java/companion/io/FixtureGenerator.java](../common/src/test/java/companion/io/FixtureGenerator.java)，可作为各 stage 的算法对照。完整文件用途、规模权衡、重生时机详见 [tests/data/fixtures/README.md](../tests/data/fixtures/README.md)。

字节布局合约变更后必须重生：

```bash
scripts/regenerate_fixtures.sh
```

[FixtureGoldenRoundTripTest](../common/src/test/java/companion/io/FixtureGoldenRoundTripTest.java) 是这套 fixture 的不变量守护者：Writable 字节布局或参考实现漂移时它会失败并指示重生。
