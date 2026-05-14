# tests/data/fixtures — 跨 stage golden fixtures

由 `tests/data/mini.csv` 前 10 000 行经参考 Stage 0/1/2 实现产出的真实 fixture，供 R3 / R4 / R5 单测做最小正确性 diff。

| 文件 | 内容 | 当前大小 | 行/记录数 |
|---|---|---|---|
| `filtered.seq` | `SequenceFile<NullWritable, RecordWritable>`，CompressionType=NONE | ~44 KB | 2 191 |
| `pair_loc_slot.seq` | `SequenceFile<PairKey, LocSlotWritable>`，CompressionType=NONE | ~458 KB | 25 361 |
| `companions.csv` | 文本 `vidA,vidB,count`，按 `(count desc, pair asc)` 排序 | ~442 B | 37 |

三份文件相互一致：把 `filtered.seq` 喂入正确的 Stage 1 实现得到的输出，去重 `(loc, slot)` 后与 `pair_loc_slot.seq` 等价；继续跑 Stage 2 + Stage 3 排序得到的就是 `companions.csv`。

字节布局合约见 [docs/fixtures.md](../../../docs/fixtures.md)。

## 派生方式与配置

- 输入：`tests/data/mini.csv` 的**前 10 000 行**（硬编码在 [FixtureGenerator.LINES](../../../common/src/test/java/companion/io/FixtureGenerator.java)；快速验证目的，不需要可调）。
- 配置：`companion.t0=1420041600`、`delta.t=300`、`slot.size=300`、`k.min=3`（即 [common/src/main/resources/companion-conf.xml](../../../common/src/main/resources/companion-conf.xml) 的默认值）。
- 参考实现：[common/src/test/java/companion/io/FixtureGenerator.java](../../../common/src/test/java/companion/io/FixtureGenerator.java)。
  - Stage 0：双 pass，丢弃 vid 出现次数 `< 2` 的记录，`tNorm = ts - t0`。
  - Stage 1：按 `(loc, slot/2)` 分区，分区内 `|Δt| ≤ delta.t` 的滑动窗口配对；pair 归属到**后到记录**的 slot。跨 `slot=2k+1 → 2k+2` 的配对**不**输出（与 [stage1/README.md](../../../stage1/README.md) 已知限制一致）。
  - Stage 2：按 PairKey 聚合，distinct `(loc, slot)` 计数，过滤 `< k.min`。
  - Stage 3 sort：count desc，二级 key 按 PairKey asc。

## 下游单测怎么用

R3 / R4 / R5 单测可以选择以下任一对账模式：

1. **classpath 加载本目录的 .seq**：在自己模块的 `src/test/resources/` 软链或拷贝路径（也可直接读 `tests/data/fixtures/*.seq`）。
2. **自己手写 `src/test/resources/` 内置 fixture**：仍以 [docs/fixtures.md](../../../docs/fixtures.md) 字节布局为权威，与本目录 .seq decode 后字段一致即可。

字节级 diff 比较的是**应用层 (key, value)**——SequenceFile 容器头、压缩 codec 不参与比较。所以你的 fixture 用 Snappy / Default / NONE 任何一种压缩都行。

## 何时重新生成

跑 [scripts/regenerate_fixtures.sh](../../../scripts/regenerate_fixtures.sh) 重生 `.seq` + `companions.csv`，以下任一情况触发：

- 修改了 `RecordWritable` / `PairKey` / `LocSlotWritable` 任一字节布局。
- 修改了 `FixtureGenerator` 中的 Stage 0/1/2 参考实现。
- 修改了 `companion.t0` / `delta.t` / `slot.size` / `k.min` 任一默认值。
- 修改了 `mini.csv` 内容（不太可能）。

[FixtureGoldenRoundTripTest](../../../common/src/test/java/companion/io/FixtureGoldenRoundTripTest.java) 会在上述情况下失败，并在错误信息里提示重生。
