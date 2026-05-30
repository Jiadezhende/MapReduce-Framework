# tests/data/fixtures — 跨 stage golden fixtures

由 `tests/data/mini.csv`（**整文件**）经 stage0/1/2 **理想**参考实现产出的 fixture，供单 stage 隔离测试（`cluster_test.sh` + 各 stage 的 JUnit `LocalJobTest`）做 byte-level 对账。

| 文件 | 内容 | 当前大小 | 行/记录数 |
|---|---|---|---|
| `filtered.seq` | `SequenceFile<NullWritable, RecordWritable>`，`BLOCK + DefaultCodec` (gzip) | ~350 KB | 58 105 |
| `pair_loc_slot.seq` | `SequenceFile<PairKey, LocSlotWritable>`，`BLOCK + DefaultCodec` (gzip) | ~10 MB | 4 487 006 witnesses |
| `companions.csv` | 文本 `vidA,vidB,count`，按 `(count desc, pair asc)` 排序 | ~720 KB | 53 946 |

> 体积说明：原版（10k 行子集 + 缺陷设计）只有 ~500 KB 总和；改为全量 mini.csv + 理想语义后未压缩超 80 MB，BLOCK+gzip 压回 10 MB 量级。DefaultCodec 在 hadoop-common 自带，无需 native，`fs -text` / `SequenceFile.Reader` 自动识别。

三份文件相互一致：把 `filtered.seq` 喂入正确的 Stage 1 实现得到的输出，去重 `(loc, slot)` 后与 `pair_loc_slot.seq` 等价；继续跑 Stage 2 + Stage 3 排序得到的就是 `companions.csv`。

字节布局合约见 [docs/fixtures.md](../../../docs/fixtures.md)；端到端语义规范见 [docs/reference-semantics.md](../../../docs/reference-semantics.md)。

## 派生方式与配置

- **输入**：`tests/data/mini.csv` 整文件（100 000 行）。`FixtureGenerator` 不再做前缀截取——避免与 `cluster_test.sh` 喂给 stage0 的输入不一致。
- **配置**：`companion.t0=1420041600`、`delta.t=300`、`slot.size=300`、`k.min=3`（[common/src/main/resources/companion-conf.xml](../../../common/src/main/resources/companion-conf.xml) 的默认值）。
- **参考实现**：[common/src/test/java/companion/io/FixtureGenerator.java](../../../common/src/test/java/companion/io/FixtureGenerator.java)。
  - Stage 0：双 pass，丢弃 vid 出现次数 `< 2` 的记录，`tNorm = ts - t0`；CSV 解析逐字镜像 `Stage0CsvParser.parseRecord`。
  - Stage 1：**按 `loc` 全局分组**滑窗，`|Δt| ≤ delta.t` 配对；pair 归属到**后到记录**的 slot。**不做 `slot/2` 分区，不丢边界**。这是 ideal 语义；生产 `Stage1Job` 当前不满足（详见 [docs/stage1-boundary-gap.md](../../../docs/stage1-boundary-gap.md)）。
  - Stage 2：按 PairKey 聚合，distinct `(loc, slot)` 计数，过滤 `< k.min`，按 `(count desc, pair asc)` 排序。

## 下游单测怎么用

R3 / R4 / R5 单测可以选择以下任一对账模式：

1. **classpath 加载本目录的 .seq**：在自己模块的 `src/test/resources/` 软链或拷贝路径（也可直接读 `tests/data/fixtures/*.seq`）。
2. **自己手写 `src/test/resources/` 内置 fixture**：仍以 [docs/fixtures.md](../../../docs/fixtures.md) 字节布局为权威，与本目录 .seq decode 后字段一致即可。

字节级 diff 比较的是**应用层 (key, value)**——SequenceFile 容器头、压缩 codec 不参与比较。所以你的 fixture 用 Snappy / Default / NONE 任何一种压缩都行。

## `cluster_test.sh --stage stage1` 预期红

stage1 fixture 表达 ideal 语义；生产 `Stage1Job` 还在跑 `(loc, slot/2)` 分区设计。所以：

- `cluster_test.sh --stage stage0` / `stage2` / `stage3` → 期望绿
- `cluster_test.sh --stage stage1` → 期望 **EXPECTED-FAIL**（脚本会以 exit 0 退出但打印 diff）。golden vs cluster 缺失行数应在 golden 总量的 **15–25%**，对应 `docs/stage1-boundary-gap.md` §3 的 boundary loss 估算。J1b 上线后这条会自动恢复绿。

如果 diff 比例显著偏离 15–25%，说明 stage1 生产代码引入了非 boundary 类型的差异，需立即排查。

## 何时重新生成

跑 [scripts/regenerate_fixtures.sh](../../../scripts/regenerate_fixtures.sh) 重生 `.seq` + `companions.csv`，以下任一情况触发：

- 修改了 `RecordWritable` / `PairKey` / `LocSlotWritable` 任一字节布局
- 修改了 `FixtureGenerator` 中的 Stage 0/1/2 参考实现
- 修改了 `companion.t0` / `delta.t` / `slot.size` / `k.min` 任一默认值
- 修改了 `mini.csv` 内容（不太可能）
- J1b 上线、stage1 production 改为符合 ideal 语义后——此时 `pair_loc_slot.seq` 内容不变（fixture 一直是 ideal），但 `cluster_test --stage stage1` 应从 EXPECTED-FAIL 转为 PASS；若不转说明 J1b 没完全收敛 boundary loss

[FixtureGoldenRoundTripTest](../../../common/src/test/java/companion/io/FixtureGoldenRoundTripTest.java) 会在上述情况下失败，并在错误信息里提示重生。
