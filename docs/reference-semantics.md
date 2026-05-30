# Reference Semantics — Stage 0/1/2/3 Ideal Specification

## 1. Purpose

本文档是 stage0/1/2/3 **理想语义**的唯一规范来源。两份独立参考实现都对照本规范实现，互不依赖：

| 实现 | 语言 | 作用 | 入口 |
|---|---|---|---|
| `FixtureGenerator` | Java | 生成 `tests/data/fixtures/` 的 per-stage 中间产物，供 `cluster_test.sh` 做单 stage 隔离测试 | `common/src/test/java/companion/io/FixtureGenerator.java` |
| `baseline` | Python (Pandas / PySpark) | 端到端语义参考 + 小数据量性能分析，与 MR 整链路输出做 diff | `baseline/single_machine.py`、`baseline/spark_companion.py` |

跨语言双实现是为了互相校验——任何一边偏离本规范都会先被另一边发现。当前 production `Stage1Job` **不符合本规范**，差异规模见 `docs/stage1-boundary-gap.md`，修复在 J1b。

## 2. Inputs & Config

- **输入**：`tests/data/mini.csv`，整文件 100,000 行；CSV 格式 `vid,loc,ts`，三列均为非负整数
- **配置默认值**（来自 `common/src/main/resources/companion-conf.xml`，可被 `-D` 覆盖）：
  - `companion.t0 = 1420041600`（时间归一化基准）
  - `companion.delta.t = 300`（配对时间窗，秒）
  - `companion.slot.size = 300`（slot 长度，秒）
  - `companion.k.min = 3`（pair 保留阈值）

## 3. Stage 0 reference — 频次过滤 + 时间归一化

逐行解析 `vid,loc,ts`，三列均要求**非负整数**；`ts - t0` 必须 `≥ 0` 且 `≤ Integer.MAX_VALUE`，否则丢弃该行。

第一遍统计 `vid -> count`；第二遍输出 `(vid, loc, t_norm = ts - t0)`，仅保留 `count(vid) >= 2` 的行。

**Production deviation**：生产 `Stage0aFreqJob` 用 `BloomFilter` 近似 `count(vid) >= 2`，存在 false positive；本规范用精确计数。FP 表现为生产保留集合 ⊇ ideal 保留集合，差额由 `Stage0Bloom.KEY_EXPECTED_ENTRIES` 配置决定。

## 4. Stage 1 reference (ideal) — 全 loc 滑窗配对

按 `loc` **全局分组**（不做 `slot/2` 分区，不做热点丢弃）。每组按 `t_norm` 升序排序，滑窗维护 `[start, i)` 满足 `t_norm[i] - t_norm[start] ≤ delta_t`。

对每个 `i` 与窗内 `j ∈ [start, i)` 发出 witness：
- 跳过 `vid[i] == vid[j]`
- `vidA = min(vid[i], vid[j])`、`vidB = max(...)`
- `loc = 当前组的 loc`
- `slot = t_norm[i] / slot_size`（**归属到 LATER 记录**）

输出按 `(vidA, vidB, loc, slot)` 升序排序。

**明确：不做 `slot/2` 分区、不丢边界。** 这是与 production `Stage1Job` 的唯一语义差异——生产为了并行化把每个 loc 切成 `(loc, slot/2)` 子组，导致 `2k+1 → 2k+2` 跨 slot 配对永久丢失，详见 `docs/stage1-boundary-gap.md`。J1b 上线后这条 deviation 会被消除。

## 5. Stage 2 reference — PairKey 聚合 + 阈值过滤

对 stage1 witnesses 按 `(vidA, vidB)` 聚合，对每个 pair 维护 `distinct (loc, slot)` 集合，`count = |distinct cells|`。保留 `count >= k.min` 的 pair。

输出文本 `vidA,vidB,count`，按 `(count desc, vidA asc, vidB asc)` 排序——与 stage3 全局排序键一致，使 stage3 在已排序输入上幂等。

## 6. Stage 3 reference — 全局排序合约

输入 stage2 输出；按 `(count desc, vidA asc, vidB asc)` 全局排序后输出。在已排序输入上幂等。多 reducer 时使用 `TotalOrderPartitioner` 保证全局有序。

## 7. Production deviations 一览

| Stage | Production 偏离 | 表现 | 修复路线 |
|---|---|---|---|
| Stage 0 | BloomFilter FP（freq 近似） | 保留集 ⊇ ideal 保留集（multiset 超集），差额随 `KEY_EXPECTED_ENTRIES` 衰减 | 提高 `KEY_EXPECTED_ENTRIES`，或换精确实现。**测试侧用超集 + bounded delta 断言**（`Stage0LocalJobTest` 与 `cluster_test.sh --stage stage0`），不依赖 byte-equal |
| Stage 1 | `(loc, slot/2)` 分区 + `loc_skew_cap` 热点丢弃 | 单轮覆盖率 ~50%，10k 行下少 ~20% witnesses，规模化后 pair 少 ~70% | J1b：加一轮 `(loc, (slot+1)/2)` 偏移分区，合并去重；详见 `stage1-boundary-gap.md` §5 |
| Stage 2 | — | 与规范一致 | — |
| Stage 3 | — | 与规范一致 | — |

## 8. Cross-implementation parity checklist

| 规范条款 | FixtureGenerator (Java) | baseline (Python) |
|---|---|---|
| Stage 0 parse + freq + t-normalize | `parseRecord` / `stage0` | `main` (lines 118–125) |
| Stage 1 ideal grouping | `stage1` (group by `loc` only) | `build_pairs` (`mirror_stage1_limits=False` 分支) |
| Stage 1 mirror current production defect | — (不实现) | `build_pairs` (`mirror_stage1_limits=True` 分支) |
| Stage 2 distinct (loc,slot) + k.min + sort | `stage2` | `main` (lines 127–133 + 后续 filter/sort) |
| Stage 3 sort | 隐含于 `companions.csv` 已排序输出 | 隐含于最终 sort |

任何一份实现偏离本规范，应当先修实现，不修规范；规范修改需同时更新两份实现，并跑：

- `mvn -B clean verify` —— 确保 FixtureGenerator + JUnit 测试链路对齐
- `baseline/diff_baseline.py` —— 确保 baseline 与 MR 端到端 diff 仍可解释
