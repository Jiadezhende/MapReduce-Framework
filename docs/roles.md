# 分工

本文档是项目角色分配的唯一权威来源。各模块 README 顶部 `**Owner**` 字段只写代号，详细职责、Day 1 任务和整合报告分工以本文为准。

## 目标和约束

- **任务量均匀**：避免单点过载。
- **互不阻塞**：除 R1 一次性前置 PR 外，R2~R6 在 Day 1 即可并行开工。
- **R1 共识优先**：[common/](../common/) 模块是所有人的接口共识，必须最先合入；R1 PR 没合之前，其他人不写正式实现，只能写不依赖 common 的脚手架。

## 角色与负责模块

| 角色 | 负责模块 | 在整合报告中的角色 |
|---|---|---|
| **R1** | [common/](../common/)、[docs/architecture.md](architecture.md)、[docs/fixtures.md](fixtures.md)、[scripts/](../scripts/) 骨架 | §1 架构与契约章节，全文 review |
| **R2** | [stage0/](../stage0/)、[tools/profiler/](../tools/profiler/)、`scripts/split_by_day.sh` | §2.0 stage0 实现笔记 |
| **R3** | [stage1/](../stage1/) | §2.1 stage1 实现笔记（含 skew / salt 处理） |
| **R4** | [stage2/](../stage2/) | §2.2 stage2 实现笔记（含 HLL fallback） |
| **R5** | [stage3/](../stage3/)、[baseline/](../baseline/) | §2.3 stage3 笔记 + §3 正确性章节 |
| **R6** | [bench/](../bench/)、整合报告 stitching | §4 性能章节 + §5 复现性 + 串联全文 |

`R1~R6` 是临时代号。GitHub 账号确定后，本表和 CODEOWNERS 同步切换为 `@username`。

## R1 前置 PR：所有人的共识

R1 一次性把跨模块接口做齐做透，让 R2~R6 之后只 `import`、不再回来动 [common/](../common/)。PR 必须覆盖：

| 类别 | 交付项 | 现状 |
|---|---|---|
| Writable 字节布局 | `RecordWritable` / `CompositeKey` / `PairKey` / `LocSlotWritable` | 已落 |
| Job 入口 | `AbstractCompanionJob`，前两个参数固定 input / output | 已落 |
| 配置 key | `companion-conf.xml` + `CompanionConf` typed getter；**4 个 stage README 中出现的所有 key 全部预先声明**，包括暂时未实现的 stage-private key | 待逐 stage 核对补齐 |
| HDFS 路径 | `CompanionPaths` 工具类，把 `input/{phase}` → `filtered/{phase}` → `pair_loc_slot/{phase}` → `companions/{phase}` → `final/{phase}` 全部封装；stage 代码不允许出现 `/companion/...` 字面量 | 待新增 |
| Counter 名称 | 4 个 stage 的 13 个 Counter 名（见 [architecture.md §3](architecture.md#3-counter-naming)）做成常量类或 enum；stage 代码不允许字符串字面量 | 待新增 |
| Fixture schema | [docs/fixtures.md](fixtures.md) 规定 `filtered.seq` / `pair_loc_slot.seq` / `companions.csv` 的字节布局和最小有效记录 | 已落 |
| 时间 / 哈希工具 | `TimeUtil`、`HashUtil` | 已落 |
| Scripts 骨架 | `scripts/env.sh`、`scripts/run_pipeline.sh`、`scripts/upload_to_hdfs.sh` 雏形（参数定义、HDFS 路径用 `CompanionPaths` 输出） | 部分已落，需要校对 |

### "做齐"的两条机械化判据

R1 PR 想合入，必须通过：

1. `git grep '"companion\.'` 在 [common/](../common/) 之外**零命中**——没有任何 stage 代码硬编码配置 key。
2. `git grep '/companion/'` 在 [common/](../common/) 和 [scripts/](../scripts/) 之外**零命中**——没有任何 stage 代码硬编码 HDFS 路径。

后续 `.github/workflows/contract-guard.yml` 会把这两条做成 PR 必过检查。

### R1 PR 合入后

R1 的日常工作转为"被动 review 模式"：

- 仅当出现 Writable 字段变更、新的跨 stage 共享语义、或 Counter 命名规则变更时，才需要 R1 PR。
- 这些都属于 [architecture.md §4 末尾](architecture.md) 已经声明的"破坏性变更"，按那里的流程执行。

## Day 1 启动任务

R1 PR 合入即视为 Day 1 开始。下表给出每人**不依赖他人产出**的启动任务：

| 角色 | Day 1 启动任务 |
|---|---|
| **R1** | 持续答疑；同步起草 [docs/report/](report/) 目录骨架（章节文件 stub）；建 [tests/data/](../tests/data/) 下各 fixture 文件的占位 |
| **R2** | stage0 mapper（CSV → `RecordWritable`），用 [tests/data/mini.csv](../tests/data/mini.csv) 做单测；profiler 跑 mini.csv 出长尾直方图 |
| **R3** | 在 `stage1/src/test/resources/` 手写极小 `filtered.seq` fixture；写 stage1 mapper / reducer 单测；skew partitioner 纯单测 |
| **R4** | 在 `stage2/src/test/resources/` 手写极小 `pair_loc_slot.seq` fixture；stage2 reducer 单测；HLL fallback 路径单测 |
| **R5** | baseline pandas 实现（跑 [tests/data/mini.csv](../tests/data/mini.csv)，**完全不依赖 Hadoop**，Day 1 即可独立 PR）；stage3 的 total-order partitioner + TopN 写盘逻辑，用手写 `companions.csv` fixture 做单测 |
| **R6** | `bench/parse_counters.py`（用 fake `.jhist` 文本做单测）；`monitor_cluster.sh` 雏形；[docs/report/04-performance.md](report/04-performance.md) M1/M2/M3 表头模板 |

跨模块对账靠 fixture：每个 stage owner 同时在自己模块的 `src/test/resources/` 手写本 stage 的输入 fixture，单测先绿；上游 stage 完工后，`mvn verify` 会把上游真实输出 diff 到下游 fixture，字节级一致才算 pass。这一机制让 R2~R5 之间不再串行等待。

## 整合报告

报告位于 [docs/report/](report/)，章节归属：

| 章节 | 内容 | 主笔 |
|---|---|---|
| §1 Overview & Architecture | 项目目标、流水线、模块边界、HDFS 路径、契约 | R1 |
| §2 Per-stage Implementation Notes | 每 stage 实现要点、关键 trade-off | R2 / R3 / R4 / R5（各半页） |
| §3 Correctness | golden set、baseline diff、recall / precision 结果 | R5 |
| §4 Performance | M1 / M2 / M3 wall-clock、shuffle、reducer skew、参数扫表结论 | R6 |
| §5 Operations / Reproducibility | 集群拓扑、提交命令、复现步骤 | R6 + R1 |
| §6 Lessons & Trade-offs | 每人 1 段心得 | R6 牵头汇总 |

**commit-as-you-go**：实现某个 stage 时同步写自己的章节笔记，不要留到 M3 集中赶。R6 每周做一次 `cat 0?-*.md > FINAL.md` 性质的 stitch。

## CODEOWNERS

GitHub 账号到位后追加 [.github/CODEOWNERS](../.github/CODEOWNERS)，按本表 1:1 对应。本文先用代号占位。

模板：

```
/common/                 @r1
/docs/                   @r1
/scripts/                @r1
/stage0/  /tools/        @r2
/stage1/                 @r3
/stage2/                 @r4
/stage3/  /baseline/     @r5
/bench/                  @r6
/docs/report/            @r6
/.github/                @r1
```
