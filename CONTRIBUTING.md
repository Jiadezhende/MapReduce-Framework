# 开发指南

本文档面向开发者，说明应该按什么顺序理解项目、各模块之间有什么契约、实现时需要遵守哪些约定。项目目标和基本构建运行方式见 [README.md](README.md)。

## 技术栈

本项目覆盖完整的 MapReduce 伴随车挖掘流程，包括数据预处理、滑窗配对、共现计数、排序导出、正确性校验和性能评测。核心技术栈为：

- Hadoop 3.3.6：HDFS（存储）、YARN（资源调度）、MapReduce（计算引擎）
- Spark / PySpark：仅作为 R5 的对照基线，用于 7d 数据上的等价逻辑验证和性能参考
- 集群拓扑：master + worker1 + worker2

开发前建议先熟悉 MapReduce 常用 API、DistributedCache、SequenceFile、Snappy、secondary sort 和 Counter。

## 推荐阅读顺序

1. 读 [README.md](README.md)，确认项目目标、流水线和构建运行方式。
2. 读 [伴随车数据说明.md](伴随车数据说明.md)，确认原始输入字段含义。
3. 读 [docs/architecture.md](docs/architecture.md)，确认 HDFS 路径、配置、Counter 和模块依赖。
4. 读 [docs/roles.md](docs/roles.md)，确认角色分工、R1 前置 PR 的 checklist 和 Day 1 启动任务。
5. 读 [common/README.md](common/README.md)，理解 Writable、配置封装、Job 基类和工具函数的语义。
6. 按顺序读 `stage0/README.md`、`stage1/README.md`、`stage2/README.md`、`stage3/README.md`，确认每个 stage 的输入、输出和验收信号。
7. 读 `baseline/README.md`，理解正确性 diff 和 golden set。
8. 读 `bench/README.md`，理解性能评测、参数扫表和 M1 / M2 / M3 报告。

## 模块契约

这些约定是跨模块接口，修改时必须同步所有消费者。

| 契约 | 要求 |
|---|---|
| HDFS 路径 | `input/raw -> input/{phase} -> filtered -> pair_loc_slot -> companions -> final` |
| 配置 | stage 代码通过 `CompanionConf` typed getter 读取配置，不硬编码 key |
| Writable | 字段顺序、编码和排序规则以 `common/README.md` 为准 |
| Counter | 使用 `STAGE0`、`STAGE1`、`STAGE2`、`STAGE3` 四个组，名称不能随意改 |
| Job 入口 | 每个 Job 继承 `AbstractCompanionJob`，前两个参数固定为 input 和 output |
| 输出 schema | Stage2 主输出为 `vidA\tvidB\tcount`，Stage3 最终输出按 count 降序 |

## 模块职责

使用四阶段流水线完成主计算任务：

| 模块 | 职责 | 关键文档 |
|---|---|---|
| `common/` | 共享 Writable、配置封装、Job 基类 | `common/README.md` |
| `stage0/` | 解析 CSV、统计 vid 频次、过滤单次车 | `stage0/README.md` |
| `stage1/` | 同地点滑窗生成 pair witness | `stage1/README.md` |
| `stage2/` | 统计 distinct `(loc, slot)` witness，过滤低频 pair | `stage2/README.md` |
| `stage3/` | 全局排序、TopN、metrics 导出 | `stage3/README.md` |
| `baseline/` | Pandas / Spark 正确性基线和 diff | `baseline/README.md` |
| `bench/` | 参数扫表、性能采集和报告 | `bench/README.md` |
| `tools/` | 辅助工具和数据画像 | `tools/README.md` |

## 开发顺序

建议按数据规模和模块顺序推进：

1. 先保证 `common` 的单元测试通过。
2. 实现或调整某个 stage 前，先确认它的输入输出契约。
3. 先跑 1d 数据，完成 M1 端到端正确性验证。
4. 再跑 7d 数据，观察 reducer skew 和热点处理效果。
5. 最后跑 31d 数据，重点看 shuffle、倾斜和最终报告。

三个数据规模递进：`1d -> 7d -> 31d`，对应 M1 / M2 / M3。每个阶段都应先验证正确性，再扩大数据规模。

## 里程碑

| 里程碑 | 数据规模 | 验收重点 |
|---|---|---|
| M1 | 1d | 端到端跑通，10 分钟内完成，recall >= 99% |
| M2 | 7d | 30 分钟内完成，热点切分有效，max reducer <= 2 * mean |
| M3 | 31d | shuffle < 20 GB，max reducer <= 3 * mean，完成性能和正确性报告 |

## 修改 common 的注意事项

`common` 是跨模块接口层，修改影响面最大：

- 新增配置时，同步更新 `companion-conf.xml`、`CompanionConf.java`、`docs/architecture.md` 和相关 stage README。
- 修改 Writable 字段、编码、排序或分组规则时，需要同步所有 stage 和 baseline。
- 修改 Counter 名称时，需要同步 bench 的 counter 解析逻辑。
- 修改时间归一化或 hash / salt 语义时，需要重新跑 baseline diff。
