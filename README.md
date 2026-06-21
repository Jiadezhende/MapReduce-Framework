# Companion Vehicle Mining (MapReduce)

本项目用 Hadoop MapReduce 从 31 天卡口数据中挖掘“伴随车”：两辆车如果多次在同一地点、短时间窗口内共同出现，就把它们作为候选伴随车对输出，并按共现次数排序。

原始数据格式见 [伴随车数据说明.md](伴随车数据说明.md)，数据集获取方式见 [docs/data.md](docs/data.md)。开发与协作约定见 [CONTRIBUTING.md](CONTRIBUTING.md)，跨模块接口契约见 [docs/architecture.md](docs/architecture.md)。

## 目标功能

输入约 2.7 亿条 `(vid, loc, ts)` 记录，找出满足下面条件的车辆对：

- `vidA != vidB`，并统一保存为 `vidA < vidB`。
- 两辆车在同一个 `loc` 出现。
- 两次出现时间差满足 `|tsA - tsB| <= companion.delta.t`，默认 300 秒。
- 该车辆对至少在 `companion.k.min` 个不同 `(loc, slot)` 桶中出现，默认 3 次。

这里的 `count` 不是原始记录条数，也不是所有时间点的配对次数，而是“不同 `(loc, slot)` 见证桶”的数量。

最终输出包括：

- 完整伴随车结果，按 `count` 降序排列。
- TopN 伴随车结果，默认前 10000 行。
- 运行指标与 Counter 汇总。

## 流水线与数据流

四个 stage 串行，每个 stage 是一个（或两个）独立的 MapReduce 作业，stage 之间通过 HDFS 上的固定格式文件传递数据：

```text
CSV
  -> Stage 0: 解析、时间归一化、过滤只出现 1 次的车辆
  -> Stage 1: 在同一地点的滑动时间窗口内生成车辆对见证
  -> Stage 2: 按车辆对去重计数，并过滤 count < k.min 的结果
  -> Stage 3: 全局排序、导出 TopN 和指标
```

stage 间传递的数据类型（字段顺序、编码与排序规则以 [common/README.md](common/README.md) 为准）：

| 边界 | 类型 | 大小 | 说明 |
|---|---|---|---|
| 原始输入 | CSV 文本 | — | `vid,loc,ts` 每行一条 |
| Stage0 → Stage1 | `RecordWritable` | 12B (3×int) | `(vid, loc, tNorm)`，定宽 SequenceFile 值 |
| Stage1 内部 key | `CompositeKey` | 12B (3×int) | 二级排序 key，大端；分组按 `(loc, slot)` |
| Stage1 → Stage2 | `PairKey` + `LocSlotWritable` | 8B + 1–10B | pair 见证，`PairKey` 恒 `vidA<vidB`；`LocSlotWritable` 变长 VInt |
| Stage2 → Stage3 | CSV 文本 | — | `vidA,vidB,count`（已过阈值） |
| Stage3 最终输出 | CSV + JSON | — | 全局排序 CSV、`top_n.csv`、`_metrics.json` |

对应 HDFS 路径：

```text
/companion/input/raw/{1d,7d,31d}.csv              # 共享只读，由数据维护者上传
  -> /companion/runs/<run_id>/vid_freq/{phase}     # Stage0a BloomFilter
  -> /companion/runs/<run_id>/filtered/{phase}     # Stage0b
  -> /companion/runs/<run_id>/pair_loc_slot/{phase} # Stage1
  -> /companion/runs/<run_id>/companions/{phase}   # Stage2
  -> /companion/runs/<run_id>/final/{phase}        # Stage3
```

`{phase}` 表示数据规模，取值为 `1d`、`7d`、`31d`。开发时先跑 1 天数据验证正确性，再扩到 7 天和 31 天。每次提交生成独立 `run_id`，所有中间产物落到 `runs/<run_id>/`，靠 run_id 隔离，多人并行互不覆盖。完整 HDFS 布局见 [docs/architecture.md](docs/architecture.md)。

## 代码结构与关键类

| 目录 | 说明 | 关键类 / 入口 |
|---|---|---|
| `common/` | 共享 Writable、配置封装、Job 基类和工具函数 | `RecordWritable` `PairKey` `LocSlotWritable` `CompositeKey`、`CompanionConf`、`AbstractCompanionJob`、`HashUtil` `TimeUtil` |
| `stage0/` | CSV 预处理和单次车过滤 | `Stage0aFreqJob` `Stage0bFilterJob` `Stage0CsvParser` `Stage0Bloom` |
| `stage1/` | 同地点滑动窗口配对 | `Stage1Job` |
| `stage2/` | pair 去重计数和阈值过滤 | `Stage2Job` |
| `stage3/` | 全局排序、TopN 和指标导出 | `Stage3SortJob` `Stage3Key` |
| `baseline/` | Pandas / Spark 对照实现和正确性 diff | `single_machine.py` `spark_companion.py` `diff_baseline.py` |
| `bench/` | 性能评测、参数扫表和报告 | `run_matrix.sh` `parse_counters.py` |
| `tools/` | 辅助工具，例如长尾分布统计 | `profiler/` |
| `scripts/` | 上传数据和提交流水线的脚本 | `cluster_run.sh`（统一入口） |
| `deploy/` | 单机 Hadoop 部署模板与一键 bring-up | `single-node/bootstrap.sh` |
| `tests/` | golden 夹具，跨 stage 字节级对账 | `data/fixtures/` |

模块依赖是单向的：`common → stage0 → stage1 → stage2 → stage3`，stage 之间无反向依赖，stage 只 import `common`。`common` 仅依赖 Hadoop 与 slf4j。

## 各 Stage 算法概要

- **Stage 0 — 预处理与频次过滤**（两轮 MR）。J0a 读 CSV 统计每个 `vid` 的出现频次，把频次 ≥2 的 vid 写成一个稀疏 `BloomFilter`；J0b 经 DistributedCache 加载该 BloomFilter，在 map 端过滤掉只出现一次的车辆，并做时间归一化（`tNorm = ts - t0`），输出定宽 `RecordWritable` 的 SequenceFile。
- **Stage 1 — 滑窗配对**（两轮 MR）。按 `loc` 分组、在 `|tsA - tsB| <= delta.t` 的滑动窗口内生成车对见证。J1a 按 `(loc, slot/2)` 分组配对，J1b 偏移一格弥补奇/偶 slot 的边界缝隙（见 [docs/stage1-boundary-gap.md](docs/stage1-boundary-gap.md)）。对热点 `(loc, slot)` 用 salt 切片降低 reducer 倾斜，用 fastutil 集合控制内存。
- **Stage 2 — 共现计数与阈值过滤**。按 `PairKey` 聚合，统计 distinct `(loc, slot)` 见证数；见证数超过 `hll.threshold` 时切换到 HyperLogLog 概率计数控内存；过滤掉 `count < k.min` 的 pair，输出 `vidA,vidB,count` CSV。支持按 hash 分多轮（`stage2.rounds`）跑，压低单轮 shuffle 峰值。
- **Stage 3 — 全局排序与导出**。用 `TotalOrderPartitioner` 按 `count` 降序做全局有序排序，导出完整 `companions.csv`、`top_n.csv`（前 `top.n` 行）和 `_metrics.json` 运行指标。

每个 stage 的输入/输出契约与验收信号见各自的 `stage*/README.md`。

## 构建

> **前置：必须用 JDK 8。** 项目锁定 Java 1.8（Hadoop 3.3.6），高版本 JDK 会破坏构建。若本机 `mvn` 默认指向新版 JDK，先切换：
>
> ```bash
> export JAVA_HOME=/path/to/corretto-1.8   # 或任意 JDK 8
> ```

```bash
mvn -B clean verify
```

该命令会构建所有模块，并运行 `common/` 的单元测试和 mini-cluster smoke test。各 stage 打成 **shaded fat jar**（`maven-shade-plugin` 把 `companion:common` 打进去，`hadoop-client` 保持 `provided` 由集群提供），输出到 `<stage>/target/`，提交时无需 `-libjars`。

## 运行

### A. 集群 / 单机服务器（本地驱动，不登录提交机）

所有集群操作都从本地仓库通过非交互 `ssh master ...` 触发，不需要登录 master，本地也不需要装 hadoop。单节点部署时把 `master` 别名指向那台服务器即可，提交流程完全一致。

```bash
# 1. 上传原始 CSV 到 HDFS（维护者一次性操作；组员见 docs/data.md）
scp 1d.csv master:/tmp/ && ssh master "hadoop fs -mkdir -p /companion/input/raw && hadoop fs -put -f /tmp/1d.csv /companion/input/raw/1d.csv"

# 2. 本地构建并提交 1d 端到端
scripts/cluster_run.sh --days 1 --build --dry-run    # 先看命令
scripts/cluster_run.sh --days 1 --build              # 实际提交

# 3. 不登录 master 也能看结果
scripts/cluster_status.sh <run_id>
scripts/cluster_fetch.sh  <run_id> 1d
```

配置可以用 `-D` 覆盖，例如：

```bash
scripts/cluster_run.sh --days 1 -Dcompanion.delta.t=600 -Dcompanion.k.min=5
```

只想跑某个 stage、或者在已有 `run_id` 上从中间往后续：

```bash
scripts/cluster_run.sh --days 1 --stage stage1 --build
scripts/cluster_run.sh --days 1 --run-id <run_id> --from stage2 --until stage3
```

具体协作约定见 [CONTRIBUTING.md](CONTRIBUTING.md) 的「集群协作开发流程」一节。

### B. 单机 Hadoop 一键部署

把原来 3 节点集群的管线迁到一台高性能服务器上，由 [deploy/single-node/bootstrap.sh](deploy/single-node/bootstrap.sh) 在**服务器上**一键起 Hadoop（渲染配置模板、首次格式化 NameNode、启动全部 daemon、建好 `/companion` HDFS 布局，幂等可重跑）：

```bash
# 推荐双盘：HDFS 与 Stage2 shuffle 分盘，避免 shuffle 突发威胁 HDFS
HADOOP_HOME=/opt/module/hadoop-3.3.6 \
JAVA8_HOME=/opt/module/jdk1.8.0_xxx \
HDFS_VOL=/data/hdfs SHUFFLE_VOL=/data/shuffle \
SERVER_HOST=$(hostname) \
bash deploy/single-node/bootstrap.sh
```

单盘时把上面两个卷换成 `DATA_VOL=/data/hadoop`。完成后即可像上面 A 节一样从本地用 `scripts/cluster_run.sh` 提交作业。单机磁盘是 31d 规模的成败关键（shuffle 峰值、`STAGE2_ROUNDS` 调参），完整说明见 [docs/single-node-deploy.md](docs/single-node-deploy.md)。

## 配置

所有配置键集中在 `common/src/main/resources/companion-conf.xml`，stage 代码只通过 `CompanionConf` 的 typed getter 读取，不硬编码 key。常用键：

| 键 | 默认值 | 含义 |
|---|---|---|
| `companion.delta.t` | 300 | 共现时间窗口（秒） |
| `companion.k.min` | 3 | 入选阈值：distinct `(loc, slot)` 桶数下限 |
| `companion.slot.size` | 300 | 时间槽宽度（秒） |
| `companion.top.n` | 10000 | TopN 输出行数 |
| `companion.pair.salt.n` | 16 | 热点 pair 的 salt 分片数 |
| `companion.hll.threshold` | 1000000 | 切换 HLL 基数估计的见证数阈值 |
| `companion.stage1.reducers` / `stage2.reducers` | 8/32/128 (1d/7d/31d) | 各 stage reducer 数 |

提交时用 `-D companion.delta.t=600 …` 覆盖。完整键表（含 owner 与取值依据）见 [docs/architecture.md](docs/architecture.md) §2。

## 测试与正确性

- **单元测试 / golden 夹具**：`mvn test` 运行各模块单测；`tests/data/fixtures/` 存有 Stage0/1/2 的字节级 golden 输出，用于跨 stage 对账，重生方式见 [scripts/regenerate_fixtures.sh](scripts/regenerate_fixtures.sh) 与 [docs/fixtures.md](docs/fixtures.md)。
- **正确性基线**：`baseline/` 提供 Pandas（小数据）与 PySpark（7d）两套独立实现，`diff_baseline.py` 把 MR 输出与基线做 diff 生成 JSON 报告，语义对齐规范见 [baseline/README.md](baseline/README.md) 与 [docs/reference-semantics.md](docs/reference-semantics.md)。

## 输出

默认输出位于：

```text
/companion/runs/<run_id>/final/{phase}/
```

主要文件：

| 文件 | 说明 |
|---|---|
| `companions.csv/part-*` | 完整结果，按 `count` 降序全局有序 |
| `top_n.csv` | TopN 单文件 |
| `_metrics.json` | 运行指标、Counter 和分布统计 |

## 深入阅读

| 主题 | 文档 |
|---|---|
| 开发与协作流程、里程碑 | [CONTRIBUTING.md](CONTRIBUTING.md) |
| 跨模块契约（HDFS / 配置 / Counter / Writable） | [docs/architecture.md](docs/architecture.md) |
| 各模块细节 | [common](common/README.md) · [stage0](stage0/README.md) · [stage1](stage1/README.md) · [stage2](stage2/README.md) · [stage3](stage3/README.md) |
| 数据格式与获取 | [伴随车数据说明.md](伴随车数据说明.md) · [docs/data.md](docs/data.md) |
| 集群脚本用法 | [scripts/README.md](scripts/README.md) |
| 单机部署 | [docs/single-node-deploy.md](docs/single-node-deploy.md) |
| 压缩与存储优化 | [docs/compression-strategy.md](docs/compression-strategy.md) · [docs/space-optimization.md](docs/space-optimization.md) · [docs/disk-scaling-analysis.md](docs/disk-scaling-analysis.md) |
| Stage1 优化与演进 | [vid-bucket 重写](docs/stage1-vid-bucket-rewrite.md)（已实施） · [bucket 去重](docs/stage1-bucket-dedup.md)（待评审） · [边界缝隙](docs/stage1-boundary-gap.md)（已解决） · [倾斜定位](docs/stage1-optimization.md) |
| Stage2 优化 | [docs/stage2-optimization.md](docs/stage2-optimization.md) |
| 故障排查与运行记录 | [docs/cluster-troubleshooting.md](docs/cluster-troubleshooting.md) · [docs/7d-trouble-shooting.md](docs/7d-trouble-shooting.md) · [docs/runs/](docs/runs/) |
