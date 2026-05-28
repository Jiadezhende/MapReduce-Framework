# Companion Vehicle Mining (MapReduce)

本项目用 Hadoop MapReduce 从 31 天卡口数据中挖掘“伴随车”：两辆车如果多次在同一地点、短时间窗口内共同出现，就把它们作为候选伴随车对输出，并按共现次数排序。

原始数据格式见 [伴随车数据说明.md](伴随车数据说明.md)，数据集获取方式见 [docs/data.md](docs/data.md)。开发说明见 [CONTRIBUTING.md](CONTRIBUTING.md)。

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

## 流水线

```text
CSV
  -> Stage 0: 解析、时间归一化、过滤只出现 1 次的车辆
  -> Stage 1: 在同一地点的滑动时间窗口内生成车辆对见证
  -> Stage 2: 按车辆对去重计数，并过滤 count < k.min 的结果
  -> Stage 3: 全局排序、导出 TopN 和指标
```

对应 HDFS 路径：

```text
/companion/input/raw/{1d,7d,31d}.csv              # 共享只读，由数据维护者上传
  -> /companion/runs/<run_id>/vid_freq/{phase}
  -> /companion/runs/<run_id>/filtered/{phase}
  -> /companion/runs/<run_id>/pair_loc_slot/{phase}
  -> /companion/runs/<run_id>/companions/{phase}
  -> /companion/runs/<run_id>/final/{phase}
```

`{phase}` 表示数据规模，取值为 `1d`、`7d`、`31d`。开发时先跑 1 天数据验证正确性，再扩到 7 天和 31 天。每次提交生成独立 `run_id`，所有中间产物落到 `runs/<run_id>/`，靠 run_id 隔离，多人并行互不覆盖。

## 目录结构

| 目录 | 说明 |
|---|---|
| `common/` | 共享 Writable、配置封装、Job 基类和工具函数 |
| `stage0/` | CSV 预处理和单次车过滤 |
| `stage1/` | 同地点滑动窗口配对 |
| `stage2/` | pair 去重计数和阈值过滤 |
| `stage3/` | 全局排序、TopN 和指标导出 |
| `baseline/` | Pandas / Spark 对照实现和正确性 diff |
| `bench/` | 性能评测、参数扫表和报告 |
| `tools/` | 辅助工具，例如长尾分布统计 |
| `scripts/` | 上传数据和提交流水线的脚本 |

## 构建

```bash
mvn -B clean verify
```

该命令会构建所有模块，并运行 `common/` 的单元测试和 mini-cluster smoke test。各 stage 的 jar 会输出到 `<stage>/target/`。

## 运行

所有集群操作都从本地仓库通过非交互 `ssh master ...` 触发，不需要登录 master，本地也不需要装 hadoop。

```bash
# 1. 上传原始 CSV 到 HDFS（维护者一次性操作；组员见 docs/data.md）
scripts/upload_to_hdfs.sh 1d.csv 7d.csv 31d.csv

# 2. 本地构建并提交 1d 端到端
scripts/cluster_run.sh --days 1 --build --dry-run    # 先看命令
scripts/cluster_run.sh --days 1 --build              # 实际提交

# 3. 不登录 master 也能看结果
scripts/cluster_status.sh <run_id>
scripts/cluster_head.sh   <run_id> final 1d
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
