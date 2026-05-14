# bench - 性能评测与运行报告

**Owner**: R6

bench 模块负责评估整条 MapReduce 流水线的性能表现。它不修改 stage 代码，只负责运行参数扫表、采集指标、分析瓶颈并产出报告。

## 本模块要解决什么

流水线跑通后，需要回答：

1. 1d、7d、31d 分别跑了多久？
2. 哪个 stage 是瓶颈？
3. shuffle、输出 pair 数、过滤率是否在可接受范围内？
4. reducer 是否倾斜，max reducer 是否远慢于平均值？
5. 参数变化对 recall 和 wall-clock 的影响是什么？

bench 的输出是 M1 / M2 / M3 验收报告。

## 工作流

```text
run_matrix.sh
  -> 多组参数调用 scripts/run_pipeline.sh
  -> monitor_cluster.sh 采集集群资源
  -> parse_counters.py 解析 YARN history counter
  -> reducer_skew.py 统计 reducer 分布
  -> plot_skew.py 生成图
  -> reports/M{1,2,3}_perf_report.md
```

## 交付物

| 文件 | 用途 |
|---|---|
| `run_matrix.sh` | 扫表入口 |
| `parse_counters.py` | 拉 YARN history 中的 counter，扁平化成 CSV |
| `reducer_skew.py` | 给定 Job，输出每 reducer 的 record 数 / shuffle 字节 / wall time 分布（mean/p50/p95/max） |
| `plot_skew.py` | 渲染 skew 分布 PNG（box + cdf） |
| `monitor_cluster.sh` | Job 运行期间采样 CPU / mem / net / disk，落 CSV |
| `reports/M{1,2,3}_perf_report.md` | 里程碑总结报告 |

## 参数矩阵

| 维度 | 取值 |
|---|---|
| `companion.delta.t` | 180 / 300 / 600 |
| `companion.k.min` | 2 / 3 / 5 |
| reducer 数 | 1d: 4/8/16 ；7d: 16/32/64 ；31d: 64/128/256 |
| phase | 1d / 7d / 31d |

不要跑完整笛卡尔积。使用正交切片：

- 基准点：`delta.t=300`、`k.min=3`、`reducers=mid`。
- 每次只改变一个维度。
- 每个 phase 约 10 次运行覆盖主要趋势。

## 运行产物

每次运行写入独立目录：

```text
bench/results/{phase}_{delta}_{k}_{reducers}_{YYYYMMDD-HHMMSS}/
```

目录内至少包含：

| 文件 | 内容 |
|---|---|
| `run.log` | 本次流水线 stdout / stderr |
| `counters.csv` | 各 stage Counter |
| `wall_clock.csv` | 各 stage wall-clock |
| `skew.csv` | reducer record / shuffle / wall time 分布 |
| `cluster_metrics.csv` | CPU、内存、网络、磁盘采样 |

禁止覆盖历史结果。

## 必须保持的约束

- bench 不直接修改 stage 代码。
- Counter 名称依赖 `docs/architecture.md`，stage 改名时必须同步更新 `parse_counters.py`。
- 每次运行前清理对应 phase 的中间输出：`filtered`、`pair_loc_slot`、`companions`、`final`。
- `monitor_cluster.sh` 默认 5 秒采样一次。
- 性能结论优先看 wall-clock、Counter、shuffle 和 reducer skew，不用单点 CPU 利用率下结论。
- 报告描述现状和 trade-off，不写空泛的未来改进。

## Counter 契约（消费的）

| 组 | 关键 Counter | 派生指标 |
|---|---|---|
| `STAGE0` | `RAW_RECORDS`, `KEPT_RECORDS`, `SINGLETON_VIDS` | 长尾削减率 |
| `STAGE1` | `PAIRS_EMITTED`, `SKEW_DROP`, `HOT_LOCS_SLICED`, `CROSS_SLOT_PAIRS` | pair 放大率、salt 命中率、跨 slot 占比 |
| `STAGE2` | `PAIRS_INPUT`, `PAIRS_OUTPUT`, `HLL_FALLBACK_COUNT` | 阈值过滤率、HLL 占比 |
| `STAGE3` | `TOPN_EMITTED` | 完整性自检 |
| 内置 | `HDFS_BYTES_READ`, `BYTES_WRITTEN` | IO |
| 内置 | `MAP_OUTPUT_BYTES`, `REDUCE_SHUFFLE_BYTES` | shuffle 量 |
| 内置 | `CPU_MILLISECONDS`, `GC_TIME_MILLIS` | CPU、GC |

新增 Counter 字段需先在 `docs/architecture.md` 申报。

## 验收阈值

| 里程碑 | wall-clock | shuffle | max/mean reducer |
|---|---|---|---|
| M1 (1d) | < 10 min | 不设固定阈值 | 不设固定阈值 |
| M2 (7d) | < 30 min | 不设固定阈值 | <= 2 |
| M3 (31d) | 不设固定阈值 | < 20 GB | <= 3 |

超阈值项需要在报告中明显标出。

## 运行要求

- 扫表串行执行。集群只有 master + 2 worker，并行会互相抢资源，导致 wall-clock 失真。
- 1d 和 7d 优先完整扫表；31d 只跑基准点和少量关键对比。
- Counter 数据源使用 `.jhist` 文件，不依赖 RM REST API。
- 集群启动后检查时钟同步，漂移超过 1 秒应告警。
- 扫表前检查 YARN 节点资源，单机内存低于 8 GB 时 Stage 2 reducer 风险较高。
- Stage 1 shuffle 可粗略估算为 `PAIRS_EMITTED * 11 B`，其中 PairKey 约 8 B，LocSlotWritable 平均约 3 B。

## 报告内容

每个里程碑一个 markdown 报告：

1. 配置矩阵和基准点选择。
2. 每组配置、每个 stage 的 wall-clock 表。
3. reducer skew 分布图。
4. Counter 派生指标表。
5. recall 与 wall-clock 的关系，recall 来自 baseline diff。
6. 推荐生产参数和已知 trade-off。
