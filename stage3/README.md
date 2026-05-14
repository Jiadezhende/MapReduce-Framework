# Stage 3 - 全局排序、TopN 与指标导出

**Owner**: R4

Stage 3 是流水线的收尾阶段。它读取 Stage 2 的候选伴随车结果，按 `count` 降序全局排序，输出完整结果、TopN 文件和运行指标。

## 本阶段要解决什么

输入来自 Stage 2：

```text
vidA\tvidB\tcount
```

Stage 3 需要输出三类交付物：

| 交付物 | 说明 |
|---|---|
| `companions.csv/part-*` | 全量结果，按 `count` 降序全局有序 |
| `top_n.csv` | 前 `companion.top.n` 行，默认 10000，必须是单文件 |
| `_metrics.json` | 端到端运行指标、Counter 和分布统计 |

## 输入输出

| 类型 | 路径 | 格式 |
|---|---|---|
| 输入 | `hdfs:///companion/companions/{phase}/` | 文本 `vidA\tvidB\tcount` |
| 输入 | `hdfs:///companion/companions/{phase}/_hll_pairs/` | Stage 2 HLL pair 副输出 |
| 输出 | `hdfs:///companion/final/{phase}/companions.csv/part-*` | 全局按 `count` 降序有序 |
| 输出 | `hdfs:///companion/final/{phase}/top_n.csv` | TopN 单文件 |
| 输出 | `hdfs:///companion/final/{phase}/_metrics.json` | JSON 指标 |

## 推荐实现流程

```text
Stage3SortJob
  使用 InputSampler 抽样 count
  使用 TotalOrderPartitioner 生成全局有序的多 part 输出
  mapper 把 count 转成 -count 作为 key
  reducer identity 输出时把 count 转回正数

TopNJob
  优先读取排序结果的 part-00000
  取前 companion.top.n 行
  如果 part-00000 不足 N 行，再继续读后续 part
  最终强制输出一个文件

MetricsWriter
  读取 YARN history、Counter 和必要的副输出
  写出 _metrics.json
```

Sort Job 不能为了全局排序退化成 1 个 reducer。TopN 可以使用单 reducer，因为它只处理最终前 N 行。

## `_metrics.json` schema

字段名是 bench 和最终报告的契约，不能随意改：

```json
{
  "phase": "7d",
  "pair_total": 1234567,
  "count_histogram": {
    "3": 800000,
    "4": 200000,
    "5-9": 150000,
    "10-99": 80000,
    "100-999": 4000,
    "1000+": 567
  },
  "hot_locs": [{"loc": 42, "pair_contribution": 0.18}],
  "hll_pair_count": 123,
  "counters": {"STAGE0.RAW_RECORDS": 270000000},
  "wall_clock_ms": {"stage0": 123456, "stage1": 234567}
}
```

`count_histogram` 桶固定为：`3`、`4`、`5-9`、`10-99`、`100-999`、`1000+`。

`hot_locs` 的数据来源需要在实现前和 bench 对齐。候选来源包括 Stage 1 副输出或 bench 侧单独统计，但 Stage 3 不能重新扫描 Stage 0 或 Stage 1 的主输出做临时统计。

## 必须保持的契约

- `companions.csv/part-*` 多文件可以接受，但整体必须全局有序。
- `top_n.csv` 必须是单文件。
- count 使用 `long` 语义，不要用 `int` 限制结果范围。
- Stage 3 只消费 Stage 2 输出、HLL 副输出、YARN history 和 Counter。
- `_metrics.json` 字段名要和 architecture / bench 保持一致。

## Counter

Counter 组名固定为 `STAGE3`。

| Counter | 含义 |
|---|---|
| `TOPN_EMITTED` | `top_n.csv` 每写出一行加 1 |

`TOPN_EMITTED` 应等于 `min(pair_total, companion.top.n)`。

## 性能要求

- `InputSampler` 推荐参数：`RandomSampler(freq=0.001, numSamples=10000, maxSplitsSampled=10)`。
- 分区文件写到 `hdfs:///companion/_stage3/_partition.lst`。
- 降序排序推荐使用 `-count` 作为 key，避免自定义反向 comparator。
- 若 Stage 2 输出超过 10 GB，应在 Stage 2 端启用文件级 Snappy 压缩，Stage 3 自动解压读取。
- `MetricsWriter` 用脚本或 Java main 实现，不需要写成 MapReduce Job。

## 验收信号

- `companions.csv` 拼接后按 `count` 降序。
- `top_n.csv` 是单文件，且内容等于全量排序结果前 N 行。
- `_metrics.json` 能被 bench 报告直接消费。
- `TOPN_EMITTED` 与预期 TopN 行数一致。
