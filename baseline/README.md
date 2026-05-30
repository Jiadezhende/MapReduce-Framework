# baseline - 对照基线与正确性校验

**Owner**: R5（同时负责 [stage3/](../stage3/)；详见 [docs/roles.md](../docs/roles.md)）

baseline 模块不参与生产流水线计算。它的作用是提供独立实现，用来判断 MapReduce 结果是否正确。

## 本模块要解决什么

MapReduce 流水线输出以后，需要回答三个问题：

1. MR 有没有漏掉应该出现的 pair？
2. MR 有没有多算不该出现的 pair？
3. MR 对每个 pair 的 `count` 是否正确？

baseline 模块通过 Pandas / Spark 实现同一套算法语义，再用 diff 工具和 MR 输出对比。

## 交付物

| 文件 | 作用 | 数据规模 |
|---|---|---|
| `single_machine.py` | Pandas 正确性基线 | 1d |
| `spark_companion.py` | PySpark 等价实现 | 7d |
| `diff_baseline.py` | 比较 MR 输出和 baseline 输出 | 任意 phase |
| `golden_inject.py` | 注入已知 fake pair 做端到端测试 | 小数据 / 1d |

对比流程：

```text
/companion/final/{phase}/companions.csv
  + baseline/{phase}_baseline.csv
  -> diff_baseline.py
  -> baseline/reports/{phase}_diff.json
```

## 必须对齐的算法语义

- 时间窗口是闭区间：`|tsA - tsB| <= companion.delta.t`。
- 两辆车必须出现在同一个 `loc`。
- pair 固定保存为 `vidA < vidB`。
- `count = distinct (loc, slot)` 数，不是原始共现记录数。
- Stage 0 会过滤出现次数 `<= 1` 的车辆，baseline diff 前也要对齐这个行为。
- 参数从 `common/src/main/resources/companion-conf.xml` 读取，不要硬编码 `delta.t`、`k.min`、`slot.size`。

## 输出格式

baseline 的主输出必须和 MR 保持一致：

```text
vidA,vidB,count
```

按 `count` 降序排列。MR Stage 2、Stage 3 主输出与 baseline 全部统一使用逗号分隔（详见 [docs/fixtures.md §3](../docs/fixtures.md#3-companionscsv--stage-2-主输出--stage-3-输入--stage-3-重整后)），diff 工具直接按逗号读取，无需做分隔符转换。

## diff 报告

报告路径：

```text
baseline/reports/{phase}_diff.json
```

schema：

```json
{
  "phase": "1d",
  "mr_pairs": 12345,
  "baseline_pairs": 12350,
  "precision": 0.9996,
  "recall": 0.9999,
  "count_mae": 0.02,
  "missing_in_mr": [],
  "extra_in_mr": [],
  "count_mismatches": []
}
```

`missing_in_mr`、`extra_in_mr`、`count_mismatches` 只保留最重要的前 100 条，避免报告过大。

HLL pair 可以允许 count 有相对误差，默认容忍 `+/- 2%`；非 HLL pair 应精确一致。

## 验收信号

| 里程碑 | 数据 | 要求 |
|---|---|---|
| M1 | 1d | recall >= 99%，理想情况下接近 100% |
| M2 | 7d | 使用 Spark baseline 辅助验证，具体 recall 由 bench 报告给出 |
| M3 | 31d | 不要求 Pandas / Spark 全量复算，重点看抽样、HLL 和 golden set |

## 性能要求

- Pandas baseline 只服务 1d，不扩展到 7d 或 31d。
- Pandas 实现优先保证正确性，性能可以接受较慢。
- Spark 实现使用 DataFrame API，不使用 RDD。
- diff 比较时按 `(vidA, vidB)` 排序后归并扫描，不要把两边全量 join 到内存。
- `golden_inject.py` 注入 fake vid 时使用大数段，例如 `>= 90000000`，避免和真实 vid 冲突。

## 本地跑法

baseline 是本地独立验证用的脚本，跟集群侧 Hadoop 没关系，环境单独搭。

### 一次性环境

```bash
python3 -m venv .venv
.venv/bin/pip install pandas 'pyspark==3.5.3'
```

PySpark 版本必须钉在 3.5.x，原因：

- 4.x 要求 JDK 17+；本机常装的是 Corretto 8（Hadoop 编译需要）和较新的 JDK 26，缺中间版本时 4.x 启动会报 `JAVA_GATEWAY_EXITED`。
- 3.5.x 兼容 JDK 8/11/17，能直接复用已有的 Corretto 8，不必再装一套 JDK。

Spark baseline 跑之前需要把 `JAVA_HOME` 显式指到 JDK 8（默认 `java` 通常是 JDK 26，Spark 3.5 在 26 上同样会失败）：

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
export PATH=$JAVA_HOME/bin:$PATH
```

### 跑 Pandas baseline

```bash
.venv/bin/python baseline/single_machine.py \
  --input  tests/data/mini.csv \
  --output baseline/out/pandas.csv
```

### 跑 Spark baseline

```bash
.venv/bin/python baseline/spark_companion.py \
  --input  tests/data/mini.csv \
  --output baseline/out/spark_dir \
  --shuffle-partitions 4
```

`--output` 是目录，结果在 `baseline/out/spark_dir/part-*.csv`。

### 交叉验证

两份 baseline 在同一输入上应当逐字节一致（HLL 行为只在 MR 侧出现，baseline 不带 HLL）：

```bash
cp baseline/out/spark_dir/part-*.csv baseline/out/spark.csv
diff baseline/out/pandas.csv baseline/out/spark.csv
```

不一致就说明两边语义跑偏了，先修 baseline 再去 diff MR 输出，否则 MR 的对错判断都不可信。
