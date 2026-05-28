# Stage 0 - 预处理与单次车过滤

**Owner**: R2（详见 [docs/roles.md](../docs/roles.md)）

Stage 0 是流水线的入口。它把原始 CSV 变成下游可直接读取的 `SequenceFile`，并尽早过滤只出现 1 次的车辆，减少 Stage 1 的配对规模。

## 本阶段要解决什么

原始数据是一行一条文本记录：

```text
vid,loc,ts
```

Stage 0 需要完成三件事：

1. 解析 CSV，丢弃格式错误或越界记录。
2. 统计每个 `vid` 的出现次数，只保留出现次数 `>= 2` 的车辆。
3. 把保留下来的记录写成 `RecordWritable(vid, loc, ts_norm)`。

`ts_norm = ts - companion.t0`。也就是说，下游 stage 读取到的 `ts` 已经是归一化后的相对秒数，不应再减一次 `companion.t0`。

## 输入输出

| 类型 | 路径 | 格式 |
|---|---|---|
| 输入 | `${COMPANION_ROOT}/input/raw/{phase}.csv` | 文本行 `vid,loc,ts` |
| 中间结果 | `/companion/runs/<run_id>/vid_freq/{phase}` | `SequenceFile<NullWritable, BloomFilter>` |
| 输出 | `/companion/runs/<run_id>/filtered/{phase}/part-*` | `SequenceFile<NullWritable, RecordWritable>` |

`{phase}` 取值为 `1d`、`7d`、`31d`。

## 推荐实现流程

```text
Stage0aFreqJob
  输入原始 CSV
  -> 解析 vid
  -> combiner 本地累加
  -> reducer 统计全局 vid 频次
  -> 输出包含出现次数 >= 2 的 vid 的 BloomFilter

Stage0bFilterJob
  输入原始 CSV
  -> DistributedCache 加载 Stage0a 的 BloomFilter
  -> 解析完整记录
  -> 命中 BloomFilter 则写出 RecordWritable
```

Bloom Filter 必须保证业务上不产生假阴性：出现次数 `>= 2` 的 vid 不能被过滤掉。少量 false positive 只会让个别 singleton 继续进入后续 stage，不应影响 Stage2 阈值过滤后的最终结果。默认按 2500 万 vid、`1e-9` false-positive rate 配置；测试或小规模调试可通过 `-D companion.stage0.bloom.expected.entries=...` 和 `-D companion.stage0.bloom.false.positive.rate=...` 覆盖。

## 必须保持的契约

- 输出值固定为 `RecordWritable`，字段顺序为 `(vid, loc, ts_norm)`。
- `RecordWritable` 字节布局固定为 12 字节，也就是 3 个 `int`。
- 输出使用 `SequenceFile`，并启用 block 压缩；codec 使用集群 Hadoop 默认值。
- 脏数据只计入 `PARSE_FAIL`，不能让 mapper 因单行坏数据失败。
- 输出路径只包含 Hadoop 正常生成的 part 文件和元数据文件，不额外混入临时业务文件。

## Counter

Counter 组名固定为 `STAGE0`。

| Counter | 含义 |
|---|---|
| `RAW_RECORDS` | Stage0a 每读入一行 CSV 加 1 |
| `PARSE_FAIL` | 字段缺失、非数字、时间戳越界等解析失败加 1 |
| `SINGLETON_VIDS` | Stage0a reducer 发现出现次数为 1 的 vid 加 1 |
| `KEPT_RECORDS` | Stage0b 每写出一条记录加 1 |

这些名称会被 bench 模块解析，不能改名。

## 性能要求

- Stage0a 必须使用 combiner，避免把全部 `(vid, 1)` 直接送到 reducer。
- 不要用 `String.split(",")` 解析大规模 CSV；优先使用 `indexOf` 和无额外对象分配的整数解析方式。
- Mapper 中复用 `RecordWritable` 实例。
- 不要用全量 `HashSet<Integer>` 保存 2300 万车辆；Stage0b 应从 DistributedCache 加载 BloomFilter。
- `PARSE_FAIL / RAW_RECORDS > 0.1%` 时应告警。

## 验收信号

- `KEPT_RECORDS < RAW_RECORDS`，说明单次车过滤生效。
- `PARSE_FAIL` 很小，且坏数据不会中断任务。
- Stage 1 能直接读取 `/companion/filtered/{phase}` 的 `SequenceFile`。
