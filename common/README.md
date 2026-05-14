# common - 共享语义与接口契约

**Owner**: R1（同时维护 [docs/architecture.md](../docs/architecture.md) 和 [scripts/](../scripts/) 骨架；详见 [docs/roles.md](../docs/roles.md)）

`common` 是整条 MapReduce 流水线的公共接口层。它不实现具体业务 stage，但定义了各 stage 之间传递数据、读取配置、提交 Job 和计算分区/时间桶时必须遵守的语义。

如果修改 `common` 中的 Writable 字节布局、配置 key、Counter 组名或工具函数语义，需要同步检查 `stage0` 到 `stage3`、`baseline`、`bench` 和 [docs/architecture.md](/home/ywc/MapReduce-Framework/docs/architecture.md)。

## 模块边界

`common` 可以被所有 stage、工具和测试依赖；`common` 自己不能依赖任何 stage。

```text
common
  -> stage0
  -> stage1
  -> stage2
  -> stage3
  -> baseline / bench / tools
```

这里的方向表示“被依赖”。如果 `common` 反过来依赖 stage，就会破坏流水线的接口边界。

## Writable 是什么

Hadoop MapReduce 在 mapper、shuffle、reducer 之间传输 key/value 时，需要知道如何把对象写成字节、再从字节读回来。`Writable` 就是 Hadoop 的二进制序列化接口。

普通 value 实现 `Writable`；需要作为 key 排序或分组的类型实现 `WritableComparable`。

本项目的 Writable 不只是 Java 类，还是 stage 之间的数据协议。字段顺序、编码方式和排序规则都属于契约。

## 当前 Writable 设计

| 类型 | 语义 | 编码 | 使用位置 |
|---|---|---|---|
| `RecordWritable` | 一条过滤后的车辆出现记录 `(vid, loc, tNorm)` | 固定 12 字节，3 个 int | Stage0 输出，Stage1 输入 |
| `CompositeKey` | Stage1 二级排序 key `(loc, slot, ts)` | 固定 12 字节，3 个 int | Stage1 mapper 输出 key |
| `PairKey` | 车辆对 `(vidA, vidB)`，强制 `vidA < vidB` | 固定 8 字节，2 个 int | Stage1 输出，Stage2 输入 |
| `LocSlotWritable` | 一次 pair 共现见证 `(loc, slot)` | 两个 VInt，变长编码 | Stage1 输出 value，Stage2 输入 value |

### `RecordWritable`

路径：[RecordWritable.java](/home/ywc/MapReduce-Framework/common/src/main/java/companion/io/RecordWritable.java:1)

语义：

```text
(vid, loc, tNorm)
```

其中 `tNorm = raw_ts - companion.t0`。Stage0 写出时完成时间归一化；Stage1 之后的代码都应把该字段当作相对秒数使用。

不能改：

- 字段顺序。
- `int, int, int` 的固定宽度编码。
- `tNorm` 的语义。

### `CompositeKey`

路径：[CompositeKey.java](/home/ywc/MapReduce-Framework/common/src/main/java/companion/io/CompositeKey.java:1)

语义：

```text
(loc, slot, ts)
```

用于 Stage1 的 secondary sort：

- 全排序按 `(loc asc, slot asc, ts asc)`。
- 分组只按 `(loc, slot)`。

这样 reducer 能在同一个 `(loc, slot)` 内按时间顺序滑窗配对。

不能改：

- 排序字段顺序。
- `LocSlotGroupComparator` 只按 `(loc, slot)` 分组的语义。
- 固定 12 字节编码。

### `PairKey`

路径：[PairKey.java](/home/ywc/MapReduce-Framework/common/src/main/java/companion/io/PairKey.java:1)

语义：

```text
(vidA, vidB), vidA < vidB
```

`PairKey.set(v1, v2)` 会自动把小的 vid 放前面。这样 `(100, 200)` 和 `(200, 100)` 会成为同一个 key，Stage2 才能正确聚合。

不能改：

- `vidA < vidB` 的规范化规则。
- 相同车辆不能构成 pair。
- 固定 8 字节编码。
- 按 `(vidA, vidB)` 升序排序的比较规则。

### `LocSlotWritable`

路径：[LocSlotWritable.java](/home/ywc/MapReduce-Framework/common/src/main/java/companion/io/LocSlotWritable.java:1)

语义：

```text
(loc, slot)
```

它表示一个 pair 在某个地点和时间桶中有过共同出现。Stage2 的 `count` 就是同一 pair 的 distinct `(loc, slot)` 数。

它使用 `WritableUtils.writeVInt` 变长编码，因为 loc 和 slot 通常较小，可以减少 Stage1 到 Stage2 的 shuffle 体积。

不能改：

- `(loc, slot)` 作为 witness 的语义。
- Stage2 以 distinct `(loc, slot)` 计数的口径。

## 配置语义

路径：

- [companion-conf.xml](/home/ywc/MapReduce-Framework/common/src/main/resources/companion-conf.xml:1)
- [CompanionConf.java](/home/ywc/MapReduce-Framework/common/src/main/java/companion/conf/CompanionConf.java:1)

所有 stage 必须通过 `CompanionConf` 的 typed getter 读取配置，不应在 stage 代码里手写字符串 key。

| 配置 | 语义 |
|---|---|
| `companion.t0` | 原始 Unix 时间戳归一化的基准时间 |
| `companion.delta.t` | 伴随车时间窗口，默认 300 秒 |
| `companion.k.min` | pair 至少需要多少个 distinct witness 才输出 |
| `companion.slot.size` | 时间桶宽度，默认 300 秒 |
| `companion.loc.skew.cap` | Stage1 单个热点桶的保护上限 |
| `companion.pair.salt.n` | Stage2 pair salt 桶数 |
| `companion.top.n` | Stage3 TopN 输出行数 |
| `companion.stage1.reducers` | Stage1 默认 reducer 数 |
| `companion.stage2.reducers` | Stage2 默认 reducer 数 |
| `companion.salt.seed` | salt / 二次切片使用的稳定 seed |

新增配置时需要同时更新：

1. `companion-conf.xml`
2. `CompanionConf.java`
3. `docs/architecture.md`
4. 使用该配置的 stage README

## Job 入口约定

路径：[AbstractCompanionJob.java](/home/ywc/MapReduce-Framework/common/src/main/java/companion/job/AbstractCompanionJob.java:1)

所有 MapReduce Job 应继承 `AbstractCompanionJob`。它统一处理：

- 加载 `companion-conf.xml`。
- 解析前两个位置参数：`<input> <output>`。
- 透传 `-Dkey=value` 配置覆盖。
- 等待 Job 完成并返回统一退出码。
- 定义 Counter 组名常量：`STAGE0`、`STAGE1`、`STAGE2`、`STAGE3`。

标准提交形式：

```bash
hadoop jar <stage-jar> <job-class> <input-path> <output-path> -Dcompanion.delta.t=600
```

## 工具函数语义

### `TimeUtil`

路径：[TimeUtil.java](/home/ywc/MapReduce-Framework/common/src/main/java/companion/util/TimeUtil.java:1)

提供时间轴转换：

```text
raw unix ts -> tNorm -> slot
```

- `normalize(ts, t0)`：计算 `ts - t0`。
- `slot(tNorm, slotSize)`：计算时间桶。
- `slotStart(slot, slotSize)`：计算桶起始相对秒数。

### `HashUtil`

路径：[HashUtil.java](/home/ywc/MapReduce-Framework/common/src/main/java/companion/util/HashUtil.java:1)

提供稳定 hash / salt 计算。不要依赖 Java 对象默认 `hashCode()` 来做跨 Job 的分区或 salt，因为它不适合作为稳定的数据协议。

## 修改检查清单

修改 `common` 前先判断改动类型：

- 只新增 helper，且不改变已有语义：通常只需要补测试。
- 新增配置：同步配置文件、typed getter、架构文档和对应 stage README。
- 修改 Writable 字段、编码或排序：这是跨模块破坏性变更，需要同步所有消费者。
- 修改 Counter 组名：会影响 bench 的 `parse_counters.py`，需要同步更新。
- 修改时间或 hash 语义：会影响正确性和性能评测，需要用 baseline 重新验证。
