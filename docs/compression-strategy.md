# 压缩策略

> 范围：管线全链路（Stage0a / 0b / 1 / 2 / 3）的输出格式 + 压缩配置 + shuffle 压缩，配套说明每处选择背后的取舍。
> 配套阅读：[`docs/space-optimization.md`](space-optimization.md)（压缩在整个存储压力优化体系中的位置）。

## TL;DR

管线里有**三处独立**的压缩配置，不能混为一谈：

1. **Shuffle 压缩**（mapper→reducer 中间 spill）：全管线统一 **Snappy**，在 `common/src/main/resources/companion-conf.xml:100-109` 全局开启，所有 Job 不重写。
2. **Stage0/1 落 HDFS 输出**：在 Java 代码里 `setCompressOutput(true)` + `CompressionType.BLOCK`，**codec 走 Hadoop 默认 = `DefaultCodec`（zlib/Deflate），不是 Snappy**。
3. **Stage2/3 落 HDFS 输出**：`TextOutputFormat`，**完全未压缩**（明文 CSV，下游有裸 `FSDataInputStream` 读 + `hdfs dfs -cat` 依赖）。

经验性常识两条：
- **"压缩开了" ≠ "压缩开对了"**：`mapreduce.map.output.compress` 只管 shuffle，不管 HDFS 落盘；后者必须在 Java 代码里 `setCompressOutput`。审计压缩前先读代码而不是只读 conf XML。
- **不要把 Stage1 切到 Snappy**：方向错了。Snappy 比 zlib 压得**松** 1.5–2×，切过去 Stage1 输出会涨。Snappy 是省 CPU 的方向，省空间的方向是 Zstd。

---

## 1. 三处压缩位的全景

| 位置 | 作用对象 | 配置入口 | 当前值 | 谁来读 |
|---|---|---|---|---|
| Shuffle | mapper→reducer 中间 spill（落 NM `local-dirs`） | `companion-conf.xml:100-109` 全局 | 开 / Snappy | YARN NodeManager + reducer fetcher |
| HDFS 输出（Stage0/1） | 落 HDFS 的 `SequenceFile` part 文件 | 各 Job Java 代码 `setCompressOutput` | 开 / zlib（DefaultCodec）/ BLOCK | 下游 Stage 的 `SequenceFileInputFormat` |
| HDFS 输出（Stage2/3） | 落 HDFS 的明文 CSV | 未配置 | 不压缩 | Stage3 / `cluster_head.sh` / 用户 |

三个位独立，互不传染——Stage1 输出走 zlib 不会逼下游 Stage2 也用 zlib；shuffle 走 Snappy 也不影响 HDFS 输出 codec。

---

## 2. Shuffle 压缩（全局）

**配置**：`common/src/main/resources/companion-conf.xml:100-109`

```xml
<property>
  <name>mapreduce.map.output.compress</name>
  <value>true</value>
</property>
<property>
  <name>mapreduce.map.output.compress.codec</name>
  <value>org.apache.hadoop.io.compress.SnappyCodec</value>
</property>
```

**适用于所有 Job**：Stage0a / 0b / 1 / 2 / 3 都共享这两条，没有任何 Job 在 Java 代码里重写。

**为何选 Snappy 而不是 zlib**：shuffle spill 是热路径（mapper 持续往 NM `local-dirs` 写 + reducer fetch 时解压），编解码 CPU 必须低；Snappy 压缩比 ~2×、CPU 极低，正是这场景的标配。zlib 压缩比更好但 CPU ×3–5，会拖垮 mapper 吞吐。

**为何在 conf XML 里、不在 Java 里**：全管线统一开关，一处改全管线生效。`AbstractCompanionJob` 加载 `companion-conf.xml`（`Configuration.addResource`），所有 Job 继承该 conf 自然拿到，无需各 Job 复制粘贴。

**事故关联**：见 [`docs/space-optimization.md`](space-optimization.md) §3.2 F2.5。早期 shuffle 压缩没默认开，7d 跑要手动 `-D mapreduce.map.output.compress=true ...` 传；忘传过一次就把 NM `local-dirs` 写穿（Stage1 shuffle 量 ≈ Stage1 输出量）。落到 conf XML 之后不再依赖人手记得传。

---

## 3. Stage0 / Stage1 HDFS 输出压缩

**配置入口**：每个 Job 的 `configureJob()` 里显式调用，**不依赖 conf XML**（避免和下游 Stage 解耦时混淆）。

### 3.1 代码定位

| Job | 关键代码 |
|---|---|
| Stage0a FreqJob | `stage0/src/main/java/companion/stage0/Stage0aFreqJob.java:49-50` |
| Stage0b FilterJob | `stage0/src/main/java/companion/stage0/Stage0bFilterJob.java:70-71` |
| Stage1Job | `stage1/src/main/java/companion/stage1/Stage1Job.java:70-71` |

三处都是同一模板：

```java
job.setOutputFormatClass(SequenceFileOutputFormat.class);
SequenceFileOutputFormat.setCompressOutput(job, true);
SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
// 注意：没有 setOutputCompressorClass —— 走 Hadoop 默认 codec
```

### 3.2 codec 是 zlib 而不是 Snappy

由于代码没显式调 `setOutputCompressorClass`，落地 codec 取自 `mapreduce.output.fileoutputformat.compress.codec` 的默认值 = `org.apache.hadoop.io.compress.DefaultCodec`（zlib / Deflate）。

**这是设计选择，不是疏漏**：

| 维度 | Snappy | zlib (DefaultCodec) | Stage0/1 该选谁 |
|---|---|---|---|
| 压缩比（文本/SequenceFile BLOCK） | ~2× | **3–4×** | 选 zlib：Stage1 输出是 HDFS 主导项（7d 实测 28.76 GB，占 run 88%），1 GB 压缩比差距直接转成 GB 级存储节省 |
| 编码 CPU | 极低 | 中 | 输出写一次，下游读 1–2 次；不是热路径 |
| 解码 CPU（下游读时） | 极低 | 中 | 下游 Stage1/2 顺序读 SequenceFile BLOCK，解压 CPU 不是瓶颈，I/O 才是 |

结论：Stage0/1 输出场景是"写一次读两次的批存储"，与 shuffle 的"高频热路径"性质完全相反，选 zlib 而不是 Snappy。

### 3.3 为什么用 `CompressionType.BLOCK`

`SequenceFile` 三档：`NONE` / `RECORD` / `BLOCK`。

- `RECORD`：每条 KV 单独压，压缩比差（小记录头开销大），实测 1–2 倍
- `BLOCK`：攒一批 KV（默认 1 MB）整体压，复用字典与 LZ 重叠窗口，压缩比 3–4 倍
- `NONE`：纯走 codec 无 SequenceFile 自带的批压逻辑

Stage0/1 单条 record 很小（`RecordWritable` ~30–60 B、`PairKey+LocSlotWritable` 24 B），必须用 BLOCK 才有压缩收益。

### 3.4 与 InputFormat 的对应关系

| 上游 Job | 输出格式 | 下游消费者 InputFormat | 是否需要额外解压逻辑 |
|---|---|---|---|
| Stage0a | SequenceFile + zlib BLOCK | DistributedCache → Stage0b mapper 直接反序列化 BloomFilter | 否（Hadoop 自动按文件头识别 codec） |
| Stage0b | SequenceFile + zlib BLOCK | Stage1: `SequenceFileInputFormat`（`Stage1Job.java:53`） | 否 |
| Stage1 | SequenceFile + zlib BLOCK | Stage2: `SequenceFileInputFormat`（`Stage2Job.java:44`） | 否 |

**关键**：`SequenceFileInputFormat` 读 BLOCK 压缩文件**完全透明**——文件头里写了 codec 类名，读端自动 new 对应 Decompressor。所以"Stage1 写 zlib、Stage2 没显式声明解码"不是 bug，是 SequenceFile 设计。

### 3.5 实测压缩效果（7d run `weichenyin-80925e5-...`）

| 指标 | j1a | j1b | 合计 |
|---|---|---|---|
| Reduce output records | 7,490,617,020 | 2,531,431,634 | 10.02 B |
| HDFS_BYTES_WRITTEN（zlib BLOCK 后） | 21.56 GB | 7.20 GB | **28.76 GB** |
| 估算原始量（10.02 B × 24 B） | — | — | ~240 GB |

≈ 8× 压缩比（含 SequenceFile sync marker / 块头开销），比典型 zlib 文本压缩比 3–4× 更高，原因是输出键值高度同质化（PairKey 是窄整型对、LocSlotWritable 字段重复度高），BLOCK 压缩字典命中率极高。**这就是为什么不能换 Snappy**——切过去会丢一半空间收益。

---

## 4. Stage2 / Stage3 HDFS 输出（未压缩）

### 4.1 现状

| Job | OutputFormat | 压缩 | 代码位置 |
|---|---|---|---|
| Stage2Job | `TextOutputFormat` | 无 | `stage2/src/main/java/companion/stage2/Stage2Job.java:60` |
| Stage3SortJob 主输出（companions） | `TextOutputFormat` | 无 | `stage3/src/main/java/companion/stage3/Stage3SortJob.java:116` |
| Stage3 TopNJob | `TextOutputFormat` | 无 | `stage3/src/main/java/companion/stage3/Stage3SortJob.java:254` |
| Stage2 旁路 `_hll_pairs/`（MultipleOutputs） | `TextOutputFormat` | 无 | `Stage2Job.java:62-63` |

输出形如：

```
companions/part-r-00000   ←  vid1,vid2,count
top_n.csv/part-r-00000    ←  最终 top-N 行
_hll_pairs/...            ←  Stage2 旁路给 Stage3 metric 用的 HLL 寄存器
```

### 4.2 为何留着不压缩——不是没想做

工作量评估见 [`docs/space-optimization.md`](space-optimization.md) §7.3 / §3.3。把 Stage2/3 输出加上压缩不是 1 行代码，而是要配套改：

1. `Stage3SortJob.java` 三处**裸 `FSDataInputStream` 读**，全部要套 `CompressionCodecFactory`：
   - `countLinesInDir:358`（统计 metric 用行数）
   - `scanSortedOutput:211`（Stage3 reducer 后扫一遍输出做 top-N 输入）
   - `runTopNJob:262`（硬编码 `part-r-00000` rename，要改成 glob 匹配压缩后缀）
2. `cluster_head.sh:33` 用 `hdfs dfs -cat`，要改 `hdfs dfs -text`（`-text` 会自动按 codec 解压）。
3. Stage2 旁路 `MultipleOutputs._hll_pairs/`：开了 Stage2 输出压缩它会跟着压，Stage3 metric 计算路径如果走裸读会**立挂**。

### 4.3 ROI

| 阶段 | 1d 输出 | 占 run HDFS | 31d 外推（5× 系数） | 加压缩省（按 3× 比） |
|---|---|---|---|---|
| Stage2 companions | 365 MB | 9.3% | ~1.8 GB | ~1.2 GB |
| Stage3 final | 365 MB | 9.3%（Stage2 复制 + 全排序） | ~1.8 GB | ~1.2 GB |

总共最多 **~2–3 GB**。同样的 1–2 天工时投到 Stage1 / Stage2 算法（REQ-S1-A1 / REQ-S2-A1）回报是 10–30 GB 量级或者整 run wall-time 收敛，所以这条优先级最低、最后做。

### 4.4 下游"明文依赖"清单

任何后续要给 Stage2/3 加压缩的人必须先解开这些点：

| 路径 | 当前依赖 | 改造方向 |
|---|---|---|
| `Stage3SortJob.java:358` `countLinesInDir` | 裸 `FSDataInputStream` + `BufferedReader` | 用 `CompressionCodecFactory.getCodec(path)`，存在则 `codec.createInputStream(in)` 套一层 |
| `Stage3SortJob.java:211` `scanSortedOutput` | 同上 | 同上 |
| `Stage3SortJob.java:262` `runTopNJob` rename | 硬编码 `part-r-00000` → `top_n.csv` | `globStatus("part-r-*")` 取首个，保留扩展名 |
| `scripts/cluster_head.sh:33` | `hdfs dfs -cat` | 换 `hdfs dfs -text`，对未压缩文件等价、对压缩文件自动解 |
| Stage2 `_hll_pairs/` MultipleOutputs | `TextOutputFormat` 跟主输出 | 要么单独 `setOutputFormat` 旁路，要么 Stage3 读端按 codec 走 |

---

## 5. 决策记录

把"为什么是这样、为什么不是另一样"集中在一处，省得每次审计都重新想：

| 决策 | 选择 | 拒绝项 | 原因 |
|---|---|---|---|
| Shuffle codec | Snappy | zlib | shuffle 是高频热路径，CPU 优先 |
| Stage0/1 输出 codec | zlib（DefaultCodec） | Snappy | 写一次读两次的批存储，压缩比优先；Snappy 切过来 Stage1 输出涨 ×1.5–2 |
| Stage0/1 输出 SequenceFile 压缩档 | BLOCK | RECORD / NONE | 单条 record 小（~24–60 B），只有 BLOCK 能拿到 3–4× 实际压缩比 |
| Stage0/1 codec 配置位 | Java `setCompressOutput` | conf XML | 与上游/下游 Job 解耦，避免审计 conf XML 漏看 |
| Shuffle 压缩配置位 | conf XML 全局 | 各 Job Java 重复 | 全管线统一开关，事故时单点修复 |
| Stage2/3 输出 | 不压缩（TextOutputFormat） | TextOutputFormat + codec / SequenceFile | ROI 低；下游有裸读 + `hdfs dfs -cat` 强依赖，改造成本远高于收益 |
| 副本数 | rep=1 | rep=2 | 2 worker 集群 rep=2 容错收益 ≈ 0，空间代价 ×2。见 `space-optimization.md` H4 |

---

## 6. 验证命令

### 6.1 确认 shuffle 压缩生效

```bash
# 提交一个 Job 后看 RM UI / counter
mapred job -counter <job_id> "FileSystemCounters" "FILE_BYTES_WRITTEN"
# 同时看 mapreduce.map.output.compress 是否 true（应来自 companion-conf.xml）
yarn application -status <app_id>   # 间接看 AM env
# 或者直接看 client 看到的 conf：
hdfs getconf -confKey mapreduce.map.output.compress
```

### 6.2 确认 Stage0/1 输出真是压缩

```bash
# 看 SequenceFile 头里的 codec
hdfs dfs -text /companion/runs/<run>/stage1/pair_loc_slot/part-r-00000 | head -5
# 报错 / 输出乱码 → 不是 SequenceFile
# 正常输出 → SequenceFile，header 里有 codec 类名

# 直接看文件头识别 codec
hdfs dfs -cat /companion/runs/<run>/stage1/pair_loc_slot/part-r-00000 | head -c 200 | xxd | head
# SequenceFile magic = "SEQ"，后面跟 keyclass/valclass/CompressionCodec FQCN
```

预期看到 `org.apache.hadoop.io.compress.DefaultCodec` 字样。如果意外看到 `SnappyCodec`，说明有人在 conf 里加了 `mapreduce.output.fileoutputformat.compress.codec` —— 这是反方向改动，立刻回滚。

### 6.3 确认 Stage2/3 没压缩

```bash
hdfs dfs -cat /companion/runs/<run>/stage2/companions/part-r-00000 | head -3
# 应直接看到 "vidA,vidB,count" 明文
```

---

## 7. 修改流程

任何想动压缩配置的改动按这个顺序走：

1. **明确改的是哪一处**：shuffle / Stage0-1 HDFS 输出 / Stage2-3 HDFS 输出。三者独立。
2. **如果动 codec**：先回看 §5 决策记录，确认场景属性（热路径 vs 批存储）和现有选择不冲突。
3. **如果给 Stage2/3 加压缩**：先扫 §4.4 清单的下游依赖，全部改完再开。
4. **改完跑一遍 1d 端到端**：1d 数据量在分钟级，能快速暴露 codec 配错 / 下游裸读漏改。
5. **改 conf XML 而不是 Java 时**：注意 `cluster_run.sh --build` 走 `mvn package` 把 `companion-conf.xml` 打进 jar，**必须重新 scp jar 到 master**，否则集群跑的还是老版本。

---

## 8. 索引

- 整体存储压力背景：[`docs/space-optimization.md`](space-optimization.md)
- shuffle 压缩落地的事故复盘：[`docs/space-optimization.md`](space-optimization.md) §3.2 F2.5
- Stage0/1 输出压缩在事故中的角色：[`docs/space-optimization.md`](space-optimization.md) §4 H6
- 跑 31d 前的 checklist（含压缩相关项）：[`docs/space-optimization.md`](space-optimization.md) §8
