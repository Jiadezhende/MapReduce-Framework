# Stage 开发手册（整合 owner / reviewer 视角）

本手册面向 R1（架构 / 契约）与 R6（指标 / 复现）。它不是 stage 内部算法的教程——那些在各 stage 的 README 里。它要回答的是：评审一个 stage PR 时，**怎么判断它符合跨 stage 契约**，以及**它的产出怎么进入下游和报告**。

权威源：[architecture.md](architecture.md)（契约）、[fixtures.md](fixtures.md)（字节布局）、[roles.md](roles.md)（分工）。本手册三者之上做横向梳理；与上述文档冲突时以上述文档为准。

---

## 1. 每个 stage 的产出三分类

每个 stage 的"输出"不是只有一类。评审时要分别确认这三类的边界、用途、归属：

| 类别 | 是否落盘 | 是否进入下游 stage | 谁消费 | 举例 |
|---|---|---|---|---|
| **主产出** | 是 | 是 | 下一 stage mapper | `filtered/`、`pair_loc_slot/`、`companions/`、`final/companions.csv` |
| **副产物（落盘但旁路）** | 是 | 否（不进主数据流） | 工具 / 报告 | `${COMPANION_ROOT}/profile/` 长尾直方图、Stage 2 `_hll_pairs/`、Stage 3 `_metrics.json` |
| **运行时机制** | 否（仅内存 / 单 job 生命周期） | 否 | 仅通过 counter 可见 | Stage 1 hot loc salt 切分、Stage 1 cross-slot tail buffer、Stage 2 内存 HLL 切换 |
| **框架持久化指标** | 是（JobHistory `.jhist`） | 否 | `bench/parse_counters.py` | [architecture.md §3](architecture.md) 的 13 个 Counter |

**评审常见错误**：

- 把"hot loc 切分"或"HLL fallback"当成可读取的副产物去拼装下游——它们没有稳定的字节布局，只在该 job 的内存里。要观测它们，只能读 counter。
- 把 counter 当成"仅内存"——counter 经 JobHistory 持久化在 HDFS，bench 会离线读。命名一旦变化，bench 即 silent break。
- 把 `_metrics.json` / `_hll_pairs/` 当成主产出——它们是旁路输出，不在 fixture 字节级 diff 范围内（[fixtures.md §4](fixtures.md)）。

---

## 2. 跨 stage 数据流（最终交付物视角）

```
input/raw/{phase}.csv            主产出: 文本
       │  Stage0a (BloomFilter)
       ▼
vid_freq/{phase}                 主产出: SequenceFile<NullWritable, BloomFilter>
       │  Stage0b
       ▼
filtered/{phase}                 主产出: SequenceFile<NullWritable, RecordWritable>
       │  Stage1
       ▼
pair_loc_slot/{phase}            主产出: SequenceFile<PairKey, LocSlotWritable>
       │  Stage2
       ▼
companions/{phase}/part-*        主产出: 文本 vidA,vidB,count
companions/{phase}/_hll_pairs/   副产物: HLL 估算 pair（R4 自定义 schema）
       │  Stage3
       ▼
final/{phase}/companions.csv     主产出: 文本，按 count 降序全局有序
final/{phase}/top_n.csv          主产出: 文本单文件，前 N 行
final/{phase}/_metrics.json      副产物: 端到端指标 JSON
```

旁路：

- `${COMPANION_ROOT}/profile/`：R2 profiler 独立产出，不与流水线串联，仅供调参。
- JobHistory `.jhist`：所有 4 个 stage 经 YARN 写出，由 `bench/parse_counters.py` 离线读。

**最终交付物**就是 `final/{phase}/` 三件：`companions.csv`（按序伴随车对）+ `top_n.csv` + `_metrics.json`，搭配 JobHistory Counter 作为性能与计数指标。

---

## 3. Reviewer 5 项硬性检查

PR 进入 review 时，先机械化地过这五项；任何一项不过，直接 request changes。

### 3.1 路径硬编码

```bash
git grep '/companion/' -- ':!common' ':!scripts' ':!docs'
```

零命中。stage 代码必须经 `CompanionPaths` 访问 HDFS 路径（[roles.md:33](roles.md)）。

### 3.2 配置 key 硬编码

```bash
git grep '"companion\.' -- ':!common' ':!docs'
```

零命中。stage 代码必须经 `CompanionConf` typed getter 访问配置（[roles.md:32](roles.md)）。

### 3.3 Counter 名字面量

stage 代码不允许出现 counter 字符串字面量，只能引用 `common` 里的常量 / enum。Counter 组与名清单见 [architecture.md §3](architecture.md)。改名必须同步改 `bench/parse_counters.py`。

### 3.4 Writable 字节布局

任何对 `RecordWritable` / `PairKey` / `LocSlotWritable` / `CompositeKey` 的字段、顺序、编码方式的修改，按 [fixtures.md §5](fixtures.md) 破坏性变更流程走：源码与 fixtures.md 同 PR、通知所有下游 owner、`companion-parent` minor +1、`scripts/regenerate_fixtures.sh` 重生 golden fixture。

### 3.5 Fixture 字节级一致

```bash
mvn verify
```

`FixtureGoldenRoundTripTest`（[common/src/test/java/companion/io/FixtureGoldenRoundTripTest.java](../common/src/test/java/companion/io/FixtureGoldenRoundTripTest.java)）守护 [tests/data/fixtures/](../tests/data/fixtures/) 的不变量。fixture 漂移而未重生的 PR 不能合。

---

## 4. Counter 与 bench 对接

[architecture.md §3](architecture.md) 列出 4 组共 13 个 counter。评审时关注的不是"是否计数正确"（那是 stage owner 的单测责任），而是**是否能被 bench 解析**：

- 组名必须是 `STAGE0` / `STAGE1` / `STAGE2` / `STAGE3` 之一。
- counter 名是 upper snake-case 名词，与 architecture.md 表格逐字一致。
- 新增 counter 要先改 architecture.md，再改 `bench/parse_counters.py`，最后改 stage 代码。顺序反了，bench 报告会哑掉。

**异常信号速查**（来自各 stage README 验收信号）：

| 信号 | 含义 | 处置 |
|---|---|---|
| `STAGE0.PARSE_FAIL / RAW_RECORDS > 0.1%` | 上游 CSV 脏数据比例异常 | 检查 raw 数据切分 / 编码 |
| `STAGE1.SKEW_DROP > 0` 且持续 | hot loc 切分阈值或 `loc.skew.cap` 偏低 | 重新评估 salt 策略，不要直接调大 cap 掩盖 |
| `STAGE1.INPUT_RECORDS ≠ STAGE0.KEPT_RECORDS` | Stage0→Stage1 数据流断裂 | 检查 filtered/ 路径与 SequenceFile 反序列化 |
| `STAGE2.PAIRS_INPUT ≠ STAGE1.PAIRS_EMITTED` | Stage1→Stage2 数据流断裂 | 同上 |
| `STAGE2.HLL_FALLBACK_COUNT / PAIRS_OUTPUT > 1%` | HLL 切换过频，精确路径退化 | 重新评估 `companion.hll.threshold` 与热点分布 |
| `STAGE3.TOPN_EMITTED ≠ min(pair_total, top.n)` | TopN 截取错误 | 检查 sort job 全局有序性与 part-00000 行数 |
| reducer max/mean wall time > 3（Stage1）或 > 2（Stage2） | reducer 倾斜 | 先看热点分布与 salt，再考虑加 reducer |

---

## 5. 配置 key 改动评审清单

新增或改动一个 `companion.*` 配置时，PR 必须同时修改：

1. `common/src/main/resources/companion-conf.xml`：声明默认值。
2. `common/src/main/java/companion/conf/CompanionConf.java`：加 typed getter。
3. `docs/architecture.md §2`：表格补行，标注 owner。
4. 涉及的 stage README：默认值、含义、调参建议。
5. 若 key 影响 bench 指标（如 reducer 数、阈值），同步 `bench/` 内对应解析或脚本。

漏改 1/2 → 编译失败或运行时取默认；漏改 3 → owner 不知道动了它的字段；漏改 4 → 调参者拿不到上下文；漏改 5 → bench 报告偏差。

---

## 6. Job 入口规范

所有 Job 类必须继承 `companion.job.AbstractCompanionJob`，调用约定（[architecture.md §5](architecture.md)）：

```
hadoop jar <stageX-jar> <fully-qualified-job-class> \
    <input-path> <output-path> \
    [-D companion.delta.t=600] [-D mapreduce.job.reduces=32] ...
```

- 前两个位置参数固定为 input / output，第三个起一律 `-D` override。
- stage 内部不允许 `System.exit`；交给 `ToolRunner.run` 决定退出码。
- 不允许 job 类里 hard-code phase（`1d` / `7d` / `31d`）；phase 来自外层 `scripts/cluster_run.sh` 拼出的路径。
- **打包**：每个 stage jar 是 shaded fat jar（`maven-shade-plugin` 打入 `companion:common`，`hadoop-client` 保持 `provided`）。提交无需 `-libjars`、无需额外 `HADOOP_CLASSPATH`，test 与 prod 提交命令完全一致。新建 stage 模块时照搬 `stage0/pom.xml` 的 shade 段。
- **可取消/续跑**：`cluster_run.sh` 给每个 job 传 `-D companion.run.tag=<run_id>`，`AbstractCompanionJob` 把它拼进 YARN 作业名（`<JobName> [<run_id>]`）；据此可 `cluster_cancel.sh <run_id>` 取消、或 Ctrl-C 中断（launcher 已 trap）。续跑靠输出目录的 `_SUCCESS` 标记跳过已完成 stage，job 类自身不需要感知。

---

## 7. 跨 stage 对账机制

[fixtures.md §4](fixtures.md) 与 [roles.md:68](roles.md) 规定的机制：

1. 每个 stage owner 在 `src/test/resources/` 手写**本 stage 输入**的 fixture（字节级符合 fixtures.md）。
2. 本 stage 单测先绿。
3. 上游 stage 完工后，`mvn verify` 把上游真实输出按应用层 `(key, value)` 字节序与下游 fixture diff。
4. 不一致 → 上游或下游有一方违反契约，必须有一方修。

Reviewer 在两种 PR 上要触发这条机制：

- 上游 stage 改动产出格式：必须解释 fixtures.md / golden fixture 同步策略。
- 下游 stage 新增对上游输出的假设：fixture 单测必须先在 `src/test/resources/` 显式落字节布局，不能从代码注释里推断。

---

## 8. 性能口径

R6 的报告以下列三类口径汇总，stage owner 在写章节笔记时要按这个口径喂数据：

- **Wall-clock**：YARN 提供的 job 起止时间，逐 stage 累加。
- **Shuffle bytes**：YARN `Shuffle bytes` counter。
- **Reducer 倾斜**：单 job 内 reducer wall time 的 `max/mean`，由 `bench/parse_counters.py` 计算。

stage owner **不应**在 stage 代码里另起一套计时打印（StringBuilder 日志、自定义 counter）来报性能——口径会与 YARN 不一致，bench 取数会冲突。如果确实需要 stage 内部分阶段计时，做成 stage-private counter，不入 architecture.md 表格，bench 不消费。

---

## 9. 评审常见踩坑速查

| 现象 | 根因 | 处置 |
|---|---|---|
| Stage 1 mapper 再减一次 `companion.t0` | `tNorm` 已在 Stage 0 归一化（[fixtures.md §1](fixtures.md)） | 删减法，加注释或单测固化 |
| Stage 1 partitioner 用 `hash(loc, slot)` 而非 `hash(loc, slot / 2)` | 跨 slot tail buffer 失效 | 改回 `slot / 2`；`CROSS_SLOT_PAIRS` 单测断言非零 |
| Stage 2 combiner 提前算 count 输出 | 全局 distinct `(loc, slot)` 语义破坏 | combiner 只去重 witness，不出 final count |
| Stage 2 salt 后未做二次合并 | 同一 pair 被拆到多 reducer 拿局部 count | 走 architecture.md 规定的两阶段方案或退回单阶段 |
| Stage 3 sort 退化为 1 reducer | 性能崩塌；不是全局有序的实现 | 用 `InputSampler` + `TotalOrderPartitioner`；TopN 单 reducer 是允许的 |
| Stage 3 重新扫 Stage 0/1 算 hot locs | 越权读上游主数据 | hot_locs 必须来自 Stage 1 副输出或 bench 侧统计 |
| 新 counter 没改 bench | bench 报告无声丢字段 | architecture.md → bench/parse_counters.py → stage 代码 顺序走完 |
| Job 类自带 `System.exit` | 干扰 `ToolRunner` 退出码 | 删除，返回 0 / 非 0 即可 |

---

## 10. 与本手册的关系

- 本手册仅在以下情况更新：契约新增了"跨 stage 级别"的注意事项、或 review 中重复发现新的踩坑模式。
- stage 内部算法、单测策略、私有调优属于各 stage README，不进本手册。
- 与 [architecture.md](architecture.md)、[fixtures.md](fixtures.md)、[roles.md](roles.md) 三个权威源冲突时，以那三个为准；本手册有义务追平。
