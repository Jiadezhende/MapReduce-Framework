# 31d 全量首跑失败复盘 — `31399-cf1f2f6-20260601004404`

提交 commit **cf1f2f6**，`scripts/cluster_run.sh --days 31`，`TUNE_31D` 全套参数已生效（见 [`scripts/env.sh`](../../../scripts/env.sh)）。Stage0/Stage1 全部 SUCCEEDED，**Stage2（app `_0129`）在 map ~28% 时 worker2 NM 自爆，被人工 kill**。无最终产出，故本目录只有本复盘，没有 `_metrics.json` / `top_n.csv`。

> 一句话结论：**这次不是 HDFS 装不下（Stage1 仅 103.5 GB，远低于 §1.4 外推的 115–175 GB），是 Stage2 shuffle 把每个 worker 的本地盘（nm-local-dir）打穿 95% 健康阈值。** `TUNE_31D` 里 `io.sort.mb=400` 防的是 spill *累计*写量，防不住 shuffle 的*常驻峰值*——后者下界 = Stage1 整份输出 ÷ 节点数，与 sort buffer 无关。

## 1. 失败链路

客户端提交日志（map% 时间线）：

| 时间 | 事件 |
|---|---|
| 13:16 | Stage2（`_0129`）提交，AM 拉起在 worker1 |
| 13:16 → 14:53 | map 0% → **28%**，平稳爬升（~1.6h） |
| **~14:53–14:56** | **worker2 NM → UNHEALTHY，容器全清，已完成的 map 因节点丢失被重排** |
| 14:56 | map **28% → 17%** 回退（即用户察觉"不对劲"的点） |
| ~15:08 | 现场诊断：worker2 `/` 97% / `/home` 96%，两卷 nm-local-dir 38G | 
| 15:1x | 人工 `cluster_cancel` → app KILLED（`Final-State: KILLED`，Aggregate 136.9M MB-s / 66705 vcore-s） |

集群侧实测（EDT，诊断时刻）：

```
yarn node -status worker2:59881
  Node-State : UNHEALTHY
  Health-Report : 2/2 local-dirs ... used space above threshold of 95.0%
      [ /home/yarn/nm-local-dir , /opt/module/hadoop-3.3.6/data/tmp/nm-local-dir ]
                  ; 2/2 log-dirs ... above 95.0%
```

## 2. 根因：Stage2 shuffle 常驻量打穿本地盘

### 2.1 数据（`hdfs dfs -du` on `/companion/runs/31399-cf1f2f6-20260601004404`）

| 产物 | 大小 | 备注 |
|---|---|---|
| `vid_freq` (Stage0a) | 435.5 MB | |
| `filtered` (Stage0b) | 1.8 GB | |
| **`pair_loc_slot` (Stage1)** | **103.5 GB** | zlib 压缩后，rep=1，占 run HDFS 98% |
| `companions` (Stage2) | 0 | 未产出 |

- 103.5 GB / 7d 26.8 GB = **3.86×**（输入行数比 275.9M/66.9M=4.13×，pair 增长略低于线性，桶级去重 A0 在更高密度下收益更大）。
- 对照 [`space-optimization.md §1.4`](../../space-optimization.md) 外推（4×=115 GB）：**实际比最低档还低**，HDFS 全程剩余 193 GB，容量轴这次完全不是约束。

### 2.2 崩盘点 = nm-local-dir，不是 HDFS

worker2 崩盘时本地盘（诊断现场实测）：

| 卷 | 容量 | 使用率 | 其中 nm-local-dir(app `_0129`) |
|---|---|---|---|
| `/home` | 48 G | **96%** | `/home/yarn/nm-local-dir` = **24 G** |
| `/` | 50 G | **97%** | `/opt/.../data/tmp/nm-local-dir` = **15 G** |

- 单 worker shuffle 常驻 ≈ **38–39 G** ≈ `103.5 GB ÷ 3 节点`（轻微偏斜，worker2 略高）。userlogs 仅几 MB，**不是日志问题**。
- 这 38G 全部属于正在跑的 `_0129`（`du` 确认 appcache owner 唯一是 `application_..._0129`），**不是旧 run 残留**。
- NM 只在 app 结束时清 appcache → worker2 在 `_0129` 被 kill 前不可能自愈 → 这趟跑永久少一节点。

### 2.3 kill 后即恢复，反证根因

`cluster_cancel` 后 NM 立即回收 appcache，worker2 当场恢复：

| 卷 | 崩盘时 | kill 后 |
|---|---|---|
| `/` | 97% | **68%**（34G/50G） |
| `/home` | 96% | **47%**（23G/48G） |
| NM 状态 | UNHEALTHY | **RUNNING** |

崩盘→kill→恢复差值正好 ≈ 38G，闭环验证了"38G 是 `_0129` 活 shuffle"这一根因。

## 3. 为什么 `TUNE_31D` 没拦住

[`space-optimization.md §8.2`](../../space-optimization.md) 设 `io.sort.mb=400` 的预期是"Stage2 期间 worker 本地盘*累计*写量从 297 GB 降到 ~150 GB，关键防 worker2 `/` 撞 95%"。但：

- `io.sort.mb` 减的是 map 端 spill 合并的**重写次数**（累计 I/O 量），减不了**任一时刻驻留**在 nm-local-dir 等 reducer 拉取的 map 输出总量。
- 该常驻量的下界 = **Stage2 全部 map 输出 = Stage1 整份输出**（Stage2 mapper 基本是把 Stage1 输出原样重发去聚合），按节点平摊 ≈ `103.5/3 ≈ 34.5 G/节点`，与 sort buffer 完全无关。
- 这正是 [`space-optimization.md §1.1 / §7.3`](../../space-optimization.md) 早已点名的"Stage2 shuffle 把 Stage1 整份输出再写一次到 nm-local-dir，是节点本地盘崩盘的真凶"。本次是该已知风险在 31d 量级下的兑现。

物理余量核算（单 worker，扣 OS + HDFS DataNode 数据后）：`/`50G + `/home`48G 两卷总 98G，HDFS DataNode 占 ~45–48G，OS ~6G，留给 nm-local-dir 的稳定余量 ~40G——而需要 ~38G 且有偏斜。**起跑就贴在红线上，偏斜一推就破。**

## 4. 调整方案（已落地）

### 4.1 先排除两条看似显然、实则无效的路

- **加盘 / 扩 local-dirs**：审计后排除。每 worker 仅一块 100G 物理盘（`sda`），已全部分给 root(50G)+swap(2G)+home(47.5G)，VG `centos` VFree 仅 **64 MiB**，无未分配容量、无第二块盘。nm-local-dir 早已是双卷（H1），不是"只落一个盘"。
- **REQ-S2-A1（salt 两轮 J2a/J2b）**：**对本次崩盘无益、甚至更糟**，不做。理由：
  1. 它治的是"热 pair 拖尾"，但实测直方图（[7d `_metrics.json`](../7d-49d70f2/_metrics.json)）单 pair witness 数封顶 <1000、`hll_pair_count=0`（HLL 阈值 100K 从未触发）——**这数据没有热 pair**；31d 是 7d 时间超集，单 pair 计数最多 ~×4 到几千，仍远不构成单 reducer 跑数小时的拖尾。
  2. J2a 的 mapper 仍透传 → 其 map 输出 = 同样 319G，salt 只散到更多 reducer 桶，**一字节不少**；且 A0 后每个 `(pair,loc,slot)` 已全局唯一 → reducer 去重去不掉一条 → 非热 pair 经两轮反而把 shuffle ~翻倍，**加重磁盘**。

### 4.2 实际采用：A（密编码 shuffle）+ D（pair-hash 分轮）

只有"砍每节点 map 输出字节"能治本次崩盘。

- **A. shuffle codec Snappy→Gzip(zlib)**：zlib 比 Snappy 密 ~1.6–1.7×，materialized 峰值 106→~63 G/节点。固化在 `scripts/env.sh:TUNE_31D`（`-D mapreduce.map.output.compress.codec=...GzipCodec`，`mapreduce.map.output.compress=true` 已默认开）。
- **D. 按 `mix(vidA,vidB)%K` 分 K 轮串跑**：每轮只 emit 1/K → 单节点 shuffle 峰值 /K。一个 pair 的全部 witness 共享 `(vidA,vidB)` → 必落同一轮，各轮 pair 集**不相交**，输出直接拼接即精确结果，无需 partial 合并。实现：`Stage2Mapper` 加 round 过滤 + `CompanionConf.stage2Rounds/Round` + `cluster_run.sh` 串跑 K 轮写 `companions/<phase>/r{k}/` + Stage3 开 `input.dir.recursive`。
- **组合与定档**：`STAGE2_ROUNDS_31D=3` + Gzip。定 K=3 而非 2 的原因见 §4.4——master 只跑 ~17% map、两个 worker 各扛 ~41%，有效节点数 ~2.4 不是 3。1d/7d 维持 K=1，零影响。

不做：再调大 `io.sort.mb`（治的是 spill 累计写量，不是常驻峰值）；抬高 95% 阈值（物理盘真满，只延后崩盘）。

### 4.3 复跑验证入口

Stage1 的 103.5G 仍在 `pair_loc_slot/`，可只重跑 Stage2+Stage3：

```bash
scripts/cluster_run.sh --days 31 --from stage2 --run-id 31399-cf1f2f6-20260601004404 --build
```

跑时盯 `df`（worker2 `/` 为瓶颈），期望峰值 ~40 G、稳在 47.5 G 红线下；顺带核对 31d 直方图是否真有 ≥1万 witness 的 pair（若有才需重估 REQ-S2-A1）。

### 4.4 为什么定 K=3 而非 2：master 份额 + worker2 偏重

两个实测把"理想 ÷3"打回现实：

1. **master 只跑 ~17% 的 map**：master NM 仅配 `resource.memory-mb=4096`/2 vcore（worker 是 8192/8），@`map.memory.mb=1536` 只有 2 个 map 槽，两个 worker 各 5 个 → master 占 ~17%、worker 各 ~41%。map 中间输出落在跑该 map 的节点本地盘上，所以单 worker 实扛 **0.41** 份而非 0.33，有效节点数 ~2.4。这正是首跑 28%（而非 ~45%）就崩的原因：`0.41 × 319G × 28% ≈ 36G` ≈ 实测 38G。master 是 8G 机器兼跑 NN+RM+DN，4G NM 已近安全上限，硬调大有拖垮 NN/RM 的风险，故按 ~2.4 节点规划。
2. **worker2 的 `/` 卷天生偏重**：rebalance 前 worker2 DN 53.4%（最重），`/` 用 35G、到 95% 红线仅 12.5G 余量。跑了一次 `hdfs balancer -threshold 5`（带宽调到 50MB/s，3.7 min 搬 10 GB 到最空的 master），worker2 → 43%、`/` 降到 29G、余量升到 18.5G。

定档核算（Gzip，worker 0.41 份，按上次崩盘实测的卷分布 `/home:/ ≈ 62:38` 外推）：

| K | worker2 `/` 峰值（29G 基线 +）| vs 47.5 G 红线 |
|---|---|---|
| K=2 | ~45 G | 仅 2.5 G，太险 |
| **K=3** | **~40 G** | **~7.5 G，稳** |

所以 rebalance + `STAGE2_ROUNDS_31D=3` 是最终定档。rebalance 对后续所有跑都有益，应作为 31d 跑前的常规预步（见 §8 checklist）。

### 4.5 重跑实测（`_0131`，2026-06-01 16:44 起，进行中）

`--from stage2` 复跑(沿用 `pair_loc_slot/` 的 103.5G)。`job.xml` 实参确认 K=3+Gzip 真正生效：`companion.stage2.rounds=3`、`companion.stage2.round=0`、`map.output.compress.codec=GzipCodec`、reduces=32，输出落 `companions/31d/r0/`。

**磁盘（验证修复有效）**：第 0 轮 map ~8% 时实测 worker2 `/` 30G/59%、`/home` 20G/41%、nm-local-dir 合计仅 ~3.2G；worker1 `/` 25G/50%。三节点 NM 全 RUNNING(master 2 + worker1 4 + worker2 4 容器)，无 UNHEALTHY。峰值轨迹完全压在 47.5G 红线下，**§4.2 的 A+D 修复确认生效，未复现崩盘**。

**wall-time（补文档此前缺的真实数据）**：

| 实测项 | 值 |
|---|---|
| 单轮 map 数 | **867**（103.5G ÷ 128MB 块） |
| 平均单 map 耗时 | ~117 s（9,036 s / 77 launched） |
| 有效并发 map 槽 | ~9（master 2 + worker 各 ~4，扣 AM） |
| **单轮 map 阶段** | `867 × 117s ÷ 9 ≈` **~3 h** |
| **Stage2 总估** | 3 轮 × (map ~3h + reduce) ≈ **~10–13 h** |

**关键修正**：每一轮都**重扫整份 103.5G Stage1 输出**（mapper 读全量、只 emit `mix%3==k` 的 1/3），所以 K=3 的成本是 **3× 全量重扫 ≈ ~9h 纯 map**——这是 Stage2 慢的主因，不是热 pair、不是倾斜。据此**修正 §4.4 的 K 取舍**：K=2 比 K=3 省的不是"零头"，而是**一整轮 ~3–4h**；但仍只换来 worker2 `/` 余量 7.5G→2.5G，一次中途崩盘赔掉整个 10h+ run，不划算。真要砍掉这 9h 重扫，只能上 REQ-S2-A1（单 pass salt 分区，不重扫），见 [`space-optimization.md §7.2`](../../space-optimization.md)。
