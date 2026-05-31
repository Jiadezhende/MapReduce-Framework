# 存储压力优化综述

> 范围：从 1d → 7d → 31d 三档数据规模升级中，集群"装不下 / 跑挂磁盘"系列问题的所有应对手段。
> 整合了两次 7d 失败复盘、Stage1 桶内 pair 去重方案、Stage1/2 已规划算法优化、2026-05-31 集群实测。
> 区分三档状态：**已落地** / **待评审** / **已规划未做**。

## TL;DR

7d 实测 Stage1 输出 28.76 GB（已 zlib 压缩），rep=2 时单 run 集群占用 57.5 GB，直接撞 worker 90% 健康阈值导致 Stage2 两次跑挂。Stage1 输出占整 run HDFS 体积的 **88%**，是 HDFS 容量的绝对主导；Stage2 输出仅占 9.3%，但 Stage2 shuffle 把 Stage1 整份输出再写一次到 nm-local-dir，是节点本地盘崩盘的真凶。

已做的努力分两路：

- **Hadoop 配置层** 7 项已落地（双卷 local-dirs / 双卷 log-dirs / AvailableSpace 选盘策略 / rep=2→1 / shuffle 压缩默认开 / Stage0+1 输出 zlib 压缩 / 死数据清理）
- **算法实现层** 2 项已落地（REQ-S1-A3：j1b 跳过 within-slot；REQ-S1-A0：桶内 pair 去重），3 项已规划未做（REQ-S1-A1 热 loc salt / REQ-S2-A1 PairPartitioner salt / Stage2-3 输出压缩）

剩余 31d 主要风险是 PairPartitioner 无 salt 导致的热 pair 拖尾（不爆磁盘，但 wall-time 不可控）。

---

## 1. 问题画像

### 1.1 三种"存储压力"

不是一个轴，而是三个互相牵连但物理位置不同的盘：

| 维度 | 主要写入者 | 落在哪 | 7d 实测量级 | 主要约束 |
|---|---|---|---|---|
| **HDFS 永久占用** | Stage1 reducer 输出 | DataNode 数据卷（/opt + /home 双 vol） | 28.76 GB 逻辑 × rep | DataNode 总容量 194.80 GB |
| **节点本地 shuffle** | Stage2 mapper→reducer | NM local-dir（/opt + /home 双 vol） | ~28 GB（≈ Stage1 输出） | NM 健康阈值 90% / 95% |
| **节点本地 logs** | 容器 stdout/stderr | NM log-dir | 小，但单卷易爆 | 单 vol 满即 NM UNHEALTHY |

Stage2 输出本身只有 Stage1 输出的 ~11%（1d 实测：3.4 GB → 365 MB），算法把 pair-witness 聚合成 pair 行后体积自然垮塌。**所以 Stage2 不是 HDFS 容量问题，是 shuffle 中转问题**。

### 1.2 7d 实测基线

来自 `mapred job -status job_1515238638289_0108/0109`，run `weichenyin-80925e5-20260531042733`（当前唯一一次端到端跑到 Stage1 SUCCEEDED 的 7d，rep=2 状态，Stage2 KILLED）：

| 指标 | j1a (0108) | j1b (0109) | 合计 |
|---|---|---|---|
| Map input records | 61,677,163 | 61,677,163 | — |
| Reduce output records | 7,490,617,020 | 2,531,431,634 | **10.02 B** |
| CROSS_SLOT_PAIRS | 2,540,956,445 | 2,531,431,634 | — |
| HDFS_BYTES_WRITTEN（逻辑，已含 zlib BLOCK） | 21.56 GB | 7.20 GB | **28.76 GB** |
| 集群物理占用（rep=2） | 43.12 GB | 14.40 GB | **57.52 GB** |
| FILE_BYTES_WRITTEN（shuffle 落 nm-local） | 2.47 GB | 2.47 GB | 4.93 GB |

关键确认：**Stage1 输出已是压缩后**（`Stage1Job.java:69-72`：SequenceFile + BLOCK + `setCompressOutput(true)`；codec 默认 DefaultCodec/zlib）。28.76 GB 不能再乘任何"压缩节省"。

### 1.3 1d 实测：各 stage HDFS 占比

来自 `hdfs dfs -du` on run `weichenyin-cbe21c8-20260530221139`（1d 完整跑，rep=1）：

| 阶段 | 输出大小 | 占 run 总 HDFS |
|---|---|---|
| **Stage1 pair_loc_slot** | **3.4 GB** | **88%** |
| Stage2 companions | 365 MB | 9.3% |
| Stage3 final | 365 MB | 9.3%（基本是 Stage2 复制+排序） |
| Stage0 filtered + vid_freq | 94 MB | 2.4% |

Stage1 / Stage2 输出比 ≈ 9.3×，是任何后续优化的"哪个阶段值得砍存储"的核心数据。

### 1.4 31d 容量外推

输入是 7d 的 4.13×（5.8 GB / 275.9 M 行）。pair 是 O(n²/桶) 量级，外推按 4–6× 区间：

| 增长系数 | Stage1 输出（已压缩） | 集群占用（rep=1） | 当前剩余 170 GB |
|---|---|---|---|
| 4× | 115 GB | 115 GB | 55 GB ✓ |
| 5× | 144 GB | 144 GB | 26 GB ⚠ |
| 6× | 175 GB | 175 GB | -5 GB ✗ |

---

## 2. 7d 第一次失败复盘（run `weichenyin-cbe21c8-20260530235827`）

最终诊断：**Stage2 attempt 1 因 worker2 磁盘 >90% 触发 NM UNHEALTHY，AM 容器被强制释放；attempt 2 没真正起来就被人工 kill**。表面是"节点丢失"，根因是 Stage1 输出 31 GB（rep=2 后 ~60 GB）写满了 /opt 那块 50G 盘。

### 2.1 失败链路

| 时间(EDT) | 事件 |
|---|---|
| 02:09:30 | Stage2 提交，AM 容器拉起在 `worker2:52458` |
| 02:09–02:13 | 19 个 map 容器顺利完成 |
| **02:20:31** | **worker2 NM 报警：`/opt/.../nm-local-dir` 使用率 >90.0%，标记 unhealthy** |
| 02:20:31 | NM 开始删除 AM 容器目录 |
| 02:20:10–25 | AM 大量 `Task Transitioned from SUCCEEDED to SCHEDULED`（已完成 map 因节点丢失被重排） |
| 02:26:27 | attempt 1 FAILED，exit code **-100 "Container released on a *lost* node"** |
| 02:26:27 | 人工 cluster_cancel，整 app KILLED |

worker2 NM 日志关键三行：

```
2026-05-31 02:20:31,040 WARN  DirectoryCollection: Directory /opt/.../nm-local-dir
    error, used space above threshold of 90.0%, removing from list of valid directories
2026-05-31 02:20:31,041 ERROR LocalDirsHandlerService: Most of the disks failed.
2026-05-31 02:20:31,181 INFO  DefaultContainerExecutor: Deleting absolute path :
    .../container_..._01_000001     ← AM 容器
```

排除项（都有证据）：
- **不是内存**：AM syslog 无 OOM；Stage1 Peak Reduce Physical memory 才 600 MB，离 6G 上限差得远
- **不是算法倾斜**：Stage2 才跑 11 分钟，还没到出现热 pair 拖尾的阶段
- **不是代码 bug**：AM 死前还在正常 schedule map

### 2.2 根因：HDFS 单盘填满

worker NM 配置（双 local-dir，本来就有）：

```
yarn.nodemanager.local-dirs = /opt/.../nm-local-dir, /home/yarn/nm-local-dir
dfs.datanode.data.dir       = /opt/.../dfs/data,     /home/hdfs/data
```

跑挂时两块盘实际占用（失衡严重）：

| 路径 | worker1 | worker2 | 所在盘 | 容量 / 使用率 |
|---|---|---|---|---|
| `/opt/.../dfs/data` | **34 GB** | **34 GB** | `/` | 50G / 86% |
| `/home/hdfs/data` | 18 GB | 18 GB | `/home` | 48G / 37% |

两块盘容量基本一样，/opt 多了 16G，**起跑就比 /home 高，后面始终追不平**。

根因：HDFS DataNode 默认 `dfs.datanode.fsdataset.volume.choosing.policy = RoundRobinVolumeChoosingPolicy`——**轮询写下一个 volume，不看哪个还剩多少**。OS + Hadoop 安装本就占了 /opt 几个 G，起跑差距永远填不平。

Stage1 写 31 GB（× 2 副本 ≈ 62 GB）+ Stage2 启动后 shuffle 中间文件，叠加之下 /opt 跨过 90% 阈值。NM 自爆 → AM 被踢 → attempt 1 failed。

### 2.3 修复（4 项）

| # | 改动 | 文件 / 命令 |
|---|---|---|
| F1.1 | 清旧 run 释放 HDFS（6 dir → 2 dir）| `hdfs dfs -rm -r -skipTrash /companion/runs/<旧>` |
| F1.2 | HDFS 选盘策略 RoundRobin → AvailableSpace | 三节点 `hdfs-site.xml` 加 `dfs.datanode.fsdataset.volume.choosing.policy=AvailableSpaceVolumeChoosingPolicy` + `balanced-space-threshold=10737418240`（10G）+ `preference-fraction=0.85` |
| F1.3 | 滚动重启 DataNode | 一台一台：`hdfs --daemon stop datanode && sleep 2 && hdfs --daemon start datanode`，HDFS 全程 2 节点存活 |
| F1.4 | （下次跑要带）shuffle 压缩 -D | `-Dmapreduce.map.output.compress=true -Dmapreduce.map.output.compress.codec=SnappyCodec` |

结果：DFS Used 降到 84 GB / 195 GB（47.7%）。

---

## 3. 7d 第二次失败复盘（run `weichenyin-80925e5-20260531042733`）

带 §2.4 推荐的 shuffle 压缩 -D 重跑，Stage2 又挂在 worker / >90%。**表面同 §2.1，但触发位置不同，且暴露出 §2.3 那次没看到的两个深层问题**。

### 3.1 新发现

1. **log-dirs 单卷才是这次的触发点，不是 local-dirs**。§2.3 把 `local-dirs` 改成双卷有效（`Disk(s) failed: 1/2 local-dirs` 不致命），但 `log-dirs` 仍只配了 `/opt/.../logs/userlogs` 一条，`1/1 log-dirs failed` → `Most disks failed` → NM 整机 UNHEALTHY → AM SIGTERM(exit 143)。
2. **/ 基线被 6 GB 死数据垫高**：`/opt/module/hadoop-3.3.6/dfs/data` 是 2023-12 老安装残留（4.7 GB），`/opt/module/hadoop-3.3.6/tmp/nm-local-dir` 是老路径（1 GB）。`AvailableSpaceVolumeChoosingPolicy`（§2.3 配的）只调控新写，不动存量，/ 比 /home 永远高 6 GB 起跑，7d 写入下 / 必先爆。
3. **rep=2 是空间放大器**：Stage1 j1b 输出 27.5 GB，rep=2 ⇒ 集群 55 GB，每 worker ~14 GB 落 /；rep=1 直接砍半。**两 worker 集群 rep=2 买的容错很有限**（挂一台就基本全停），不值这个空间代价。

### 3.2 修复（5 项）

| # | 改动 | 文件 / 命令 |
|---|---|---|
| F2.1 | 清死数据 | 两 worker `rm -rf /opt/module/hadoop-3.3.6/{dfs,tmp/nm-local-dir}` + `rm -f /opt/module/jdk-*.tar.gz`。活的 DN 在 `data/dfs/data`+`/home/hdfs/data`，活的 NM 在 `data/tmp/nm-local-dir`+`/home/yarn/nm-local-dir`，死路径名字接近，删前 `ls -la` 看时间戳确认 |
| F2.2 | 删失败 run + appcache | `hdfs dfs -rm -r /companion/runs/weichenyin-80925e5-...` + 两 worker 删 appcache |
| F2.3 | log-dirs 跨卷 + 阈值 | `yarn-site.xml` 加 `log-dirs=/opt/.../logs/userlogs,/home/yarn/logs/userlogs` + `max-disk-utilization=95`；预步 `mkdir -p /home/yarn/logs/userlogs`；两 worker `yarn --daemon stop/start nodemanager` 同步；NM REST `:8042/conf` 验证 `source=yarn-site.xml` |
| F2.4 | `dfs.replication` 2 → 1 | 三节点 `hdfs-site.xml` 同步改；存量 `hdfs dfs -setrep -R -w 1 /companion`。client-side 属性，无需重启 NN/DN |
| F2.5 | 默认开 shuffle 压缩 + slowstart=0.9 | `common/src/main/resources/companion-conf.xml` 加 `mapreduce.map.output.compress`+codec+`reduce.slowstart.completedmaps=0.9`；mvn package + `cluster_run.sh --build` 重新 scp jar；以后不用每次手动传 -D |

`slowstart=0.9` 单独提：本次 AM `totalResourceLimit=<6144MB,7vCores>`，4 reducer 在 `completedMapPercent=0.265` 时已经占了 4 vCore，mapper 排队跑；0.9 让 mapper 跑完 90% 再放 reducer 进。

**效果**：DFS Used 80 GB → 13 GB，worker1 / 62% → **9%**，worker2 / 62% → 28%（setrep 删的副本正好集中在另一卷，后续新写靠 AvailableSpace 自然均衡）。

### 3.3 显式没做（持有的技术债）

- **Stage2/3 输出加压缩**：`Stage3SortJob.java` 有 3 处裸 `FSDataInputStream` 读（`countLinesInDir:358`、`scanSortedOutput:211`、`runTopNJob:262` 硬编码 `part-r-00000` rename）+ `cluster_fetch.sh` 用 `hadoop fs -cat`，光开 Stage2 输出压缩会让 `MultipleOutputs` 的 `_hll_pairs/` 也压，Stage3 立挂。要做需配套 `CompressionCodecFactory` 改裸读 + rename glob + cluster_fetch 改 `-text`，单开。
- **Stage1/Stage2 reducer 数被静默吞掉**：`Stage1Job.java:67`、`Stage2Job.java:57-58` 用 `job.setNumReduceTasks(conf.getInt(MRJobConfig.NUM_REDUCES, fallback))`。`MRJobConfig.NUM_REDUCES`（即 `mapreduce.job.reduces`）在 `mapred-default.xml` 永远有默认值 1，`conf.getInt` 永远返回那个 1 而不是 fallback。规避方式：脚本始终传 `-D mapreduce.job.reduces=${RED}`（`cluster_run.sh:93 RED_CONF`），生产路径不踩；代码修复留作技术债。
- **`balanced-space-preference-fraction` 0.85 → 0.95**：线上仍是 0.85（2026-05-31 实测 `hdfs-site.xml`）。§3.2 修复直后 worker / 都跌到 < 30%，"不急"的判断成立；但 7d 跑活跃期 worker2 / 会回到 60%+（实测当前 62%，剩 20 GB），双卷差距 ~20%。31d 跑前再 `df` 一次，若 worker2 / 超 70% 建议升 0.95 让 AvailableSpace 更激进往空卷写。

### 3.4 跨次失败的关键经验

1. **真集群 trouble-shoot 第一步看 RM log 找 `Transitioned from RUNNING to UNHEALTHY` / `LOST`**，直接告诉你是节点级问题，不用绕进 AM syslog。
2. **exitCode -100 = "Container released on a lost node"**，99% 是 NM 自爆（磁盘 / 心跳）。看 NM 日志 `DirectoryCollection` / `LocalDirsHandlerService` 关键字定位。
3. **HDFS 多盘配置不等于自动负载均衡**，默认 RoundRobin 在容量相等但起始占用不等时永远填不平。生产环境必须显式开 `AvailableSpaceVolumeChoosingPolicy`。
4. **NM `log-dirs` 和 `local-dirs` 是独立的健康轴**，两者都要冗余配多卷。任何一轴 `N/N` 全失败都让节点 UNHEALTHY，不管另一轴有多健康。
5. **小集群 rep=1 是默认应有的选择，不是优化项**——2 worker 的 rep=2 容错收益约等于 0，空间代价 ×2。决定 rep 之前先问"挂一台机能继续吗"，答案是"不能"时 rep=2 就是纯浪费。
6. **`AvailableSpaceVolumeChoosingPolicy` 不能跨基线差**：只调控新写不动存量，治本是清"死数据"（orphan DN dir、老路径 NM local）。
7. **上游 stage 计数器是失败 stage 的伏笔**：本次 Stage2 KILLED 看 Stage2 诊断什么都看不出，必须去看 Stage1 输出量是不是本身就大到撑爆下游。

---

## 4. Hadoop 配置层已落地（7 项汇总）

把 §2 / §3 修复总结成横向表：

| # | 改动 | 配置文件 | 解决的问题 | 来源 |
|---|---|---|---|---|
| H1 | 双卷 local-dirs | `yarn-site.xml`: `local-dirs=/opt/.../nm-local-dir,/home/yarn/nm-local-dir` | 单卷满即 NM UNHEALTHY，shuffle 文件没地方再写 | §2.3 之前已有，事故时确认有效 |
| H2 | **双卷 log-dirs** | `yarn-site.xml`: `log-dirs=/opt/.../logs/userlogs,/home/yarn/logs/userlogs` + `max-disk-utilization=95` | §3.1 触发点；local-dirs 双卷不顶用，log-dirs `1/1 failed` 也算 "Most disks failed" | §3.2 F2.3 |
| H3 | `AvailableSpaceVolumeChoosingPolicy` | `hdfs-site.xml` + 三节点滚动重启 DataNode | 默认 RoundRobin 不看剩余空间，/opt vs /home 起跑差 6 GB 永远填不平 | §2.3 F1.2 |
| H4 | **`dfs.replication` 2 → 1** | `hdfs-site.xml` + `hdfs dfs -setrep -R -w 1 /companion` | 2 worker 集群 rep=2 容错收益约等于 0，空间代价 ×2 | §3.2 F2.4 |
| H5 | shuffle 压缩 + slowstart=0.9 写入默认 | `common/src/main/resources/companion-conf.xml` 加 `mapreduce.map.output.compress=true` + Snappy codec + `mapreduce.job.reduce.slowstart.completedmaps=0.9` | nm-local-dir shuffle spill 体积砍 ~½；slowstart 避免 reducer 在 mapper 还没跑完时占满 vcore | §3.2 F2.5 |
| H6 | Stage0/1 输出压缩（确认而非新做） | `Stage0aFreqJob.java:48` / `Stage0bFilterJob.java:70` / `Stage1Job.java:69-72` 均 `SequenceFileOutputFormat.setCompressOutput(true)` + `CompressionType.BLOCK` | HDFS 永久占用直接砍。codec 取 `mapreduce.output.fileoutputformat.compress.codec` 默认值 DefaultCodec(zlib) | 代码层已有，2026-05-31 conf 实测确认 |
| H7 | 死数据清理 | 两 worker `rm -rf /opt/module/hadoop-3.3.6/{dfs,tmp/nm-local-dir}` + 老 `jdk-*.tar.gz` | 2023-12 老安装残留 ~6 GB 把 / 永久垫高，§2 的 AvailableSpace 只调控新写 | §3.2 F2.1 |

**当前集群状态**（2026-05-31）：DFS Used 13.70 GB / Configured 194.80 GB，Remaining 170.17 GB；worker1 / 9%、worker2 / 28%。

### 关键认知

- **"压缩开了" ≠ "压缩开对了"**：`mapreduce.map.output.compress` 只管 shuffle 中间文件，不管 HDFS 输出。后者必须 `setCompressOutput` 在 Java 代码里设（H6 已设）。审计任何"压缩没开"的判断前先看代码而非只看 conf XML。
- **DefaultCodec(zlib) 不是 Snappy**：当前 Stage1 输出走 zlib（文本压缩比 3–4×）。"切 Snappy 省空间"是错的——Snappy 比 zlib 压得**松** 1.5–2 倍，切过去 Stage1 输出会涨。Snappy 是省 CPU 的方向；想更省空间该评估 Zstd。
- **rep=2 → rep=1 是单点收益最大的"集群配置"改动**：5 分钟改 + setrep 跑一会，单 run 集群占用直接 ×½。

---

## 5. 算法实现层已落地

### REQ-S1-A3：j1b 跳过 within-slot 发射

**问题**：补偿两趟 j1a / j1b 都对每个 `(loc, slot)` 完整发射 within-slot pair，Stage2 端靠 dedup 吃掉。j1b 实际只是为了补 cross-slot 边界缺口，within-slot 重复计算白白翻倍 Stage2 输入。

**改造**：`Stage1Reducer.setup` 读 `slotOffset`，offset==1（j1b 趟）时 `emitWithinSlotPairs` 直接返回；j1a 负责所有 within-slot + cross-slot(2k→2k+1)，j1b 只发射 cross-slot(2k+1→2k+2)。

**实测验证**（7d run 80925e5）：j1b `PAIRS_EMITTED == CROSS_SLOT_PAIRS == 2,531,431,634`，within-slot 全跳过。j1b 数据量从理论 ~958 M 降到 ~311 M（1d 基线下），Stage2 输入整体砍 ~34%。

---

## 6. 算法实现层已落地：REQ-S1-A0 桶内 Pair 去重

完整设计材料：`docs/stage1-bucket-dedup.md`。本节整合落地实现 + 与存储压力的直接联系。

### 6.1 问题定位

`Stage1Reducer` 在单个 `(loc, slot)` 桶内对同一对车 `(V1, V2)` 重复发射 pair-witness，重复倍数约等于 `nA × nB`（V1、V2 在该桶内的记录数乘积）。`PAIRS_EMITTED` 中相当大一部分是这类重复，Stage2 端靠 `HashSet<Long>` 去重吃掉。

**1d 实测异常**：

```
PAIRS_EMITTED        = 958,942,040
Reduce input groups  = 182,214       （distinct (loc, slot) 桶数）
平均 emit/桶         = 5,257
平均 record/桶       = 44
桶内若无重复发射上限 = C(44, 2) = 946
```

观测 5,257 是理论上限的 ~5.5×。一部分来自长尾热桶，一部分来自桶内重复发射——后者可证伪、可消除。

### 6.2 根因（代码定位）

**within-slot 发射路径** `Stage1Job.java:306-311`：

```java
for (SeenRecord prev : window) {          // window 可包含同一 vid 的多条记录
    emitPair(prev.vid, cur.vid, loc, slot, context);
}
```

**cross-slot 发射路径** `Stage1Job.java:290-304`：

```java
for (SeenRecord prev : tailBuffer) {      // tailBuffer 同样可含同 vid 多条
    if (cur.ts - prev.ts > deltaT) continue;
    if (emitPair(prev.vid, cur.vid, loc, slot, context)) { ... }
}
```

核心 emit 只跳过 `vid1==vid2`，对 `(V1@t1i, V2@t2j)` 的每个时间组合都发射一次。设 V1 有 nA 条、V2 有 nB 条且全在 deltaT 窗口内：within 路径发射 nA × nB 次，cross 路径再叠 nA' × nB'，**全部 tagged 为同一 `(loc, slot)`**，Stage2 dedup → 1 个 witness。

### 6.3 已落地实现

`Stage1Reducer` 内加桶级 dedup set，within / cross 两路统一查表。当前代码（`Stage1Job.java:221-339`）：

```java
public static class Stage1Reducer extends Reducer<...> {
    // Stage1Job.java:229
    private final Set<Long> emittedPairs = new HashSet<>();

    @Override
    protected void reduce(CompositeKey key, Iterable<RecordWritable> values, Context context) {
        // ...
        window.clear();
        emittedPairs.clear();           // Stage1Job.java:261：每个 (loc, slot) 桶开始时清空
        // ...
    }

    // Stage1Job.java:319-335
    private boolean emitPair(int vid1, int vid2, int loc, int slot, Context context) {
        if (vid1 == vid2) return false;
        int vidA = Math.min(vid1, vid2);
        int vidB = Math.max(vid1, vid2);
        long pair = encodePair(vidA, vidB);
        if (!emittedPairs.add(pair)) {  // first-seen 才下发
            return false;
        }
        outKey.set(vid1, vid2);
        outValue.set(loc, slot);
        context.write(outKey, outValue);
        context.getCounter(COUNTER_GROUP_STAGE1, Stage1Counter.PAIRS_EMITTED.name()).increment(1L);
        return true;
    }

    // Stage1Job.java:337-339：vid 顺序归一化后 pack 成 long
    private static long encodePair(int vidA, int vidB) {
        return ((long) vidA << 32) ^ (vidB & 0xffffffffL);
    }
}
```

落地版与原始提案的唯一差异：归一化采用 `Math.min/max` + 异或 pack（提案中是 `|`），语义等价。

### 6.4 实测收益与残留风险

落地后 `PAIRS_EMITTED` 计数器口径已变成"distinct (pair, loc, slot) 数"，7d j1b 计数器 `PAIRS_EMITTED == CROSS_SLOT_PAIRS == 2,531,431,634` 与 j1a 7,490,617,020（合计 10.02 B）即为去重后的真实下游输入量。原始发射次数无法直接对照（口径切换前未保留 raw counter）。

残留待办：

1. **dedup set 内存上界仍裸奔**：最坏单桶 D 个 distinct vid → C(D,2) 个 pair。`HashSet<Long>` 单元素 ~48 B；D=5,000 → 12.5 M pair → 600 MB。当前 `locSkewCap=200,000` 是 record 数不是 vid 数，对此无效。热桶 vid 数破万时仍会打爆 reducer 堆。**建议**：加 `emittedPairs.size()` 上限保护（5 M / 240 MB），超过则关闭 dedup 改回原始模式继续；新增计数器 `DEDUP_DISABLED_BUCKETS`。当前 7d 跑没撞，但 31d 上 vid 密度更高，需提前防。
2. **HashSet → primitive long set**：换 eclipse-collections `LongHashSet` 单元素从 48 B 降到 ~12-16 B；不引依赖则手写 `LongOpenHashSet` 约半天工作量。是上一条限流之外的另一条降内存路径。
3. **`PAIRS_EMITTED_RAW` 计数器**：当前没记原始发射次数，A0 的"压缩倍数"无法事后核算。如果以后想量化 A0 收益（比如对比 31d 上的真实重复倍数），需要在 `emitPair` 顶部加 probe counter `PAIRS_EMITTED_RAW`（写在 set.add 之前），开销可忽略。

### 6.5 与其他优化的关系

| 项 | 关系 |
|---|---|
| REQ-S1-A1（热 loc salt 拆分，未做） | **正交**，可同时落地。salt 把热 loc 跨 reducer 拆分后，每个 reducer 看到的桶 distinct vid 数变小，A0 的 dedup set 内存上界同步下降，互相利好 |
| REQ-S1-A3（已落地，j1b 跳 within-slot） | **协同**。A3 让 j1b 不再发 within-slot，A0 在 j1b 端 dedup set 只装 cross 路径 emit 的 pair，内存压力天然更小 |
| REQ-S2-A1（PairPartitioner salt，未做） | **下游受益**。A0 已先把 Stage2 输入砍掉一半左右，salt 拆分的收益基础更大 |
| §2/§3 集群磁盘崩盘 | **直接缓解**。Stage2 shuffle 落盘量随 A0 输入下降同比例下降，是 §3 中 nm-local-dir 不再撞 90% 的关键贡献者之一 |

---

## 7. 算法实现层已规划未做

### 7.1 REQ-S1-A1：热 loc salt 拆分

**问题**：`SkewAwarePartitioner.getPartition` 用 `HashUtil.mix(loc, (slot+offset)/2)`，单 loc 的所有 slot 配对永远去同一个 reducer。1d 数据下平均 reduce task 3.3 min，wall-time 却 10 min——单点拖尾 3×；7d 实测 j1a 平均 reduce 328 s 但 wall-time 拖得更久。

**改造**：引入 `companion.stage1.loc.salt.threshold`（默认 50,000 record/loc）。Mapper 阶段维持每 `(loc)` 计数 sketch，超阈值的 loc 在 partitioner 里追加 salt 维度 `mix(loc, (slot+offset)/2, vid % saltN)`；配套修改 grouping comparator，salt 只作为分区辅助键，reduce 端 grouping 仍按 `(loc, slot)`。

**对存储的影响**：不直接砍存储，但拆开热 loc 后单 reducer 拖尾消失，Stage1 wall-time 从 10 min → 4–5 min。**与 A0 协同**：salt 后单桶 distinct vid 数小，A0 的 dedup set 内存上限更易满足。

### 7.2 REQ-S2-A1：PairPartitioner 引入 salt（双轮 J2a/J2b）

**问题**：`PairPartitioner.getPartition` 用 `HashUtil.mix(vidA, vidB) % numPartitions`，单个热 pair 的所有 witness 必然落同一 reducer。即使 reducer 数加到 48 个，热 pair 也只占 1 个，wall-time 不会变。

**改造**（推荐方案：双轮聚合）：
1. **J2a**：用 `mix(vidA, vidB, witness_salt)` 分区，`salt = (loc * 31 + slot) % PAIR_SALT_N`；reducer 做局部 dedup/HLL，输出 `(pair, partial_witness_set 或 partial_HLL)`
2. **J2b**：用 `mix(vidA, vidB) % numPartitions` 分区，reducer 做最终合并（HashSet 求并集或 HLL register 取 max），过 `kMin` 后输出

`PAIR_SALT_N` 默认 16，1d 数据下 J2a 可放 64-128 个 reducer，J2b 16-32。

**对存储的影响**：不直接砍存储，但热 pair 不再单 reducer 串行处理几千万行 witness。**是 31d Stage2 能不能 wall-time 收敛的决定项**——31d 上 Stage2 不收敛的话整 run 时长不可预测，跑挂了也不知道是磁盘还是热 pair。

### 7.3 Stage2/Stage3 输出加压缩

**问题**：Stage2 输出只占 run 的 9.3%（1d 实测 365 MB / 3.4 GB），砍它收益小。

**为何没做**：`Stage3SortJob.java` 有 3 处裸 `FSDataInputStream` 读（`countLinesInDir:358` / `scanSortedOutput:211` / `runTopNJob:262` 硬编码 `part-r-00000` rename）+ `cluster_fetch.sh` 用 `hadoop fs -cat`，光开 Stage2 输出压缩会让 `MultipleOutputs` 的 `_hll_pairs/` 也压，Stage3 立挂。要做需配套：
- `CompressionCodecFactory` 改裸读
- rename 改 glob 匹配
- `cluster_fetch.sh` 改 `hadoop fs -text`

工作量约 1-2 天，但 ROI 低（最多砍 ~30 MB 在 1d，~3 GB 在 31d），单开。

### 7.4 集群配置层未做

| 项 | 备注 |
|---|---|
| `balanced-space-preference-fraction` 0.85 → 0.95 | 线上仍 0.85；7d 跑活跃期 worker2 / 已回到 62%，31d 跑前若 worker2 / > 70% 升 0.95 |
| 老 34 GB 数据从 /opt 搬到 /home | `AvailableSpaceVolumeChoosingPolicy` 不动存量，要均衡老数据得 `hdfs diskbalancer` |
| 评估 Zstd 替代 zlib | 压缩比 +10-20%，CPU 介于 zlib / Snappy 之间；要测稳定性，性价比一般 |
| log-aggregation 打开 (`yarn.log-aggregation-enable`) | yarn-site 改完要同步到 worker 并重启 NM，对共享集群侵入大；UI 看不到 stderr 暂可接受 |

### 7.5 不做的事

| 项 | 不做的理由 |
|---|---|
| 把 Stage1 codec 切到 Snappy | 反方向，会让 Stage1 输出涨 1.5–2× |
| `dfs.replication` 改回 2 | 2 worker 集群 rep=2 没有容错收益，空间代价 ×2 |

---

## 8. 跑 31d 前的 checklist

### 8.1 集群侧验证

```bash
# 1. HDFS 剩余 ≥ 170 GB（当前 170.17 GB，临界）
ssh master "hdfs dfsadmin -report | grep -E 'DFS Remaining|Configured'"

# 2. 两 worker / 卷在 60% 以下（H3 / H4 已配，但要确认数据均衡到位）
ssh master "for h in worker1 worker2; do echo === \$h ===; ssh \$h 'df -h / /home'; done"

# 3. 清旧 run（保留必要的，其他释放）
ssh master "hdfs dfs -ls /companion/runs/"

# 4. 输入就位
ssh master "hdfs dfs -ls /companion/input/raw/31d.csv"
```

### 8.2 提交命令

```bash
scripts/cluster_run.sh --days 31 --build
```

不需要再手传任何 `-D`。`scripts/env.sh:TUNE_31D` 已固化所有 31d 专用参数，`cluster_run.sh` 按 `--days 31` 自动拼到每个 stage 提交命令上。

`TUNE_31D` 内容与取舍（基于 master NM/DN 上线后 20 GB / 18 vCore / 3 DN 的实际容量）：

| 参数 | 取舍 |
|---|---|
| `REDUCERS_31D=32`（在 env.sh） | 7d 的 32 已是当前并发槽位的最佳点；加到 128 只是排队 25 波，wall-time 不变 |
| `reduce.memory.mb=2048`<br>`reduce.java.opts=-Xmx1536m` | 7d 实测 Peak Reduce Physical 600 MB，2 GB 是 3.4× 余量。reducer 内存从 4 GB 降到 2 GB，并发槽位 5 → 9（master 1 + worker1 4 + worker2 4），Stage1/2 wall-time 砍 1/3 |
| `map.memory.mb=1536`<br>`map.java.opts=-Xmx1024m` | map 端类似下调，并发 ~10 → ~13 |
| `task.io.sort.mb=400` | map sort buffer 从默认 100 MB 提到 400 MB，spill 次数 ~½，Stage2 期间 worker 本地盘累计写量从 297 GB（7d 数）降到 ~150 GB，关键防 worker2 `/` 撞 95% 红线 |
| `companion.hll.threshold=100000` | 防热 pair OOM（保留 7d 用法） |

shuffle 压缩、Stage0/1 输出压缩已在 H5/H6 默认开，不用再传 `-D`。

### 8.3 已知风险（无算法改造的前提下）

| 风险 | 触发场景 | 影响 |
|---|---|---|
| **HDFS 撑满** | Stage1 输出 > 170 GB（增长系数 ≥ 6×）| Stage1 写不下，整 run 挂 |
| **Stage2 热 pair 拖尾** | REQ-S2-A1 未做（§7.2） | 1 个 reducer 跑数小时，其余早完；wall-time 不可控（但不爆磁盘） |
| **Stage1 倾斜** | REQ-S1-A1 未做（§7.1） | 单 reducer wall-time 3–10× |

如果想让 31d 跑得"既稳又快"，**优先把 REQ-S2-A1 落了**——它不直接砍存储，但 Stage2 不收敛的话 31d 整 run 时长不可预测。

如果想进一步给 HDFS 留余量、降低撑满风险，**优先把 §6.4 残留项（dedup set 内存上限保护 + `PAIRS_EMITTED_RAW` probe）做了**——前者防 31d 高密度热桶把 reducer 堆打爆，后者让 A0 收益可量化对照。

---

## 9. 经验小结

按"杠杆 / 工作量"排序，本轮系列优化最值得反复指出的改动：

1. **`dfs.replication` 2 → 1**（H4）：5 分钟改 + setrep，单 run 集群占用直接 ×½。两 worker 集群 rep=2 是反向决策，不是"通用谨慎"。第一版方案以"通用谨慎"为由保留 rep=2，代价是省 27.5 GB 的最大杠杆漏了。
2. **REQ-S1-A3（j1b 跳过 within-slot）**：~一天工作量，Stage1 总输出量 -34%，Stage2 输入连带砍。
3. **双卷 log-dirs（H2）**：被忽视的健康轴。修了 local-dirs 不修 log-dirs，§3 还是会挂。`log-dirs` 和 `local-dirs` 是独立的健康轴，两者都要冗余配多卷。
4. **死数据清理（H7）**：6 GB 老安装把 `AvailableSpaceVolumeChoosingPolicy` 的均衡能力废了——它只调控新写不动存量。治本是 `du -sh /opt/module/hadoop-3.3.6/*` 扫"死数据"。
5. **认清 Stage1 输出本就压缩着的**（H6）：避免误判和无效的"切 Snappy 省空间"决策。Stage1 输出 28.76 GB 是压缩后 → 31d 容量评估别再乘任何"压缩节省"。conf-only 审计漏看 Java 代码里的 `setCompressOutput`，会导出错误结论。

跨"集群配置 vs 算法"两个层面看：
- 集群配置层修的是**物理资源约束**（盘满、副本浪费、单卷易爆），是低工作量的快速止血。
- 算法实现层修的是**数据量本身**（pair 重复、shuffle 量大、热 reducer 拖尾），是高工作量但治本的杠杆。
- 两层缺一不可：光改集群配置，数据量增长一档就再次撞墙；光改算法，单点物理瓶颈一样会让 NM 自爆。
