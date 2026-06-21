# 7d 全量跑 Stage2 失败 trouble-shooting 报告

本文档记录 2026-05-31 第一次端到端跑 7d 数据时 Stage2 失败的排查与修复。

- run_id: `weichenyin-cbe21c8-20260530235827`
- 失败 stage: Stage2 (`application_1515238638289_0100`)
- 集群:Hadoop 3.3.6,master(10.176.62.218)+ worker1/worker2 各 8GB / 8 vcore
- 关联文档:[cluster-troubleshooting](cluster-troubleshooting.md) §1-§6 是这次跑前已经修过的环境问题

最终诊断:**Stage2 attempt 1 因 worker2 磁盘 >90% 触发 NM UNHEALTHY,AM 容器被强制释放;attempt 2 没真正起来就被人工 kill**。表面看是"节点丢失",根因是 Stage1 输出 31GB 写满了 /opt 那块 50G 盘。

---

## §1 失败链路

| 时间(EDT) | 事件 |
|---|---|
| 02:09:30 | Stage2 提交,AM 容器拉起在 `worker2:52458` |
| 02:09–02:13 | 19 个 map 容器顺利完成 |
| **02:20:31** | **worker2 NM 报警:`/opt/module/hadoop-3.3.6/data/tmp/nm-local-dir` 使用率 >90.0%,标记 unhealthy** |
| 02:20:31 | NM 开始删除 AM 容器目录 `container_xxx_01_000001` |
| 02:20:10–25 | AM 大量 `Task Transitioned from SUCCEEDED to SCHEDULED`(已完成 map 因节点丢失被重排) |
| 02:26:27 | attempt 1 FAILED,exit code **-100 "Container released on a *lost* node"** |
| 02:26:27 | 人工 cluster_cancel,整个 app KILLED |
| 02:28:01 | worker1 UNHEALTHY → RUNNING |
| 02:28:22 | worker2 UNHEALTHY → RUNNING |

worker2 NM 日志关键三行:

```
2026-05-31 02:20:31,040 WARN  DirectoryCollection: Directory /opt/.../nm-local-dir
    error, used space above threshold of 90.0%, removing from list of valid directories
2026-05-31 02:20:31,041 ERROR LocalDirsHandlerService: Most of the disks failed.
2026-05-31 02:20:31,181 INFO  DefaultContainerExecutor: Deleting absolute path :
    .../container_1515238638289_0100_01_000001     ← AM 容器
```

排除项(都有证据):
- **不是内存**:NM 日志、AM syslog 都没有 OOM / Exception / "killing container ... beyond physical memory"。Stage1 的 `Peak Reduce Physical memory` 才 600 MB,离 6G 上限差得远。
- **不是算法倾斜**:Stage2 才跑 11 分钟,74 个容器已分配,还没到出现热 pair 拖尾的阶段。
- **不是代码 bug**:AM 死前还在正常 schedule map。

---

## §2 上游 Stage 计数器(都 SUCCEEDED)

来自 `mapred job -status job_1515238638289_009{6,7,8,9}`。

| 指标 | Stage0b filter | Stage1 j1a | Stage1 j1b |
|---|---|---|---|
| Input records | — | 61,677,163 | 61,677,163 |
| Output records | 61,677,163 | **8,307,895,851** | **2,725,099,178** |
| CROSS_SLOT_PAIRS | — | 2,735,802,625 | 2,725,099,178 |
| 内含 within-slot pair | — | ~5.57 B(保留) | 0(已跳过 ✅) |
| HDFS 写入(压缩前) | — | 23.47 GB | 7.61 GB |
| 平均 reduce 时长 | — | 328 s(~5.5 min) | 115 s(~1.9 min) |
| Reducer 数 | — | 32 | 32 |
| Killed task | — | 1 reduce | 1 map |

**亮点**:j1b 的 `PAIRS_EMITTED == CROSS_SLOT_PAIRS`,确认 [stage1-optimization](stage1-optimization.md) REQ-S1-A3(j1b 跳过 within-slot)**已落地**。

**Stage2 实际入参**:8.31B + 2.73B ≈ **11.0 B 条 pair-witness / ~31 GB**,是 1d 基线(~1.92B)的 ~5.7×。

---

## §3 根因:HDFS 单盘填满

worker NM 配置(双 local-dir,本来就有,不是这次加的):

```
yarn.nodemanager.local-dirs = /opt/.../nm-local-dir, /home/yarn/nm-local-dir
dfs.datanode.data.dir       = /opt/.../dfs/data, /home/hdfs/data
```

跑挂时两块盘实际占用(失衡严重):

| 路径 | worker1 | worker2 | 所在盘 | 容量 / 使用率 |
|---|---|---|---|---|
| `/opt/.../dfs/data` | **34 GB** | **34 GB** | `/` | 50G / 86% |
| `/home/hdfs/data` | 18 GB | 18 GB | `/home` | 48G / 37% |

两块盘容量基本一样,/opt 多了 16G,**起跑就比 /home 高,后面始终追不平**。

根因:HDFS DataNode 默认 `dfs.datanode.fsdataset.volume.choosing.policy = RoundRobinVolumeChoosingPolicy`——**轮询写下一个 volume,不看哪个还剩多少**。OS + Hadoop 安装本就占了 /opt 几个 G,起跑差距永远填不平。

Stage1 写 31GB(× 2 副本 ≈ 62GB)+ Stage2 启动后 shuffle 中间文件,叠加之下 /opt 跨过 90% 阈值。NM 自爆 → AM 被踢 → attempt 1 failed。

---

## §4 修复 1:清旧 run 释放 HDFS

清理前 `/companion/runs/` 6 个 dir,清理后 2 个(用户指定保留):

```bash
# 保留:
#   weichenyin-cbe21c8-20260530221139
#   weichenyin-cbe21c8-20260530235827
# 删除:
ssh master 'for r in bytedance-599a5df-20260530035036 \
                     weichenyin-f0170e9-2026053017{1328,4338} \
                     weichenyin-f0170e9-20260530180437; do
  hadoop fs -rm -r -skipTrash /companion/runs/$r
done'
```

结果:DFS Used 降到 **84.04 GB / 194.80 GB (47.7%)**,Remaining 92 GB。

---

## §5 修复 2:HDFS volume 选盘策略

3 个节点 `hdfs-site.xml` 都新增以下 property,备份为 `hdfs-site.xml.bak.<ts>`:

```xml
<property>
  <name>dfs.datanode.fsdataset.volume.choosing.policy</name>
  <value>org.apache.hadoop.hdfs.server.datanode.fsdataset.AvailableSpaceVolumeChoosingPolicy</value>
</property>
<property>
  <name>dfs.datanode.available-space-volume-choosing-policy.balanced-space-threshold</name>
  <value>10737418240</value>  <!-- 10 GB -->
</property>
<property>
  <name>dfs.datanode.available-space-volume-choosing-policy.balanced-space-preference-fraction</name>
  <value>0.85</value>
</property>
```

含义:两块盘可用空间差 >10GB 时,85% 的新 block 写空得多的那块,否则继续轮询。

滚动重启 DataNode(一台一台,HDFS 全程 2 节点存活):

```bash
ssh worker1 'hdfs --daemon stop datanode && sleep 2 && hdfs --daemon start datanode'
# 等 dfsadmin -report 看到 worker1 回来再做 worker2
ssh worker2 'hdfs --daemon stop datanode && sleep 2 && hdfs --daemon start datanode'
```

启动日志确认生效:

```
AvailableSpaceVolumeChoosingPolicy: Available space volume choosing policy initialized:
    balanced-space-threshold = 10737418240,
    balanced-space-preference-fraction = 0.85
```

`hdfs fsck /` → HEALTHY,无 missing/corrupt block。

**遗留**:老的 34GB 数据仍在 /opt(用户指示先不搬)。新写入会偏向 /home,但 /opt 还是 86%——下次跑大数据前再 `df` 确认下,必要时跑一次 `hdfs diskbalancer` 把老 block 搬过去。

---

## §6 缓解:启用压缩(下次跑必加,本次未应用)

§4 / §5 的修复解决了**选盘失衡**,但没解决**7d 数据本身就大**这件事——Stage1 输出 31 GB,Stage2 shuffle 还要再叠一份中间文件。开两个压缩开关能直接砍掉两边占用,属于"运行参数而非集群配置",改起来便宜,记下来续跑时必带。

| 开关 | 作用 | 预期收益 |
|---|---|---|
| `mapreduce.map.output.compress=true` + Snappy | shuffle 中间文件压缩(写 nm-local-dir 之前压一次) | nm-local-dir 占用 ~½,**直接缓解 §3 那种磁盘满** |
| `mapreduce.output.fileoutputformat.compress=true` + Snappy | reduce 最终输出落 HDFS 时压缩 | Stage1 j1a 23.47 GB + j1b 7.61 GB,预计压到 ~15 GB 总,HDFS 直接少一半 |

完整 -D 示例:

```bash
-Dmapreduce.map.output.compress=true
-Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
-Dmapreduce.output.fileoutputformat.compress=true
-Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec
-Dmapreduce.output.fileoutputformat.compress.type=BLOCK
```

> `compress.type=BLOCK` 给 SequenceFile 用,对纯文本输出无影响,带上无副作用。

**为什么选 Snappy 不选 Zstd**:Snappy CPU 开销低,Hadoop 内置 codec 默认推荐;11B 条 pair-witness 走 shuffle 时压缩耗时不可忽略,本集群 vcore 紧(16 个),先不上 Zstd。后续如果磁盘仍是瓶颈、CPU 富余,再切 Zstd 看压缩比提升。

**要不要固化到 `mapred-site.xml`**:暂不。压缩开关写进集群 conf 后所有 job 都受影响,目前小数据 fixture 测试和大数据跑共用一套配置,通过 -D 按需开更灵活。等 7d 稳定通跑后再视情况固化(届时改 master 一份、`scp` 同步两个 worker,**不需要重启**,client 端读 mapred-site 即可生效)。

**本次失败时的状态**:Stage1 j1a 23.47 GB / j1b 7.61 GB 写盘都是**未压缩**;Stage2 shuffle 也未压缩。开了的话 Stage1 阶段就已经少占 15+ GB,Stage2 attempt 1 大概率不会撞 90% 阈值。

---

## §7 未修(代码层,本次没改)

下面这几个**都不是本次失败的直接原因**,但跟 trouble-shooting 强相关,记录下来便于后续。

### 7.1 Stage1/Stage2 reducer 数被静默吞掉

文件:`Stage1Job.java:65`、`Stage2Job.java:57`(继承自 [cluster-troubleshooting §6 末尾遗留反模式](cluster-troubleshooting.md))

```java
job.setNumReduceTasks(conf.getInt(MRJobConfig.NUM_REDUCES, CompanionConf.stage2Reducers(conf)));
```

`MRJobConfig.NUM_REDUCES`(即 `mapreduce.job.reduces`)在 `mapred-default.xml` 永远有默认值 1,`conf.getInt` 永远返回那个 1 而不是 fallback 的 `CompanionConf.stage*Reducers`。本次脚本传了 `-D mapreduce.job.reduces=32`,Stage1 j1a/j1b 都跑到了 32 个 reducer;但 **Stage2 实际拿到 `companion.stage2.reducers=8`**(来自 companion-conf.xml,不是 -D),证明这个路径上的 -D 没生效——具体是 cluster_run.sh 调 Stage2 时拼参数被吞,还是 Stage2 自己读错,需要再看一次 staging 里的 `job_xxx_0100_1_conf.xml`。

**修复**:改成 `setNumReduceTasks(CompanionConf.stage*Reducers(conf))`,把唯一旋钮收敛到 `companion.stage*.reducers`。Stage0b 的 `applyTrailingDefines` 兜底可以保留也可以删,改完后冗余但无害。

### 7.2 Stage2 PairPartitioner 无 salt

文件:`Stage2Job.java:106`,目前 `Math.floorMod(HashUtil.mix(key.getVidA(), key.getVidB()), numPartitions)`。

热 pair 的所有 witness 必落同一 reducer,即使 reducer 数从 8 提到 32 也只是分 32 个桶中的 1 个跑,其余 31 个 reducer 早早完成。详见 [stage2-optimization](stage2-optimization.md) REQ-S2-A1(双轮 J2a/J2b 改造)。

**本次没暴露**只是因为 attempt 1 还没跑到那一步就被节点丢失打死。下次续跑(disk 修好之后)很可能撞到这个。

### 7.3 Stage1 SkewAwarePartitioner 无 salt

文件:`Stage1Job.java:215`,同类问题。详见 [stage1-optimization](stage1-optimization.md) REQ-S1-A1。本次 j1a 平均 reduce 时长 328 s 但 wall-time 受 killed reduce + 倾斜影响,实测能跑通,但 7d 数据上倾斜系数大概 3×。

---

## §8 续跑建议

集群侧问题修了之后续跑 Stage2:

```bash
scripts/cluster_run.sh --days 7 --run-id weichenyin-cbe21c8-20260530235827 \
    --from stage2 \
    -Dcompanion.stage2.reducers=32 \
    -Dmapreduce.map.memory.mb=2048 -Dmapreduce.map.java.opts=-Xmx1536m \
    -Dmapreduce.reduce.memory.mb=4096 -Dmapreduce.reduce.java.opts=-Xmx3072m \
    -Dmapreduce.map.output.compress=true \
    -Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
    -Dmapreduce.output.fileoutputformat.compress=true \
    -Dmapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
    -Dmapreduce.output.fileoutputformat.compress.type=BLOCK \
    -Dcompanion.hll.threshold=100000
```

参数取舍:

- `companion.stage2.reducers=32` 绕过 §7.1 反模式(直接喂业务 conf 而非 `mapreduce.job.reduces`)。
- `reduce.memory.mb=4096`(不是 6144):8GB worker 上 6G reducer 太挤,4G 能并行 2 个 reducer/worker。
- **两组压缩开关**:shuffle 中间文件 + HDFS 最终输出都开 Snappy,详见 §6。这次磁盘满的最直接缓解。
- `hll.threshold=100000`:防止热 pair 在 HashSet 阶段 OOM,见 stage2-optimization REQ-S2-C3。

跑之前先看一下两个 worker 的 `df -h /` 确认 /opt 那块还低于 80%。

---

## §9 经验

1. **真集群 trouble-shoot 第一步看 RM log 找 `Transitioned from RUNNING to UNHEALTHY` / `LOST`**。直接告诉你是节点级问题,不用绕进 AM syslog。
2. **exitCode -100 = "Container released on a lost node"**,99% 是 NM 自爆(磁盘 / 心跳),少数是 NM 进程崩。看 NM 日志的 `DirectoryCollection` / `LocalDirsHandlerService` 关键字定位。
3. **HDFS 多盘配置不等于自动负载均衡**,默认 RoundRobin 选盘策略在容量相等但起始占用不等时永远填不平差距。生产环境必须显式开 `AvailableSpaceVolumeChoosingPolicy`。
4. 计数器看上游 stage 是否健康,别只看失败 stage 的诊断 —— 本次 Stage1 输出量本身就是 Stage2 失败的伏笔(31GB),光看 Stage2 KILLED 看不出来。

---

# 第二次 7d(2026-05-31 下午,run `weichenyin-80925e5-20260531042733`)

带 §6 推荐的 shuffle 压缩 -D 重跑,Stage2 又挂在 worker / >90%。表面同 §1,但**触发位置不同**,且暴露出 §5 那次没看到的两个深层问题。

## §10 新发现

1. **log-dirs 单卷才是这次的触发点**,不是 local-dirs。§5 把 `local-dirs` 改成双卷有效(`Disk(s) failed: 1/2 local-dirs` 不致命),但 `log-dirs` 仍只配了 `/opt/.../logs/userlogs` 一条,`1/1 log-dirs failed` → `Most disks failed` → NM 整机 UNHEALTHY → AM SIGTERM(exit 143)。
2. **/ 基线被 6GB 死数据垫高**:`/opt/module/hadoop-3.3.6/dfs/data` 是 2023-12 老安装残留(4.7GB),`/opt/module/hadoop-3.3.6/tmp/nm-local-dir` 是老路径(1GB)。`AvailableSpaceVolumeChoosingPolicy`(§5 配的)只调控新写,不动存量,/ 比 /home 永远高 6GB 起跑,7d 写入下 / 必先爆。
3. **rep=2 是空间放大器**:Stage1 j1b 输出 27.5GB,rep=2 ⇒ 集群 55GB,每 worker ~14GB 落 /;rep=1 直接砍半。两 worker 集群 rep=2 买的容错很有限(挂一台就基本全停),不值这个空间代价。

## §11 修复

| 改动 | 文件 / 命令 | 备注 |
|---|---|---|
| 清死数据 | `rm -rf /opt/module/hadoop-3.3.6/{dfs,tmp/nm-local-dir}` + `rm -f /opt/module/jdk-*.tar.gz` | 两 worker;活的 DN 在 `data/dfs/data`+`/home/hdfs/data`,活的 NM 在 `data/tmp/nm-local-dir`+`/home/yarn/nm-local-dir`,死路径名字接近,删前 `ls -la` 看时间戳确认 |
| 删失败 run + appcache | `hdfs dfs -rm -r /companion/runs/weichenyin-80925e5-...` + 两 worker rm appcache/application_xxx_0110 | |
| log-dirs 跨卷 + 阈值 | `yarn-site.xml` 加 `log-dirs=/opt/.../logs/userlogs,/home/yarn/logs/userlogs` + `max-disk-utilization=95`,两 worker 同步,`yarn --daemon stop/start nodemanager` | 预步 `mkdir -p /home/yarn/logs/userlogs`。NM REST `:8042/conf` 验证 `source=yarn-site.xml` |
| `dfs.replication` 2→1 | 三节点 `hdfs-site.xml` 同步改;存量 `hdfs dfs -setrep -R -w 1 /companion` | client-side 属性,无需重启 NN/DN |
| 默认开 shuffle 压缩 + slowstart=0.9 | `common/src/main/resources/companion-conf.xml` 加 `mapreduce.map.output.compress`+codec+`reduce.slowstart.completedmaps=0.9` | mvn package + `cluster_run.sh --build` 重新 scp jar;以后不用每次手动传 -D |

`slowstart=0.9` 单独提:本次 AM `totalResourceLimit=<6144MB,7vCores>`,4 reducer 在 `completedMapPercent=0.265` 时已经占了 4 vCore,mapper 排队跑;0.9 让 mapper 跑完 90% 再放 reducer 进。

**效果**:DFS Used 80GB → 13GB,worker1 / 62% → **9%**,worker2 / 62% → 28%(setrep 删的副本正好集中在另一卷,后续新写靠 AvailableSpace 自然均衡)。

## §12 显式没做

- **Stage2/3 输出加压缩**:`Stage3SortJob.java` 有 3 处裸 `FSDataInputStream` 读(`countLinesInDir:358`、`scanSortedOutput:211`、`runTopNJob:262` 硬编码 `part-r-00000` rename)+ `cluster_head.sh:33` 用 `hdfs dfs -cat`,光开 Stage2 输出压缩会让 `MultipleOutputs` 的 `_hll_pairs/` 也压,Stage3 立挂。要做需配套 `CompressionCodecFactory` 改裸读 + rename glob + cluster_head 改 `-text`,单开。
- **§7.1 NUM_REDUCES 代码兜底**:`cluster_run.sh:93 RED_CONF` 始终传 `-D mapreduce.job.reduces=${RED}`,生产路径不触发陷阱;reducer 动态调整本就该在脚本层(env.sh `REDUCERS_1D/7D/31D=8/32/128`)。
- **`balanced-space-preference-fraction` 0.85→0.95**:清理+rep=1 后 / 还剩 37GB,本次不急。

## §13 经验补充(在 §9 基础上)

5. **NM `log-dirs` 和 `local-dirs` 是独立的健康轴**,两者都要冗余配多卷。任何一轴 `N/N` 全失败都会让节点 UNHEALTHY(`Most disks failed`),不管另一轴有多健康。
6. **小集群 rep=1 是默认应有的选择**,不是优化项 —— 2 worker 的 rep=2 容错收益约等于 0,空间代价是 2×。决定 rep 之前先问"挂一台机能继续吗",答案是"不能"时 rep=2 就是纯浪费。第一版方案这条被我以"通用谨慎"的理由删掉,代价是省 27.5GB 的最大杠杆漏了。
7. **`AvailableSpaceVolumeChoosingPolicy` 不能跨基线差**。只调控新写,不动存量;/ 和 /home 起跑差 6GB 时,均衡只能延缓不能抹平 —— 治本是 `du -sh /opt/module/hadoop-3.3.6/*` 扫"死数据"(orphan DN dir、老路径 NM local),日期老旧但不在当前配置里的目录直接清。
