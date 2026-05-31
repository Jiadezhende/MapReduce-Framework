# Master NodeManager 上线记录（2026-05-31）

## 一、动机

Stage2（run `weichenyin-49d70f2-20260531134458`）map 阶段 wall-time 2h 5m，counter 显示并发 map 仅 ~14 个，已饱和当前 2-worker × 8 GB / 8 vcore 的容器槽位。把 master 也拉进 NM 池，新增 ~2 个 map 容器槽。

**预期收益**：集群容器从 16 GB / 16 vcore → 20 GB / 18 vcore（+25% 内存、+12.5% vcore）。Stage2 map 并发预计从 ~14 → ~16（+15%）。

## 二、变更范围

仅改 **master** 的 `yarn-site.xml`。worker1 / worker2 / hdfs-site / mapred-site / workers / yarn-env / hadoop-env **均未动**。

变更前后 diff（master `/opt/module/hadoop-3.3.6/etc/hadoop/yarn-site.xml`）：

```diff
 <configuration>
+    <!-- 注释：master-only，禁止同步到 worker -->
+
     <property>
         <name>yarn.resourcemanager.hostname</name>
         <value>master</value>
     </property>
     <property>
         <name>yarn.nodemanager.aux-services</name>
         <value>mapreduce_shuffle</value>
     </property>
     <property>
         <name>yarn.nodemanager.local-dirs</name>
         <value>/opt/module/hadoop-3.3.6/data/tmp/nm-local-dir,/home/yarn/nm-local-dir</value>
     </property>
+
+    <!-- 新增：master NM 资源上限 -->
+    <property>
+        <name>yarn.nodemanager.resource.memory-mb</name>
+        <value>4096</value>
+    </property>
+    <property>
+        <name>yarn.nodemanager.resource.cpu-vcores</name>
+        <value>2</value>
+    </property>
+
+    <!-- 新增：对齐 worker 5-31 防护补丁（log 跨双 volume + 磁盘阈值 95%） -->
+    <property>
+        <name>yarn.nodemanager.log-dirs</name>
+        <value>/opt/module/hadoop-3.3.6/logs/userlogs,/home/yarn/logs/userlogs</value>
+    </property>
+    <property>
+        <name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name>
+        <value>95</value>
+    </property>
 </configuration>
```

## 三、容量预算依据

master 物理资源 4 vCPU / 8 GB RAM，已运行 NN + SNN + RM + JHS。

| 用途 | RAM | vCPU |
|---|---|---|
| OS + buff/cache | ~0.8 GB | ~0.2 |
| NameNode | ~1.0 GB（实测 RSS 远低） | ~0.5 |
| SecondaryNameNode | ~0.5 GB | ~0.2 |
| ResourceManager | ~1.0 GB | ~0.5 |
| JobHistoryServer | ~0.4 GB | ~0.1 |
| NodeManager 自身 | ~1.0 GB | ~0.5 |
| **守护进程合计** | **~4.7 GB** | **~2.0 vCPU** |
| **NM 容器预算** | **3 GB（实际配 4 GB，超卖 33%，靠守护进程 RSS 远低于 Xmx 的事实兜底）** | **2 vCPU** |

NM 启动前实测 master daemon RSS 合计 **1.9 GB**（不是估算的 4.7 GB），所以 4 GB 容器预算实际有 ~1 GB 安全余量。

## 四、为什么没动其他参数

- **不改 `yarn-env.sh` 压 NM heap**：默认 1 GB heap 在 master 上够用，没必要再调
- **不加 node label 把 reducer 关在 worker**：master NM 内存 4 GB / vcore 2，调度器在容器请求时自然偏向走 worker 的 8 GB 节点；如果观察到 reducer 落 master 严重影响，再补 label
- **`workers` 文件不加 master**：保持 master NM 手动启停（`yarn --daemon start/stop nodemanager`），避免 `start-yarn.sh` 误启停
- **`dfs.replication=1` 单副本不动**：本次只动 YARN，不动 HDFS；DataNode 是否上 master 留作后续评估
- **vmem-check 不显式关**：worker 也没显式关，沿用默认（开启，2.1 倍）；NM 启动至今未观测到 vmem kill

## 五、部署执行步骤

```bash
# 1. 备份
ssh master 'cp /opt/module/hadoop-3.3.6/etc/hadoop/yarn-site.xml \
  /opt/module/hadoop-3.3.6/etc/hadoop/yarn-site.xml.bak.$(date +%Y%m%d-%H%M%S)'
# 实际备份：yarn-site.xml.bak.20260531-184740

# 2. 预建目录（不预建 NM 也会自建，权限会一致）
ssh master 'mkdir -p /home/yarn/nm-local-dir /home/yarn/logs/userlogs \
  /opt/module/hadoop-3.3.6/data/tmp/nm-local-dir'

# 3. 写入新 yarn-site.xml（heredoc）

# 4. 启动 NM
ssh master 'yarn --daemon start nodemanager'

# 5. 验证（等 ~25 秒让 NM 注册到 RM）
ssh master 'yarn node -list -showDetails'
```

## 六、上线后基线

|  | NM 启动前 | NM 启动后 |
|---|---|---|
| master 5min load | 0.01 | 0.04 |
| NN RpcProcessingTimeAvgTime | 0.0 ms | 0.0 ms |
| NN RpcQueueTimeAvgTime | 0.0 ms | 0.0 ms |
| NN CallQueueLength | 0 | 0 |
| NN heap used | 106 MB | 107 MB |
| master daemon RSS 合计 | 1896 MB | 2895 MB（+999 MB ≈ NM JVM） |
| 集群容量 | 16384 MB / 16 vCores | 20480 MB / 18 vCores |
| activeNodes | 2 | 3 |

NN/RM/JHS/SNN **零退化**。

## 七、回退步骤

```bash
ssh master 'yarn --daemon stop nodemanager'
ssh master 'cp /opt/module/hadoop-3.3.6/etc/hadoop/yarn-site.xml.bak.20260531-184740 \
  /opt/module/hadoop-3.3.6/etc/hadoop/yarn-site.xml'
# NN / RM / JHS / SNN 不重启，不受影响
ssh master 'yarn node -list'  # 应回到 2 节点
```

## 八、需观测的指标（下次 Stage2 跑完后核对）

1. **master 容器实际拿到几个 map**：JHS 看 task assignment 是否包含 `master:48748` 的容器
2. **master load 峰值**：作业期间 1min load 不应超过 4.0（4 物理核）
3. **NN RpcQueueTimeAvgTime**：作业期间不应 > 10ms（健康集群通常 < 1ms）
4. **master `/` 盘用量**：观察 NM container log 写入速率，确认未触发 95% 阈值
5. **Stage2 map 阶段 wall-time**：对比 49d70f2 的 2h 5m，看是否下降到 ~1h 50m（理论 14/16 比例）

如果 NN RPC 退化明显（队列 > 50 或 processing > 50ms），优先回退 NM；如果只是 load 偏高但 RPC 稳定，可以容忍。

## 九、关联约定

- master `yarn-site.xml` 顶部已加注释 `!!! master-only — DO NOT sync to workers !!!`
- 文档同步更新：`memory/cluster_access.md`（私有记忆）
- 后续如果上 DataNode on master，新写一份 `docs/master-datanode-rollout.md`，不复用此文档
