# Master DataNode 上线记录（2026-05-31）

## 一、动机

31d 跑前评估：清旧 run 后 HDFS Remaining 178.58 GB，按 7d 实测 Stage1 输出 28.76 GB 外推到 31d（输入 4.13×，pair 量按 4–6× 外推），5× 时只剩 ~35 GB，6× 时撞墙（见 [space-optimization](space-optimization.md) §1.4）。两 worker × ~97 GB DN 容量已是天花板。

master 物理盘 `/` 41 GB 空闲 + `/home` 48 GB 几乎空，叠 NN/SNN/RM/JHS/NM 后内存 5.2 GB available、daemon RSS 合计 2.2 GB，**完全有余量再背一个 DN**。

**预期收益**：HDFS Configured 195 GB → ~282 GB（+45%），跑 31d Stage1 输出 5× 外推后仍剩 ~118 GB，6× 仍剩 ~87 GB——从"临界"到"宽裕"。

## 二、变更范围

仅改 **master** 的 `hdfs-site.xml`。worker1 / worker2 / yarn-site / mapred-site **均未动**。

变更前后 diff（master `/opt/module/hadoop-3.3.6/etc/hadoop/hdfs-site.xml`）：

```diff
     <property>
         <name>dfs.datanode.available-space-volume-choosing-policy.balanced-space-preference-fraction</name>
         <value>0.85</value>
     </property>
+    <!-- 2026-05-31: master 上 DN 后给 NN/RM 留 5GB 安全余量 (per-volume) -->
+    <property>
+        <name>dfs.datanode.du.reserved</name>
+        <value>5368709120</value>
+    </property>
 </configuration>
```

`dfs.datanode.data.dir` 已经预配双卷（`/opt/.../data/dfs/data,/home/hdfs/data`，沿用 worker 风格），无需再改。

## 三、容量预算依据

master 物理资源 4 vCPU / 8 GB RAM，已运行 NN + SNN + RM + JHS + NM（NM 见 [[docs/master-nodemanager-rollout]]）。

| 用途 | RAM | 备注 |
|---|---|---|
| OS + buff/cache | ~0.8 GB | — |
| NameNode | ~0.4 GB（实测 RSS） | — |
| SecondaryNameNode | ~0.4 GB | — |
| ResourceManager | ~0.5 GB | — |
| JobHistoryServer | ~0.6 GB | — |
| NodeManager 自身 | ~0.3 GB | 不含容器 |
| **守护进程合计（含 DN 前）** | **~2.2 GB（实测）** | NM 上线日实测 |
| **DataNode 自身** | **+~1 GB**（默认 heap） | — |
| **NM 容器预算** | **4 GB（已配）** | — |
| **总计** | **~7.2 GB** | 物理 7.6 GB 还留 0.4 GB / 2 GB swap 兜底 |

du.reserved=5 GB per volume：master 两块盘 ×5 GB = 10 GB 留给 NN 的 fsimage / edits log + OS / 容器临时文件。

|  | DN 上线前 | DN 上线后实测 |
|---|---|---|
| master Configured Capacity | — | 87.40 GB（41+48−2×5 ≈ 79 GB，比预算略高，因为实际 `/` 已用 9.2 GB 由 du.reserved 兜底） |
| 集群 Configured Capacity | 194.80 GB | **282.19 GB** |
| 集群 Present Capacity | 187.42 GB | **270.67 GB** |
| 集群 DFS Remaining | 178.58 GB | **261.83 GB** |

## 四、为什么没动其他参数

- **`dfs.replication=1` 不动**：master DN 与 worker DN 同等地位，rep=1 三副本分布到三 DN 反而比 rep=2 两 DN 更鲁棒（任一 DN 挂只丢该 DN 上的块，NN 还活，可重跑）
- **不加 rack awareness / topology**：3 节点小集群，单 rack 默认拓扑够用
- **不预先 hdfs balancer**：DN 上线后 HDFS 自动把 under-replicated blocks 复制到 master（实测 30 个块在 ~5 秒内补齐），存量数据均衡靠 AvailableSpace 自然漂移
- **worker 不加 du.reserved**：worker `/` 没有 NN 关键数据，5 GB reserve 浪费空间（per-volume ×2 ×2 worker = 20 GB）
- **`workers` 文件不加 master 的 DN**：保持 master DN 手动启停（`hdfs --daemon start/stop datanode`），与 NM 一致，避免 `start-dfs.sh` 误启停

## 五、部署执行步骤

```bash
# 1. 备份 hdfs-site.xml
ssh master 'cp /opt/module/hadoop-3.3.6/etc/hadoop/hdfs-site.xml \
  /opt/module/hadoop-3.3.6/etc/hadoop/hdfs-site.xml.bak.$(date +%Y%m%d-%H%M%S)'
# 实际备份：hdfs-site.xml.bak.20260531031817

# 2. 新建 DN 数据目录（master `/home/hdfs/` 不存在，需创建）
ssh master 'mkdir -p /opt/module/hadoop-3.3.6/data/dfs/data /home/hdfs/data'

# 3. 在 </configuration> 前插入 du.reserved property（见 §二 diff）
ssh master 'sed -i "/<\\/configuration>/i ..." /opt/module/hadoop-3.3.6/etc/hadoop/hdfs-site.xml'

# 4. 启动 DN（无需重启 NN）
ssh master 'hdfs --daemon start datanode'

# 5. 验证（等 ~5 秒让 DN 注册）
ssh master 'hdfs dfsadmin -report | grep -E "Live datanodes|Configured Capacity"'
# 期望：Live datanodes (3)，Configured Capacity ~282 GB
```

## 六、上线后基线

|  | DN 启动前 | DN 启动后 |
|---|---|---|
| Live datanodes | 2 | 3 |
| Configured Capacity | 194.80 GB | 282.19 GB（+87.39 GB） |
| DFS Remaining | 178.58 GB | 261.83 GB |
| master `/` | 19% used | 19%（DN 刚起，块复制还没量级） |
| Under replicated blocks | 30 | 0（DN 上线后自动补齐） |

NN/RM/JHS/SNN/NM **零退化**（沿用 NM 上线方法论：只动单一 daemon，daemon 间无重启依赖）。

## 七、回退步骤

```bash
ssh master 'hdfs --daemon stop datanode'
ssh master 'cp /opt/module/hadoop-3.3.6/etc/hadoop/hdfs-site.xml.bak.20260531031817 \
  /opt/module/hadoop-3.3.6/etc/hadoop/hdfs-site.xml'
# NN / RM / JHS / SNN / NM 不重启，不受影响
ssh master 'hdfs dfsadmin -report | grep "Live datanodes"'  # 应回到 2 节点

# 想彻底清块（释放磁盘）：先 decommission 再删数据目录
# 1) hdfs-site.xml 加 dfs.hosts.exclude 列入 master，NN refreshNodes，等 Decommissioned
# 2) rm -rf /opt/module/hadoop-3.3.6/data/dfs/data /home/hdfs/data
```

## 八、需观测的指标（31d 跑完后核对）

1. **master DN 实际写入分布**：跑后 `hdfs dfsadmin -report` 看三 DN DFS Used 是否均衡。AvailableSpace 选盘策略会让新写偏向 master（DFS 0% vs worker2 8%）
2. **NN RpcQueueTimeAvgTime / RpcProcessingTimeAvgTime**：作业期间不应 > 10ms（健康 < 1ms）。NN/DN 共置最大风险点
3. **master `/` 盘用量**：作业期间不应跨过 90%（DN 写 + NM 容器 log + NN 自身），du.reserved=5G 是底线兜底
4. **master 1min load 峰值**：不应超过 4.0（4 物理核），NN/RM RPC + DN 块 IO + NM 容器三路争资源
5. **Block report 延迟**：作业期间 NN UI（:9870）看每个 DN "Last Block Report" 应 ≤ 30 min

如果 NN RPC 队列 > 50 或 processing > 50ms，优先回退 DN；如果只是 load 偏高但 RPC 稳定，可容忍。

## 九、关联约定

- master `hdfs-site.xml` 与 worker 的差异：仅 `dfs.datanode.du.reserved`（master 加了 5 GB）。**理论上不会被同步脚本误覆盖**（master/worker hdfs-site 一直就有少量 host-specific 差异，沿用 NM 模式人工维护）
- master DN 与 master NM 共用一套"手动启停"约定：`hdfs --daemon start/stop datanode` / `yarn --daemon start/stop nodemanager`
- 文档同步更新：`memory/cluster_access.md`（私有记忆）需把 `dfs.replication=1, 195 GB` 改成 `282 GB / 3 DN`
- 关联文档：[master-nodemanager-rollout](master-nodemanager-rollout.md)（上一次单点扩容）、[space-optimization](space-optimization.md)（容量预算依据）、[7d-trouble-shooting](7d-trouble-shooting.md)（HDFS 容量撞墙复盘）
