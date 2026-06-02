# 单机（单节点）Hadoop 部署指南

把原本跑在 **3 节点集群**（master/.218 + worker1/.219 + worker2/.220，各 4c8G）的伴随车管线，
迁到 **一台高性能服务器**（32c / 64G / ~500G 可用磁盘）上：这台机器**独自承载所有 Hadoop 角色**
（NameNode + SecondaryNameNode + DataNode + ResourceManager + NodeManager + JobHistoryServer）。

> **关键认知**：Java（4 个 stage + common）是标准 `org.apache.hadoop.mapreduce.*`，**不含任何硬编码集群地址**，
> 全靠 `$HADOOP_CONF_DIR` 连接 → 单节点上原样可跑，**没有一行 Java 改动**。提交脚本只认 `MASTER_HOST`，
> **脚本逻辑零改动**。本次迁移＝**重标定 `env.sh` 旋钮 + 一套单节点 Hadoop 配置 + bring-up**，启动方式（`scripts/cluster_run.sh` + `ssh` + SCP）完全不变。

---

## 1. 拓扑对照

| | 旧（3 节点） | 新（单节点） |
|---|---|---|
| 提交目标 | `ssh master` | `ssh master`（别名指向新服务器） |
| 角色分布 | master=NN/RM/JHS，3 机各 NM/DN | 一台机器全包 |
| 副本 | rep=1（2 worker rep=2 收益≈0） | rep=1（强制，单 DataNode） |
| shuffle 落盘 | 摊到 3 个 nm-local-dir | **全压到一块 ~500G 盘** ← 唯一真难点 |

迁移只动两类东西：**提交侧旋钮** [scripts/env.sh](../scripts/env.sh) 与 **服务器侧 Hadoop 配置** [deploy/single-node/](../deploy/single-node/)。

---

## 2. 服务器侧 bring-up

### 2.1 前置
- Hadoop **3.3.6**（与 `pom.xml` 一致）已解压到 `$HADOOP_HOME`，例如 `/opt/module/hadoop-3.3.6`。
- **JDK 1.8**（项目锁定 1.8）。
- `ssh localhost` 免密（`start-dfs.sh`/`start-yarn.sh` 需要）。
  ```bash
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys
  ssh localhost true   # 应无需输入密码
  ```
- 一块 ~500G 可用卷，挑一个目录做 `DATA_VOL`，例如 `/data/hadoop`。

### 2.2 一键 bring-up
把仓库的 `deploy/single-node/` 拷到服务器（或直接在服务器 clone 仓库），然后:

**推荐：双盘**（HDFS 与 shuffle 分盘，见 §5——实测里这是"最该做的结构改动"）：

```bash
HADOOP_HOME=/opt/module/hadoop-3.3.6 \
JAVA8_HOME=/opt/module/jdk1.8.0_xxx \
HDFS_VOL=/data/hdfs SHUFFLE_VOL=/data/shuffle \
SERVER_HOST=$(hostname) \
bash deploy/single-node/bootstrap.sh
```

单盘（HDFS+shuffle 共用一块盘）：把上面两行换成 `DATA_VOL=/data/hadoop`，两个卷自动指向它。

[bootstrap.sh](../deploy/single-node/bootstrap.sh) 幂等地完成：渲染 4 个 `*-site.xml` + `workers` 到
`$HADOOP_HOME/etc/hadoop/`、追加 daemon 堆 + `HADOOP_LOG_DIR` 到 `hadoop-env.sh`、首次格式化 NN、
启动全部 daemon、建好 `/companion` HDFS 布局，最后打印 `jps` 与 `hdfs dfsadmin -report` 供核对。

**期望结果**：`jps` 见到 `NameNode / SecondaryNameNode / DataNode / ResourceManager / NodeManager / JobHistoryServer` 六个进程；
`dfsadmin -report` 显示 **1 个 Live datanode**、Configured Capacity ≈ HDFS 盘容量。

### 2.3 配置取值依据（32c/64G/500G）

| 文件 | 关键项 | 值 | 理由 |
|---|---|---|---|
| [hdfs-site.xml](../deploy/single-node/hdfs-site.xml) | `dfs.replication` | 1 | 单 DataNode 强制 |
| | `dfs.datanode.du.reserved` | 20G | 防 Stage1/2 把盘写到 100% 卡死 DN |
| [yarn-site.xml](../deploy/single-node/yarn-site.xml) | `nodemanager.resource.memory-mb` | 49152 (48G) | 64G 留 ~16G 给 OS+page cache+各 daemon |
| | `nodemanager.resource.cpu-vcores` | 28 | 32 核留 4 给 daemon/OS；**vcore 是并发上限** |
| | `max-disk-utilization...percentage` | 95 | 紧盘留安全阈，靠 STAGE2_ROUNDS 压峰值而非提阈值 |
| [hadoop-env.sh.snippet](../deploy/single-node/hadoop-env.sh.snippet) | 各 daemon 堆 | NN4/SNN2/DN2/RM2/NM2/JHS1 G | 合计 ~13G，落在预留 16G 内 |
| | `HADOOP_LOG_DIR` | `__HDFS_VOL__/logs` | **40G 系统盘必须**：daemon 日志默认落 `$HADOOP_HOME/logs`(系统盘)，长跑会写满 `/`，改到数据盘 |

---

## 3. 上传输入数据

CSV 较大，人工搬（不进脚本）：

```bash
# 在服务器上（或从能直连的机器）：
scp data/1d.csv  <server>:/tmp/
ssh <server> 'HADOOP_HOME=/opt/module/hadoop-3.3.6; \
  $HADOOP_HOME/bin/hdfs dfs -put -f /tmp/1d.csv /companion/input/raw/1d.csv && rm -f /tmp/1d.csv'
# 7d / 31d 同理。31d=5.8G，确认盘有空间后再传。
```

校验：`ssh <server> 'hdfs dfs -du -h /companion/input/raw'`。

---

## 4. 从提交侧（笔记本 / git-bash）跑作业

启动方式与集群时代**完全一致**，只需把 `master` 别名指向新服务器：

```sshconfig
# ~/.ssh/config
Host master
  HostName <新服务器 IP>
  User <user>
  IdentityFile ~/.ssh/id_cluster
  IdentitiesOnly yes
```

新服务器若**不做源 IP 限流**，可关掉节流加速 jar 上传（包装器仍在，流程不变）：
```bash
export SSH_THROTTLE_SECS=0.2
```

跑通：
```bash
./scripts/cluster_run.sh --days 1 --build          # 先 1d 跑通全链路
./scripts/cluster_fetch.sh <run_id> 1d             # 取回 top_n.csv + _metrics.json
```

正确性：与 `baseline/` 参考实现或 `tests/` 金标准（`scripts/regenerate_fixtures.sh`）比对 top_n。

逐级放大：
```bash
./scripts/cluster_run.sh --days 7  --build
./scripts/cluster_run.sh --days 31 --build
```

---

## 5. 单机磁盘是成败关键（31d）

> 本节数字来自**实测复盘** [docs/runs/31d-cf1f2f6-k3gzip/report.md](runs/31d-cf1f2f6-k3gzip/report.md)，不是估算。

**两个轴，只有一个是瓶颈：**

- **HDFS 容量轴 — 不是瓶颈。** 31d 整个 `/companion` 实测仅 **~118G**（`pair_loc_slot` 103.5G 占 98%，
  raw 5.7G + filtered 1.8G + vid_freq 0.44G + companions ~13G）。所以 **HDFS 盘给 ~200G 绰绰有余**。
- **本地盘 shuffle 轴 — 唯一瓶颈。** 单遍 Stage2 map 输出 ~319G（gzip），且**驻留到该轮 `_SUCCESS` 才整块释放**（reduce 中途不回落，与 `io.sort.mb`/`slowstart` 无关）。单机一台扛全部。

单机 shuffle 峰值的实测公式（`N=1` 即单节点，`×1.3` 含倾斜 + reduce scratch）：

```text
单机 shuffle 峰值 ≈ 320 / (N=1 × K) × 1.3
```

| `STAGE2_ROUNDS_31D` | shuffle 峰值 | 300G shuffle 盘水位 | 输入重扫代价 |
|---|---|---|---|
| 2 | ~208G | 69% | 2×103.5G，最快 |
| **3（默认）** | **~139G** | **46%（稳）** | 3×103.5G，+~1h |
| 4 | ~104G | 35% | 4×103.5G，+~2h |
| 6 | ~69G | 23% | **6×103.5G，wall-time≈翻倍，纯浪费** |

**关键结论(report §5)：K 不是越大越安全——每轮都全量重扫 `pair_loc_slot`(103.5G),K=6 比 K=3 慢近一倍却毫无收益。**
在专用 ~300G shuffle 盘上 **K=3 水位仅 46%、稳如老狗**;想更快可降到 K=2(69%,少扫一遍)。已在 [env.sh](../scripts/env.sh) 设 `31D=3 / 7D=1 / 1D=1`。

**控制手段（已标定）：**
- **`STAGE2_ROUNDS_31D=3`**：核心杠杆,把单轮 shuffle 压到 ~139G。
- **gzip shuffle codec**（`TUNE_31D` 内）：比默认 Snappy 密 ~1.6×,直接把 map 输出从 ~500G 压到 ~319G。
- **双盘**：shuffle 与 HDFS 分盘,shuffle 突发不威胁 HDFS 提交(report 称"最该做的结构改动")。
- `io.sort.mb=512` + 大容器只省 spill 重写次数,**不影响 shuffle 驻留峰值**——别指望它救盘。

### 推荐磁盘分配（总 500G）

| 盘 | 容量 | 选型 | 承载 | 水位(31d, K=3) |
|---|---|---|---|---|
| 系统盘 | 40G | ESSD PL0 | OS + `$HADOOP_HOME` + JDK(无 Hadoop 数据) | — |
| HDFS 盘 | **200G** | ESSD PL1 | `dfs.*.dir` + tmp + daemon logs | ~118G → 59% |
| shuffle 盘 | **300G** | ESSD PL1 | `nm-local-dirs`(Stage2 spill) | ~139G → 46% |

### 跑 31d 时盯盘

```bash
ssh master 'df -h /data/shuffle /data/hdfs; hdfs dfsadmin -report | grep -E "DFS Used|DFS Remaining"'
```

- 若 shuffle 盘逼近 95%：调高 `STAGE2_ROUNDS_31D` 后重跑——`cluster_run.sh` 的 `_SUCCESS` 续跑机制只重做未完成的子轮。
- **单盘**情形下若热卷顶到崩线,实测唯一在线救援是 `hdfs diskbalancer`(节点内卷间挪 DN block,不丢数据、不抢 job I/O);跨节点 `hdfs balancer` 在单机无意义。双盘从根上免除这种半夜盯盘。

---

## 6. 运维速查

| 操作 | 命令 |
|---|---|
| 查看进度/产出树 | `./scripts/cluster_status.sh <run_id>` |
| 取回结果 | `./scripts/cluster_fetch.sh <run_id> <phase>` |
| 取消 run（杀 YARN app） | `./scripts/cluster_cancel.sh <run_id>` |
| 重启全部 daemon | 服务器上 `stop-yarn.sh; stop-dfs.sh; bash deploy/single-node/bootstrap.sh` |
| 看磁盘水位 | `ssh master 'df -h <DATA_VOL>'` |

---

## 7. 本次迁移未改动项（显式声明）

- **所有 Java**（stage0/1/2/3 + common）：零改动。
- `cluster_run.sh` / `cluster_fetch.sh` / `cluster_status.sh` / `cluster_cancel.sh` / `fetch_dataset.sh`：零改动（纯 `MASTER_HOST` 驱动）。
- MR 流程：Stage2 sharding、skew cap、HLL、补偿 pass 全保留，只调旋钮。
- `companion-conf.xml`：不改（`dfs.replication` 归 hdfs-site.xml；其余被 `-D` 覆盖）。
