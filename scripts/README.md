# 集群提交脚本使用指南（cluster_run / cluster_test）

本目录下的脚本采用 **本地驱动、非登录提交** 模式：开发机本地构建 stage jar，
通过 `scp` 推到 master，再用非交互 `ssh` 触发 `hadoop jar`。master 上不放任何
项目源码或脚本，只放每次运行临时目录下的 stage jar。

所有脚本均为 `bash`。**Linux 直接跑；Windows 用 Git Bash 跑**（体验与 Linux 完全一致）。

| 脚本 | 作用 |
|---|---|
| `cluster_run.sh` | 跑完整 1d/7d/31d 流水线（stage0→3） |
| `cluster_test.sh` | 单 stage 真集群隔离测试（对拍 golden 夹具） |
| `cluster_status.sh` | 查看某个 run 的 HDFS 产出树 |
| `cluster_fetch.sh` | 把某 run 的最终 TopN 结果 + metrics 拉到本地 |
| `cluster_cancel.sh` | 按 run_id 杀掉在跑的 YARN 应用 |
| `env.sh` | 集中环境变量 + 公共函数（被其余脚本 source） |

> **Windows 用户**：在 **Git Bash** 里运行，不要用 PowerShell/CMD 的原生 `ssh`
> （后者管道喂 stdin 会报 `getsockname failed: Not a socket`）。注意：Git/MSYS 的
> ssh 连接复用（ControlMaster）在本机**不可靠**，实测会静默退回逐条连接；集群按源 IP
> 限流的问题由 `env.sh` 的 `remote_ssh`/`remote_scp`（节流 + 失败重试）兜底，与是否复用无关。

---

## 一、前期配置（一次性）

### 1. 通用前提（两个系统都要）

| 项 | 说明 |
|---|---|
| **到 master 的 SSH 免密** | `~/.ssh/config` 里有 `master` 别名，且公钥已授权（见下） |
| **JDK 1.8** | 本地构建必须用 **Java 8**（`pom.xml` 锁定 `maven.compiler.source/target=1.8`），与集群字节码一致 |
| **Maven** | 本地 `mvn package` 出 shaded stage jar（`--build` 时调用） |
| **master 端 hadoop** | master 上有 `hadoop`/`yarn` 客户端，HDFS/YARN 可用 |

### 2. `~/.ssh/config`（master 别名 + 免密 + 连接复用）

脚本里所有集群操作都是 `ssh master`，所以认证/复用都在 ssh config 层做，
**所有脚本自动共享，无需改脚本**。推荐配置：

```sshconfig
Host master
  HostName 10.176.62.218
  User root
  IdentityFile ~/.ssh/id_cluster      # 专用、无 passphrase 的密钥，供脚本非交互登录
  IdentitiesOnly yes
  ControlMaster auto                  # 连接复用：把一次 run 收敛成一条连接以规避源 IP 限流。
  ControlPath ~/.ssh/cm-%C            #   仅在 Linux 上可靠；Windows/Git-MSYS 复用不稳，设 no 即可
  ControlPersist 5m                   #   （限流兜底已在 env.sh 的 remote_ssh/remote_scp 里，与复用无关）
  ServerAliveInterval 30              # 长任务保活
  ServerAliveCountMax 3
```

生成并授权专用密钥：

```bash
# 本地生成无密码专用钥
ssh-keygen -t ed25519 -f ~/.ssh/id_cluster -N "" -C "cluster-automation"

# 授权到 master（用你现有的免密/密码登录推一次公钥）
ssh-copy-id -i ~/.ssh/id_cluster.pub master
# 或不走 ssh-copy-id：
#   cat ~/.ssh/id_cluster.pub | ssh -o ControlPath=none master \
#     'umask 077; mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys'

# 验证
ssh master            # 应免密登上
```

> **为什么用专用无密码密钥？** 脚本是非交互的，没法在运行中输 passphrase；
> 带密码的主密钥只有在 ssh-agent 已解锁时才好用，跨 Git-ssh/原生-ssh 容易踩坑。
> 一把仅授权给集群的无密码 `id_cluster` 最省心，风险也隔离在集群内。
>
> **连接复用 / 限流提示**：master 所在子网会**按源 IP 限流**——一次 run 的准备阶段
> （mkdir + scp + 一串 HDFS 检查）若在几秒内密集发起十几条连接，整个 IP 会被 DROP 封禁
> 约 5 分钟（连 ICMP 都不通，非仅 22 端口）。`ControlMaster` 把一次 run 收敛成一条连接来
> 规避它，但**仅在 Linux 上可靠**；Windows 的 Git/MSYS ssh 复用不稳
> （`mux_client ... read from master failed` / `Software caused connection abort`，会静默退回
> 逐条连接），等于没复用。
>
> 因此真正的兜底在 `env.sh`，**两套系统通用**：幂等调用（mkdir/scp/HDFS 检查/删目录）走
> `remote_ssh`/`remote_scp`——**节流**（拉开连接间隔，默认 `SSH_THROTTLE_SECS=1.5` 秒）+
> **失败重试**（仅当 ssh 连接级失败 exit 255，即超时/重置/被封时等冷却重试；命令本身的
> 非零退出如 `hadoop fs -test -e` 照原样返回、**不**重试，保住断点续跑语义）。耗时长的
> `hadoop jar` 提交保持普通 `ssh`（间隔几分钟、不成 burst，且重试会重复提交 YARN 作业）。
> 万一仍被封，会自动等冷却续上，也可用同 `--run-id` 断点续跑。

### 3. Linux 特定

```bash
# 确保构建用 JDK 8
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64   # 按实际路径
chmod +x scripts/*.sh
```

### 4. Windows 特定（Git Bash）

- **用 Git Bash 跑**，不要用 PowerShell/CMD。Git Bash 的 `ssh`
  （`C:\Program Files\Git\usr\bin\ssh.exe`，OpenSSH 10）可正常非交互登录；原生
  `C:\Windows\System32\OpenSSH\ssh.exe` 管道喂 stdin 会报 `getsockname failed: Not a socket`。
  注意 Git/MSYS 的 ssh 虽接受 `ControlMaster` 选项，但复用实际不可靠（AF_UNIX 模拟问题，
  会静默退回逐条连接），所以本机建议设 `ControlMaster no`，限流靠 env.sh 的节流/重试兜底。
- **JDK 8**：`cluster_run.sh` 在构建/提交前会 `java -version` 校验，非 1.8 直接退出
  （exit 4）。把 `JAVA8_HOME` 指向 JDK 8，脚本会自动选用（Git Bash 里）：

  ```bash
  export JAVA8_HOME="/c/Program Files/Java/jdk1.8.0_xxx"
  # 或直接设 JAVA_HOME 到 JDK8 亦可
  ```

---

## 二、运行流程

### A. 完整流水线 `cluster_run`

```
准备输入 CSV 到 HDFS（首次/换数据时）  →  cluster_run  →  cluster_status 看产出树 / cluster_fetch 拉结果
                                         │
                                  中断/失败 → cluster_cancel，再用同 run_id 续跑
```

**Step 1 — 准备原始数据到 HDFS**（仅当 `--from stage0` 且 HDFS 上还没有 `<phase>.csv`）

原始 CSV 需放到 HDFS 路径：

```
${HDFS_INPUT_ROOT}/<phase>.csv     # 默认 /companion/input/raw/<phase>.csv，<phase> ∈ {1d,7d,31d}
```

本地机器无需 hadoop 客户端——先 `scp` 到 master，再在 master 上 `hadoop fs -put`：

```bash
ssh master "hadoop fs -mkdir -p /companion/input/raw"       # 建输入目录（一次即可）
scp data/7d.csv master:/tmp/7d.csv                          # 本地 CSV 推到 master
ssh master "hadoop fs -put -f /tmp/7d.csv /companion/input/raw/7d.csv"
ssh master "hadoop fs -ls -h /companion/input/raw"          # 核对
```

**Step 2 — 跑流水线**

```bash
# Linux 与 Windows/Git Bash 通用
scripts/cluster_run.sh --days 7 --build         # 构建 + 跑 stage0..stage3
scripts/cluster_run.sh --days 31 --from stage1 --until stage2   # 只跑某段
scripts/cluster_run.sh --days 1  --dry-run      # 只打印将执行的命令，不真跑
```

- 每个 stage 的输出带 `_SUCCESS` 标记即视为完成，**用同一 `--run-id` 重跑会自动跳过已完成的 stage**（断点续跑）。
- 不传 `--run-id` 时自动生成 `<用户>-<git短sha>-<时间戳>`；续跑/排障请记下它。
- 产出落在 HDFS `${HDFS_RUNS_ROOT}/<run_id>/`：`vid_freq/`、`filtered/`、`pair_loc_slot/`、`companions/`、`final/`。

**Step 3 — 查看/续跑/取消**

```bash
scripts/cluster_status.sh <run_id>                 # 列出该 run 的 HDFS 产出树
scripts/cluster_fetch.sh  <run_id> 7d              # 拉最终 TopN + metrics 到 ./out/<run_id>/
scripts/cluster_run.sh    --days 7 --run-id <run_id>   # 续跑（跳过已完成 stage）
scripts/cluster_cancel.sh <run_id>                 # 杀掉该 run 在跑的 YARN 应用（产出不动）
```

> `cluster_run.sh` 被 Ctrl-C 中断时，会自动按 run_id 取消本次的 YARN 应用，避免孤儿任务。

### B. 单 stage 隔离测试 `cluster_test`（仅 `.sh`，Windows 走 Git Bash）

在真 YARN/HDFS 上跑某一个 stage，把集群输出与本地 golden 夹具对拍，
专门抓 LocalJobRunner 抓不到的问题（多 reducer 分区、jar 打包/classpath、HDFS committer）。
所有产物落在 `${HDFS_TEST_ROOT_BASE}/<stage>-<时间戳>/`，运行结束自动清理。

```bash
# Linux / Git Bash 通用
scripts/cluster_test.sh --stage stage1 --build           # 构建 + 测 stage1
scripts/cluster_test.sh --stage stage2 --reducers 4      # 用 4 个 reducer 验分区
scripts/cluster_test.sh --stage stage0 --keep            # 保留 HDFS/远端临时目录便于排查
scripts/cluster_test.sh --stage stage3 --dry-run         # 只打印计划
```

- 退出码：`PASS` → 0，`FAIL` → 1，参数错 → 2。
- stage0 用「超集 + 偏差上限」断言（生产 Stage0 的 BloomFilter 会放进有界的 FP 多余行）；其余 stage 为精确对拍。

---

## 三、可配置参数

### 1. 环境变量（`env.sh`，可在调用前 `export` 覆盖）

| 变量 | 默认 | 含义 |
|---|---|---|
| `MASTER_HOST` | `master` | 提交目标主机的 ssh 别名 |
| `HADOOP_BIN` | `hadoop` | master 上的 hadoop 可执行（可填绝对路径） |
| `HADOOP_CONF_DIR` | `/etc/hadoop/conf` | master 端 hadoop 配置目录 |
| `LOCAL_DATA_DIR` | 仓库根 | 本地项目根（找 `pom.xml` / `*/target/*.jar`） |
| `COMPANION_ROOT` | `/companion` | HDFS 上的项目根 |
| `HDFS_INPUT_ROOT` | `${COMPANION_ROOT}/input/raw` | 原始 CSV 输入根 |
| `HDFS_RUNS_ROOT` | `${COMPANION_ROOT}/runs` | 每次 run 的产出根（`/<run_id>/...`） |
| `HDFS_TEST_ROOT_BASE` | `${COMPANION_ROOT}/test` | `cluster_test` 隔离根 |
| `REMOTE_SUBMIT_BASE` | `/tmp/companion/submit` | master 本地暂存 jar 的目录 |
| `YARN_QUEUE` | `default` | YARN 队列 |
| `REDUCERS_1D` / `_7D` / `_31D` | `8` / `32` / `32` | 各 phase 的统一 reducer 数（驱动 stage0a/1/2/3） |
| `TUNE_1D` / `_7D` / `_31D` | 空 / 空 / 容器+排序调优串 | 各 phase 的额外 `-D`（31d 默认收紧容器内存、增大 map 排序缓冲） |
| `JAVA8_HOME` | 未设 | 指向 JDK 8；设了则 `cluster_run.sh` 自动用它构建并校验 1.8 |
| `SSH_THROTTLE_SECS` | `1.5` | `remote_ssh`/`remote_scp` 每条连接前的节流间隔（秒），降低触发源 IP 限流的概率；网络敏感可调大 |
| `SSH_MAX_RETRIES` | `6` | ssh/scp 连接级失败（exit 255）的最大重试次数 |
| `SSH_BAN_WAIT_SECS` | `120` | 每次重试前等待秒数；`6×120≈12min` 足以越过 ~5min 封禁冷却 |

### 2. `cluster_run` 参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--days {1\|7\|31}` | **必填** | 选择数据规模 phase（决定 reducer 数与调优串） |
| `--build` | 关 | 提交前先本地 `mvn -DskipTests package` |
| `--from <stage>` | `stage0` | 起始 stage |
| `--until <stage>` | `stage3` | 结束 stage |
| `--stage <stage>` | — | 只跑单个 stage（等价 `--from X --until X`） |
| `--run-id <id>` | 自动生成 | 指定 run_id（**续跑/排障关键**，跳过已完成 stage） |
| `--force` | 关 | 忽略 `_SUCCESS`，强制重跑窗口内所有 stage |
| `--dry-run` | 关 | 只打印将执行的命令，不真正跑（此时 Java 非 1.8 仅告警不退出） |
| `-Dkey=value …` | — | 透传给 `hadoop jar` 的额外配置（见下） |
| `-h` / `--help` | — | 帮助 |

### 3. `cluster_test` 参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--stage {stage0\|stage1\|stage2\|stage3}` | **必填** | 要测试的单个 stage |
| `--build` | 关 | 先本地构建 |
| `--reducers N` | `2` | 测试用 reducer 数（>1 以暴露分区问题） |
| `--keep` | 关 | 保留 HDFS 隔离目录与远端临时目录，便于排查 |
| `--dry-run` | 关 | 只打印计划，跳过提交与对拍 |
| `-Dkey=value …` | — | 透传额外配置 |
| `-h` / `--help` | — | 帮助 |

### 4. 常用 `-D` 透传项（追加到 `hadoop jar` 的 GenericOptions）

| 配置 | 用途 |
|---|---|
| `-Dcompanion.stageNa.reducers=N` | 单独覆盖某 stage 的 reducer 数（默认由 phase 统一给定） |
| `-Dmapreduce.map.memory.mb` / `-Dmapreduce.reduce.memory.mb` | 容器内存 |
| `-Dmapreduce.map.java.opts=-XmxNm` / `-Dmapreduce.reduce.java.opts` | JVM 堆 |
| `-Dmapreduce.task.io.sort.mb` | map 端排序缓冲（缓解溢写/磁盘压力） |
| `-Dcompanion.hll.threshold=N` | HLL 基数估计阈值（大数据量调优） |

> 注意 Hadoop `GenericOptionsParser` 的 `stopAtNonOption=true`：**`-D` 必须放在位置参数之前**。
> 脚本内部已自动把所有 `-D` 选项排到位置参数前，你只管在命令行随手加即可。

---

## 四、典型完整示例

```bash
# ---- Linux：从零跑一遍 7d ----
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
# 原始 CSV 已在 HDFS（首次见 Step 1：scp + hadoop fs -put）
scripts/cluster_run.sh --days 7 --build
#   ...记下输出里的 run_id，比如 alice-abc1234-20260531120000
scripts/cluster_status.sh alice-abc1234-20260531120000
scripts/cluster_fetch.sh  alice-abc1234-20260531120000 7d   # → ./out/alice-abc1234-.../top_n.csv + _metrics.json
```

```bash
# ---- Windows / Git Bash：从零跑一遍 7d ----
export JAVA8_HOME="/c/Program Files/Java/jdk1.8.0_xxx"
# 原始 CSV 已在 HDFS（首次见 Step 1：scp + hadoop fs -put）
scripts/cluster_run.sh --days 7 --build
scripts/cluster_run.sh --days 7 --run-id <上一步打印的run_id>   # 断点续跑
```
