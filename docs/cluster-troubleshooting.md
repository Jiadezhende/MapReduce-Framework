# 集群配置与运行问题修复记录

本文档记录在真实 YARN/HDFS 集群上跑 `scripts/cluster_test.sh` 时遇到的环境类问题及其修复。这些都是**集群环境**问题,不是项目代码 bug——但会让作业失败或异常缓慢,排查时优先对照本文档。

集群基本信息:Hadoop 3.3.6,`HADOOP_HOME=/opt/module/hadoop-3.3.6`;节点 `master`=10.176.62.218、`worker1`=10.176.62.219、`worker2`=10.176.62.220;所有提交经 `ssh master hadoop jar`。

---

## §1 作业一提交就失败:`Could not find or load main class ...MRAppMaster`

**现象**:`hadoop jar` 提交成功、拿到 application id,但 AM 容器启动即失败,日志:
```
Error: Could not find or load main class org.apache.hadoop.mapreduce.v2.app.MRAppMaster
```

**根因**:YARN 容器启动时环境里没有 `HADOOP_MAPRED_HOME`,找不到 MapReduce 的 jar。集群的 `mapred-site.xml` 缺少对应的 `*.env` 配置。

**修复**:在 **master** 的 `/opt/module/hadoop-3.3.6/etc/hadoop/mapred-site.xml` 增加(改前备份为 `.bak.<时间戳>`):
```xml
<property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=/opt/module/hadoop-3.3.6</value></property>
<property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=/opt/module/hadoop-3.3.6</value></property>
<property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=/opt/module/hadoop-3.3.6</value></property>
```

**说明**:这三项是**提交时(client 端)**的 job 属性,会被序列化进 `job.xml` 随作业下发。因为本项目只从 master 提交,所以**只需改 master**,且**无需重启任何守护进程**,下次提交即生效。

---

## §2 作业状态页打不开(Tracking UI 无画面)

**现象**:`http://master:8088/cluster` 能开,但点进某个作业的 Tracking UI 没画面;已结束的作业链接报 `ERR_EMPTY_RESPONSE`(浏览器显示 "workerN 未发送任何数据")。

**根因**(两层):
1. **服务端**:JobHistory Server 没启动,19888 端口无监听。作业**结束后** AM 退出,RM 会把链接重定向到 JobHistory Server,没起就打不开。运行中的作业 Tracking UI 指向 AM 在某 worker 上的临时端口,作业一结束端口即关,所以点进去是空响应。
2. **客户端**:见 §3,本机浏览器解析不了 `master` / `workerN` 主机名。

**修复(服务端)**:在 master 的 `mapred-site.xml` 增加并启动 JHS:
```xml
<property><name>mapreduce.jobhistory.address</name><value>master:10020</value></property>
<property><name>mapreduce.jobhistory.webapp.address</name><value>master:19888</value></property>
```
```bash
ssh master 'mapred --daemon start historyserver'
```
> ⚠️ `webapp.address` 必须显式写成 `master:19888`;留默认 `0.0.0.0:19888` 时 RM 会把浏览器重定向到 `http://0.0.0.0:19888/...`,照样打不开。

**用法**:作业跑完后在列表里点 **History**(不是 ApplicationMaster)→ 跳 `http://master:19888`;或直接开 `http://master:19888/jobhistory`。

**遗留**:JHS 是手动启动的,**集群重启后不会自动拉起**,需重新执行上面的 `mapred --daemon start historyserver`。

---

## §3 浏览器用 `http://master:8088` 打不开,但 IP 可以

**现象**:`http://10.176.62.218:8088/cluster` 正常,`http://master:8088/cluster` 打不开。

**根因**:`ssh master` 能通是因为本机 `~/.ssh/config` 里的 `Host master` 别名,**该别名只有 ssh 用**;浏览器走系统 DNS / `/etc/hosts`,而本机 `/etc/hosts` 没有 master 映射。

**修复**:在**本机(Mac)**的 `/etc/hosts` 加(三条都要,worker 用于看 container 日志/AM 页):
```
10.176.62.218 master
10.176.62.219 worker1
10.176.62.220 worker2
```
```bash
echo '10.176.62.218 master
10.176.62.219 worker1
10.176.62.220 worker2' | sudo tee -a /etc/hosts
```

---

## §4 数据量极小(43KB)但作业跑五分钟——时钟不同步

**现象**:stage1 测试输入仅 43KB、1 个 split,却要 ~5 分钟;且 JobHistory 的分阶段耗时离谱:`avgShuffleTime` 高达 ~200 秒,`avgReduceTime` 甚至为**负数**,两次运行耗时还忽快忽慢。

**排查**:先后排除了——资源(两台各 8GB/8 核,基本空闲)、worker 间主机名解析(`/etc/hosts` 完整、互相可解析)、防火墙(ShuffleHandler 在 `*:13562` 正常监听、firewalld 未启用)、ssh 免密(与运行期数据传输无关)。

**根因**:**三节点时钟严重漂移**,且 NTP 全部未同步:
```
master   2026-05-22 22:58   ← 正确
worker1  2026-05-22 06:41   ← 慢 ~16 小时
worker2  2026-05-22 08:58   ← 慢 ~14 小时
```
MapReduce shuffle 的重试/惩罚(penalty box)逻辑基于时间戳计算"下次重试时间";跨节点时钟差十几小时时,reducer 会**空等数分钟**才去拉那点数据。负的 `avgReduceTime` 正是跨节点时钟错乱的直接证据。

> **关键点:这个 bug 只要求集群内部时间一致,不要求等于真实时间。** shuffle 算的是节点间时间戳的**差值**,三台互相对齐即可——哪怕统一慢 16 小时,差值仍然正确,作业照样正常跑。所以修复目标是"统一",不是"准确"。
>
> 但实际仍建议对到真实时间,原因是另一些功能依赖**绝对时间**:Kerberos(与 KDC 误差需 <5 分钟)、TLS 证书有效期、HDFS mtime/trash 过期、定时任务,以及日志/JobHistory 时间戳要能和外部对齐。而且 NTP 本就同步到真实时间,节点重启/新增也会自然回到真实时间——刻意维持"一致但错"反而更难、更脆。本次让 worker 对齐到 master,而 master 本身就准(与本机一致),于是"一致"和"准确"一次到手,这也是选 master 当基准的原因。

**修复**(集群无 chrony/ntp,内网无源,故直接对时):
```bash
# 在 master 上把两台 worker 对齐到 master,并写入硬件时钟(重启不丢)
ssh master 'for w in worker1 worker2; do
  ssh $w "date -s @$(date +%s) >/dev/null && hwclock --systohc"
done'
```

**验证**:对时后同一 stage1 测试端到端从 ~5 分钟降到 **~47 秒**,`Shuffle Errors: CONNECTION=0`,结果 `PASS`。

**遗留 / 重要**:**没有持续同步守护进程,时钟以后还会漂(VM 尤其明显)**。`hwclock --systohc` 只保证重启不丢,日常仍会逐渐拉开。
- 临时:再执行一次上面的对时命令。
- 持久(轻量,无需联网):在 master 加 root cron 定期推时间,例如
  `*/13 * * * * for w in worker1 worker2; do ssh $w "date -s @$(date +%s)" >/dev/null 2>&1; done`
- 持久(正规):装 chrony/ntp,worker 指向 master 当时间源。

> 经验:**真实集群上"小作业跑了好几分钟",第一件事是 `date` 对一遍三台节点的时钟。**

---

## §5 本机 dry-run 报错 `declare: -A: invalid option`——macOS 自带 bash 3.2

**现象**:在本机(Mac)跑 `scripts/cluster_run.sh --days 1 --dry-run` 直接失败:
```
scripts/cluster_run.sh: line 139: declare: -A: invalid option
```
同一脚本在 WSL / Linux 上正常。

**根因**:`declare -A`(关联数组)是 **bash 4.0+** 才有的特性,而 **macOS 自带的 `/bin/bash` 永远停在 3.2.57**(Apple 因 bash 4 改用 GPLv3 而不再升级)。脚本首行虽是 `#!/usr/bin/env bash`,但本机 PATH 里的 `bash` 就是这只 3.2,于是 `declare -A` 当场报错。这是**改动前就存在的依赖**,不是新引入的 bug;WSL 的 bash 是 4+,所以一直没暴露。

**修复**:`scripts/cluster_run.sh` 里 `declare -A MODULE_JAR` 是全 `scripts/` 目录**唯一**的 bash-4 写法,且该关联数组只在紧随其后的 scp 循环里被读一次。于是删掉关联数组,把"模块→jar 路径"的解析直接内联进 scp 循环按需求值:
```bash
for module in "${NEEDED_MODULES[@]}"; do
    if [[ "${DRY_RUN}" == "true" ]] && ! ls .../target/${module}-*.jar >/dev/null 2>&1; then
        local_jar=".../target/${module}-<version>.jar"
    else
        local_jar=$(companion_jar "${module}")
    fi
    run scp "${local_jar}" "${MASTER_HOST}:${REMOTE_JAR_DIR}/${module}.jar"
done
```
行为与原来完全一致,只是不再需要关联数组。其余写法(`declare -a` 索引数组、`[[ ]]`、`(( ))`、`${arr[@]}`)在 bash 3.2 与 4+ 都支持。

**验证**:`/bin/bash -n`(语法)通过;`/bin/bash scripts/cluster_run.sh --days 1 --dry-run` 在本机 3.2.57 上完整跑出 stage0→stage3 的提交计划。WSL 的 bash 4+ 是超集,继续可用。

> 经验:**写集群脚本要兼顾 macOS,就别用 bash-4-only 特性**(`declare -A`、`${var,,}`/`${var^^}`、`mapfile`/`readarray`、`wait -n`、`&>>` 等)。排查这类"换台机器就崩"的脚本问题,第一步先 `bash --version` 看清本机到底是哪只 bash。

---

## §6 cluster_run 杀不掉 stage1 任务,且 1d 数据 Stage 1 跑出小时级——`-D` 选项被静默丢弃

**现象**(两个表象,同一根因):
1. Ctrl-C 中断 `cluster_run.sh` 后,Stage 1 的 YARN app 没被 kill,留在集群里继续跑。`on_interrupt` 打印的是 `no running YARN apps for run_id=...`,但 YARN UI 里那个 `Stage1Job j1a` 还在。
2. 1d 数据(~9M 条)Stage 1 跑了几十分钟到小时级,远超预期(应该是分钟级)。

**排查**:用 `mapred job -status` 抓那个还在跑的 Stage1 看到关键证据:
```
Number of maps: 2
Number of reduces: 1     ← 配置是 STAGE1_REDUCERS_1D=8,实际只有 1
```
再从 staging 目录抓 `job.xml`:
```bash
ssh master 'hadoop fs -cat /tmp/hadoop-yarn/staging/root/.staging/job_<id>/job.xml \
    | grep -E "mapreduce\.job\.reduces|companion\.run\.tag"'
# 输出:
# mapreduce.job.reduces = 1   source=programmatically   ← -D 没生效,退化到 Hadoop 默认
# companion.run.tag             ← 整个 key 不在 job.xml 里
```

**根因**:`cluster_run.sh` 的 `submit()` 把 `-D` 拼在**位置参数 `<in> <out>` 之后**:
```bash
# 改前
local cmd="${HADOOP_BIN} jar ... ${class} $* -D companion.run.tag=${RUN_ID}"
# 各 stage 调用方也是位置参数在前、-D 在后,比如 stage1:
submit stage1 companion.stage1.Stage1Job "<in>" "<out>" "-D mapreduce.job.reduces=${S1_RED}"
```
Hadoop 的 `GenericOptionsParser` 用 Commons CLI 解析,**`stopAtNonOption=true`**——一旦遇到非 option 的位置参数(`<in>`),后面所有 `-D` 都被当成位置参数**静默丢弃**。一个 bug 同时解释了两个表象:
- `-D mapreduce.job.reduces=8` 被丢 → reducer 数退化到 mapred-default.xml 的默认值 **1** → Stage 1 整个窗口配对串行化到单 reducer → 极慢。
- `-D companion.run.tag=<run_id>` 被丢 → `tag` 在 Java 端是空串 → `Stage1Job.java:129` 不再追加 `[<run_id>]` → app 名字只是 `Stage1Job j1a` → `cancel_run` 的 `grep -F "[run_id]"` 命中不到 → Ctrl-C 杀不掉。

**为什么 Stage 0 看起来没事**:`Stage0bFilterJob.java:30` 早就遇到过同一个坑,自己手写了绕过:
```java
@Override
public int run(String[] args) throws Exception {
    Stage0Bloom.applyTrailingDefines(getConf(), args);  // 从 args[2:] 扫尾部 -D 塞回 conf
    return super.run(args);
}
```
所以 Stage 0b 的 `-D companion.vid_freq.path=...` 被局部补回。**Stage 1/2/3 没这个 workaround,踩坑**。Stage 0a 调用方没传任何业务 -D,只丢了 run.tag,看不出问题。

**修复**:`scripts/cluster_run.sh` 的 `submit()` 改成把 caller 传进来的 args 拆成 `-D` 和位置参数两组,**`-D` 始终拼在位置参数之前**,调用方签名不动:
```bash
submit() {
    local module="$1"; shift
    local class="$1"; shift
    local d_opts="-D companion.run.tag=${RUN_ID}"
    local positional=()
    for arg in "$@"; do
        if [[ "${arg}" == -D* ]]; then
            d_opts="${d_opts} ${arg}"
        else
            positional+=("${arg}")
        fi
    done
    if (( ${#EXTRA_CONF[@]} > 0 )); then
        d_opts="${d_opts} ${EXTRA_CONF[*]}"
    fi
    local cmd="${HADOOP_BIN} jar ${REMOTE_JAR_DIR}/${module}.jar ${class} ${d_opts} ${positional[*]}"
    run ssh "${MASTER_HOST}" "${cmd}"
}
```

**验证**:`scripts/cluster_run.sh --days 1 --dry-run` 输出的 stage1 命令变成:
```
hadoop jar .../stage1.jar Stage1Job \
    -D companion.run.tag=<run_id> -D mapreduce.job.reduces=8 \
    <in> <out>
```
重跑后 `mapred job -status` 显示 `Number of reduces: 8`,YARN app 名字带 `[<run_id>]` 后缀,Ctrl-C 触发的 `cancel_run` 能正确 kill。

**收尾遗留的孤儿任务**:tag 缺失的旧任务 `cancel_run` 杀不掉,手动按 app id 杀:
```bash
ssh master 'yarn application -appStates RUNNING,ACCEPTED -list' | grep Stage1
ssh master 'yarn application -kill application_xxxxx_xxxx'
```

**遗留的次生反模式**(待修,不影响当前流水线):`Stage1Job.java:65` 与 `Stage2Job.java:57` 用了
```java
job.setNumReduceTasks(conf.getInt(MRJobConfig.NUM_REDUCES, CompanionConf.stage1Reducers(conf)));
```
`mapreduce.job.reduces` 在 `mapred-default.xml` 永远有默认值 1,`conf.getInt` 永远返回 1 而不是 fallback 的 `companion.stage1.reducers`。脚本侧修好后,因为 `-D mapreduce.job.reduces=N` 能传入并覆盖默认 1 而被掩盖;但**任何人直接 `hadoop jar stage1.jar` 不带 -D 仍会拿到 1 reducer**。`cluster_test.sh` 的多 reducer 验证因此也是假阴性(单测 fixture 太小,1 reducer 也能 PASS golden diff)。建议改成 `setNumReduceTasks(CompanionConf.stage1Reducers(conf))`,把 `companion.stage1.reducers` 作为唯一旋钮。Stage 0b 的 `applyTrailingDefines` 在脚本修好后变冗余但无害,可作为兜底保留。

> 经验:**Hadoop 的 `-D` 必须放在位置参数之前**。`GenericOptionsParser` 的 `stopAtNonOption=true` 不会报错也不会警告,只会让 conf "差一点点",表现成"作业能跑但参数不生效",最难排查。看到 `job.xml` 里关键 key `source=programmatically` 或干脆缺失,第一反应就是 -D 顺序。

---

## 附:尚未处理

- **日志聚合未开**(`yarn.log-aggregation-enable`):容器退出后无法在 Web UI 看 task 的 stderr/stdout。开启需改 `yarn-site.xml` 并同步到所有 worker + 重启 NodeManager(对共享集群有干扰),建议挑空闲窗口做。
- **时钟持久同步**未配置(见 §4 遗留)。
