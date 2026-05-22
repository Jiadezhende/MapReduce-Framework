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

## 附:尚未处理

- **日志聚合未开**(`yarn.log-aggregation-enable`):容器退出后无法在 Web UI 看 task 的 stderr/stdout。开启需改 `yarn-site.xml` 并同步到所有 worker + 重启 NodeManager(对共享集群有干扰),建议挑空闲窗口做。
- **时钟持久同步**未配置(见 §4 遗留)。
