HDFS 盘很健康,问题只在 shuffle 盘。两者是完全独立的物理盘:

挂载	设备	容量	已用	用途	状态
/data/hdfs	/dev/vdb	196G	116G / 63%	HDFS DataNode 数据	✅ 余 70G
/data/shuffle	/dev/vdc	196G	180G / 97%	YARN nm-local shuffle	❌ 爆盘
/	/dev/vda1	39G	14G / 35%	OS 根	✅

HDFS 侧 dfsadmin -report:

Configured Capacity 175.8 GB,DFS Used 115.9 GB(69.89%),Remaining 49.93 GB
Missing blocks = 0,corrupt = 0 → 数据完整
(有 8 个 "under-replicated blocks":单 DN + 副本数=1,没有第二个节点可放副本,属正常噪音,不是数据丢失,忽略即可)
结论
HDFS 数据盘(vdb)毫无压力,69% 占用主要是 pair_loc_slot/31d 输入(~103G)+ 各 run 产物。
爆的是独立的 shuffle 盘(vdc),被卡死的 0022 自己的 179G 中间数据撑到 97%。
两块盘互不影响,所以杀掉 0022 释放 vdc 的 179G appcache,不会动到 HDFS 的任何数据——HDFS 输入/已完成产物全在 vdb 上,安然无恙。这也印证了之前的判断:瓶颈是 shuffle 临时盘的定容(K=3 不适配 196G 盘),不是 HDFS 容量。

要我现在 kill 0022 释放 shuffle 盘吗?释放后 NM ~2min 自动转 HEALTHY,然后按 K=4 重跑 stage2。

---

全部完成。下面是这次在 speed 上做的改动总结。

磁盘拓扑(已生效并验证)
挂载点	设备	容量	用途
/data/hdfs	vdb	196G	HDFS 数据盘 #1
/data/hdfs2	vdc	196G	HDFS 数据盘 #2(新增,原 shuffle 盘改作此用)
/data/shuffle	vdd	300G	专用 shuffle 盘(YARN nm-local/nm-logs)
效果验证:

HDFS Configured Capacity:175.8G → 351.6G,DFS Remaining 50G → 215.7G,Missing blocks=0(数据完整)—— HDFS 溢出风险彻底消除。
shuffle 独享 300G:K=3 峰值 ≈139G = 46%,稳。
NM RUNNING / DataNode 双卷已注册健康。
fstab 已持久化(3 行 UUID),备份在 /etc/fstab.bak.* 和 hdfs-site.xml.bak.*,可回滚。
配置改动:hdfs-site.xml 的 dfs.datanode.data.dir = /data/hdfs/dfs/data,/data/hdfs2/dfs/data;yarn-site 无需改(local-dirs 仍 /data/shuffle/*,只是底层换成 300G 盘)。
gzip 最终输出压缩(已写入仓库脚本)
scripts/env.sh 新增 OUT_COMPRESS(final-output gzip,区别于 TUNE_31D 里的 map/shuffle gzip)。
scripts/cluster_run.sh 仅给 stage2 + stage3 注入,不碰 stage0a/stage1——pair_loc_slot 是 stage2 输入,gzip 不可切分,压它会毁掉 stage2 map 并行度。
Dry-run 已确认:3 轮 stage2(rounds=3)+ stage3 全部带上 mapreduce.output.fileoutputformat.compress=true … GzipCodec。
跑之前注意两点
用新开的终端跑,避免旧 shell 里残留的 STAGE2_ROUNDS_31D 导出值盖掉 K=3 默认(见 env.sh stale-export 坑;OUT_COMPRESS 是新变量无此风险)。
--build 需要 Java 8 —— 当前本机 java 是 21,build 前先 export JAVA8_HOME=...(不 build 直接用现有 jar 提交则无影响)。
集群现在干净空闲(0 app / 0 container),随时可以重跑 stage2。要我帮你直接发起 31d 的 --from stage2 重跑吗?

一个遗留项(非阻塞):deploy/single-node/{hdfs,yarn}-site.xml 模板仍是单 __HDFS_VOL__ 占位,和现在的双盘 HDFS 实况有出入——若以后重装机器需要同步,我已记进 memory,可随时帮你更新模板 + bootstrap。