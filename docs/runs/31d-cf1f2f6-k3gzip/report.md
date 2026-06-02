# 31d 复跑(K=3 + Gzip)恢复成功复盘 — `31399-cf1f2f6-20260601004404` (app `_0131`)

同一 run_id 的**第二次尝试**:首跑 app `_0129` 在 Stage2 map ~28% 把 worker2 nm-local 打穿 95% 而崩(见 [`../31d-cf1f2f6-failed/report.md`](../31d-cf1f2f6-failed/report.md))。本次 `--from stage2` 复用首跑的 `pair_loc_slot`(103.5 GB),换用 **§7.6 的 A+D 修复(Gzip shuffle codec + pair-hash 分 K=3 轮)** 重跑 Stage2。

> 一句话结论:**A+D 修复有效,Stage2 r0 成功落地。** 但 r0 在 reduce copy 阶段仍把 worker2 顶到 `/` 92%(离 95% 崩线仅 ~1.5 G),靠**在线 `hdfs diskbalancer`(节点内 `/`→`/home` 挪 DN block)**临场救回;`_SUCCESS` 一到,锁死的 21 G map 输出悬崖式释放、worker2 `/` 从 92%→60%。**98 G/节点跑 K=3 = "能跑但贴边、需盯盘"。稳态应取 K=4;K=6 无必要(见 §5)。**

---

## 0. 这次相对首跑改了什么

| 项 | 首跑 `_0129`(崩) | 本次 `_0131`(成) |
|---|---|---|
| Stage2 shuffle codec | 默认 Snappy | **Gzip(zlib)**,密 ~1.6× → 每节点 shuffle/round 砍掉 ~40% |
| 分轮 | 单遍 K=1 | **K=3**(`companion.stage2.rounds=3`),每轮只 emit 1/3 pair → 每节点 shuffle peak ÷3 |
| 跑前准备 | 无 | `hdfs balancer` 把 worker2 `/` 从 53%→43% 先匀出余量 |
| 入口 | `--days 31`(从 stage0) | `--from stage2` 复用 `pair_loc_slot` 103.5 G,不重跑 Stage0/1 |
| reduces | 32 | 32 |

实参确认:`companion.stage2.rounds=3`、`companion.stage2.round=0`、`mapreduce.map.output.compress.codec=GzipCodec`,输出落 `companions/31d/r0/`。

---

## 1. r0 实时轨迹(全程盯盘记录)

NM 崩线:`yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage = 95%` ⇒ 50 G 卷上 **47.5 G** 即 UNHEALTHY。有效节点 ~2.4(master 仅 4 G NM / 2 槽,跑 ~17% map;两个 8 G worker 各 ~41%)。

| 时点(map%) | worker2 `/` | worker2 `/home` | 处置 |
|---|---|---|---|
| map ~8% | 30 G / 59% | — | 基线(DN+OS),shuffle 未起 |
| map 64% | 42 G / 84% | 32 G / 68% | shuffle/`=13 G。一度误判会线性冲 46–47 G |
| map 78% | 42 G / 84% | — | **平台**(worker2 自身 map 近完成,shuffle 不再随全局 map% 涨) |
| map ~85% | 44 G / 87% | 40 G / 83% | 两卷同时回升(reduce copy 起 + 剩余 map),~3.5 G 到崩线 |
| **map 100%** | **46 G / 92%** | 42 G / 89% | **触发预设止损线**;shuffle/`=21 G、/home=22 G |
| 同点(救) | **41 G / 82%** | 37 G / 78% | **`hdfs diskbalancer` `/`→`/home` 挪 3 G DN**(DN/`:25→22 G),`/` 多腾 2.7 G |
| reduce ~16% | 46 G / 91% | 43 G / 91% | reduce copy 把 spill 堆到 `/home`;两卷顶 90–92% 平台,shuffle/`=21 G 冻死 |
| reduce ~23% | 46 G / 91% | 43 G / 90% | **平台 + 微回血**(空闲 +0.2~0.3 G),进度仍 +3.6 点 → 确认稳态非失控 |
| **r0 `_SUCCESS`** | **30 G / 60%** | (释放) | **悬崖跳水**:锁死的 21 G map 输出 + 残余 scratch 整块释放 |

r0 产出 `companions/31d/r0` = 4.4 G;app `_0131` **FINISHED / SUCCEEDED**;`cluster_run.sh` 自动进入 r1(从 DN 已均衡的更好基线起跑)。

---

## 2. 经验:三条被实测钉死的机理

### 2.1 map 输出常驻到 job 末,reduce 中途不释放
worker2 `/` 上那 **21 G map 输出全程冻死**(reduce copy/merge/reduce() 期间纹丝不动),只在 r0 `_SUCCESS` 那一刻整块释放(92%→60%)。证实 §7.6:单节点 shuffle 常驻峰值 = `单遍 map 输出 ÷ 节点数`,**留到 job 结束**(供 reducer 重试拉取),与 reduce 进度、`slowstart`、`io.sort.mb` 无关。**真正的"回落"是轮边界的悬崖,不是 reduce 中途的渐降。**

### 2.2 reduce 分波跑 → spill 趋平台,不是单调爆涨
20 G 容器池只容 ~9–10 reducer 并发,每个 reducer copy→merge→reduce()→释放→下一个补位。所以 reduce-side spill 在 `/home` 顶到 ~23 G 后**平台化**(reduce ~16%→23% 时 spill +1 G 而进度 +3.6 点),而非堆到全量 106 G。这是 r0 能在 92% 顶住没破线的根本原因。

### 2.3 一旦 shuffle 在填,唯一可用的在线腾地杠杆是节点内 diskbalancer
- **`hdfs diskbalancer`(节点内卷间)**:把 DN block 从热卷 `/` 挪到冷卷 `/home`,**在线、不丢数据、对 job 无 I/O 争用**,5 分钟救回 2.7 G —— **唯一对症 "worker2 `/` 是热卷" 的手段**。前提卷间有 DN 不均(本次 `/`=25 G vs `/home`=18 G,正源于 `/` 多扛 DN 数据)。
- **`hdfs balancer`(跨节点)= 死杠杆**:三 DN 利用率 43/41/38% 在阈值内,默认 no-op;强压低阈值也只挪 1–2 G,还抢 I/O。
- **清理 = 基本无油水**:run dir 105.7 G 里 `pair_loc_slot` 103.5 G 是 r1/r2 还要读的输入(不能删);可安全删的仅 `filtered`(1.8 G)+`vid_freq`(0.44 G),但二者是 `--from stage1` resume 的上游、且落到 worker2 仅 <1 G,**不值当**,最终未删。

---

## 3. HDFS / 容量轴评估(实测)

| 产物 | 大小 | 备注 |
|---|---|---|
| raw `31d.csv` | 5.7 G | stage0 输入,常驻 HDFS |
| `vid_freq` (S0a) | 0.44 G | Stage0b 消费完;仅 `--from stage1` resume 需要 |
| `filtered` (S0b) | 1.8 G | Stage1 消费完;同上 |
| **`pair_loc_slot` (S1)** | **103.5 G** | zlib rep=1,占 run 98%,r0/r1/r2 共同输入 |
| `companions/r0` (S2) | 4.4 G | 单遍 ~12–13 G(三轮合计) |
| **`/companion` 总计** | **117.5 G** | 跑时 HDFS 剩 ~96 G(3 DN 共 282 G,Used 54%) |

**容量轴全程不是约束**(剩 ~96 G,线性增长下约 ~50d 才逼近)。瓶颈唯一在 nm-local 本地盘轴。

---

## 4. shuffle / 本地盘轴评估(本次校准)

- 单遍 Stage2 map 输出 materialized ≈ **319 G**(82.6 G@7d × 3.86),留到 job 末。
- 每节点 shuffle 峰值 = `319 / (N_eff × K) × skew`。本次 N_eff≈2.4、K=3 ⇒ 单卷 ~44 G(实测 21 G `/` + 23 G `/home`),与公式吻合。
- 98 G/节点(50+48)跑 K=3 算下来需 ~96 G ⇒ **正好卡边**,这就是 r0 全程贴 92% 的来由。

---

## 5. 评估结果:31d 稳定运行的盘容量 / K 选择

**每节点盘容量公式**:
```
盘 ≥  (120×rep)/N        # HDFS 份额(池化)
    + 320/(N×K) × 1.3    # shuffle 峰值(×1.3 含倾斜+reduce scratch,不可池化)
    + 10G                # OS/日志/余量
```

**3 节点(rep=1,N=3):**

| K | HDFS/节点 | shuffle 峰值/节点 | 合计需要 | 配盘 | 评价 |
|---|---|---|---|---|---|
| 1 | 40 | ~139 | ~189 | 200 G | 一遍跑完最快 |
| 2 | 40 | ~70 | ~120 | **128 G** | **稳态甜点,无需盯盘** |
| 3 | 40 | ~46 | ~96 | 100 G | **本次:能跑但贴边、需 diskbalancer 兜** |
| 4 | 40 | ~35 | ~85 | 96 G | 当前硬件下的性价比拐点 |
| 6 | 40 | ~22 | ~70 | — | **过度保险,见下** |

**单机(install.sh 路线,N=1,扛全量 shuffle):** K=1→**512 G**、K=3→**256 G**。

**K 选择结论:**
- **K=3**:98 G/节点能跑(本次实证),但贴 92%、要前置 balance + 在线 diskbalancer。
- **K=4**:推荐的 **31d 稳态默认**。峰值 92%→85%,只多扫一遍 103.5 G 输入(+1~2 h),换"无感跑完"。
- **K=6:不必要。** 每轮都全量重扫 `pair_loc_slot`(K 次读 103.5 G),K=6=621 G 冗余读、wall-time 近翻倍(~10 h→~18–20 h),只为砍一个 K=3/K=4 已经解决的峰值。

**最该做的结构改动**:把 `yarn.nodemanager.local-dirs`(shuffle)与 `dfs.datanode.data.dir`(HDFS)放在**两块独立盘**,各自定容(HDFS ~50 G、shuffle 按 K),shuffle 突发不再威胁 HDFS 提交,从根上免除本次这种半夜盯盘。

---

## 6. 状态与未决项(报告落盘时)

- ✅ **r0 SUCCEEDED**,worker2 `_SUCCESS` 后回落 60%,r1 已自动起跑(DN 均衡基线,预期比 r0 更顺)。
- ⏳ r1 / r2 待完成;之后 Stage3(`final/31d`,读 `companions/31d/` 经 `input.dir.recursive=true` 取 r0/r1/r2)。
- 预期 r1/r2 复刻 r0 形状但更宽松(diskbalancer 后 DN 已 22/21 均衡,起跑点更低)。
- 收尾后补:整 run wall-time、各轮 reduce 分布、Stage3 产出与 TopN。

## 7. 关联文档
- [`../31d-cf1f2f6-failed/report.md`](../31d-cf1f2f6-failed/report.md) —— 首跑崩盘复盘(本次的对照起点)
- [`../../space-optimization.md`](../../space-optimization.md) §7.6 —— A+D 修复设计
- [`../../disk-scaling-analysis.md`](../../disk-scaling-analysis.md) —— 1d/7d/31d 伸缩与扩容 vs 优化决策
- [`../../../scripts/env.sh`](../../../scripts/env.sh) —— `STAGE2_ROUNDS_31D` / `TUNE_31D` 实参
