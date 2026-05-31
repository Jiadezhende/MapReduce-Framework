# 1d 全量跑运行报告 — `weichenyin-cbe21c8-20260530221139`

提交于 commit **cbe21c8**（在 `49d70f2` 引入分档 reducer 配置 **之前**，也在 `e3848df` 清理 Stage2 多余 Combiner 之前），所以 Stage2 既挂着无效 Combiner，Stage1/Stage2 也都用 `companion-conf.xml` 默认 reducer 数（8）。本目录同时归档 `_metrics.json` 与 `top_n.csv`（10000 行 Top-N pair）。

## 1. 时间线（UTC，2026-05-31）

| 阶段 | application | 开始 → 结束 | 用时 |
|---|---|---|---|
| Stage0a Freq | `_0089` | 02:11:25 → 02:12:43 | 1m18s |
| Stage0b Filter | `_0090` | 02:12:49 → 02:13:37 | 0m48s |
| Stage1 j1a | `_0091` | 02:13:47 → 02:23:12 | 9m25s |
| Stage1 j1b | `_0092` | 02:23:14 → 02:26:57 | 3m44s |
| **Stage2** | `_0093` | 02:27:08 → **02:57:15** | **30m07s** |
| (gap: InputSampler 本地跑) | — | 02:57:15 → 02:57:26 | ~11s |
| Stage3 Sort | `_0094` | 02:57:26 → 02:59:33 | 2m07s |
| Stage3 TopN | `_0095` | 02:59:35 → 03:01:25 | 1m50s |

- **端到端 wall-clock：50m00s**（Stage0a 起 → Stage3 TopN 止）
- Stage2 占全程 60%，体感和 7d 跑（63%）几乎一样 — 同一段瓶颈，规模缩小后比例几乎不变。
- InputSampler 本地阶段只用了 ~11s（7d 那次 ~15min）— 1d 数据小，采样开销可忽略。

## 2. 数据体量（counter cross-check，确认确为 1d 档）

- 入口 Stage0a 输入 **9.28M** 条 raw，Stage0b 过滤后 **7.97M** behavior（≈ 7d 那次 61.68M 的 1/7.7，比例对得上 1d 档）。
- Stage1 输出：**j1a 966.78M + j1b 311.30M = 1.278B** pair-witness。
- Stage2 reduce 出 **23.07M** 个唯一 pair（与 `_metrics.json` 的 `pair_total=23,071,570` 完全一致）。
- 7d 跑（49d70f2）同一指标：61.68M behavior → 10.02B pair-witness → 193.65M pair。**1d 与 7d 的体量比 ~7.7×（行为）/ ~7.8×（pair-witness）/ ~8.4×（最终 pair）**。

## 3. HDFS / 本地磁盘峰值

### HDFS（压缩后，replication=1）

| 时刻 | 同时驻留 HDFS 的中间产物 | 大小 |
|---|---|---|
| Stage1 跑完、Stage2 开跑前 | Stage0a freq + Stage0b filtered + j1a + j1b | **~3.78 GB**（j1a 2.79 + j1b 0.90 为主） |
| Stage2 输出落地 | + Stage2 sorted | +0.38 GB |

跑这次时 master DN 还没上线（2026-05-31 才加 master DN，见 [[cluster_access]]），只 2 个 DN 平摊 → 每 DN 增量约 **1.9 GB**。整个 HDFS 增量低，1d 档完全跑不动磁盘。

### Worker 本地（NM scratch + userlogs）— 触发了一次旧 90% 阈值

NM 日志（`hadoop-root-nodemanager-worker{1,2}.log`）在 1d j1a 运行窗口内有触发记录：

```
2026-05-31 02:19:55,348 WARN DirectoryCollection: Directory .../nm-local-dir
    error, used space above threshold of 90.0%, removing from list ...
2026-05-31 02:19:55,348 WARN DirectoryCollection: Directory .../logs/userlogs
    error, used space above threshold of 90.0%, removing from list ...
2026-05-31 02:20:31,040 同上，worker2 触发
```

- 时间正落在 j1a（02:13–02:23）末段。当时**阈值还是 90%**，且 worker `/` 只有 50 GB；j1a 本身写盘量不大，触发的更多是历史死数据垫高基线（[[docs/7d-trouble-shooting.md]] §10 的 6GB 老 `dfs/data` + 老 `nm-local-dir`）。
- 触发后两节点短暂 UNHEALTHY，~8min 后（02:28）自动恢复 RUNNING。Stage2（02:27 开跑）刚好接住，但确实是踩着红线过。
- 跟 7d 49d70f2 跑的差异：当时阈值已升到 95% + 双 volume + 死数据清理，所以同样级别的 spill 不会再触发。这次 1d 是"侥幸通过"，**不能因为 1d 短就以为磁盘没事**。

### Stage2 cumulative spill

| counter | 值 |
|---|---|
| Map output records | 1.278 B |
| Map output bytes（未压缩） | 14.62 GB |
| Map output materialized（shuffle 后压缩） | 15.46 GB |
| FILE bytes written（map spill + reduce shuffle 落盘累计） | **50.90 GB** |
| FILE bytes read | 35.62 GB |
| Spilled Records | 3.78 B |
| **Combine input records** | **2.43 B** |
| **Combine output records** | **2.30 B**（削掉 5.4%，~131M 记录） |

- 50.90 GB cumulative / 2 worker ≈ 每 worker **~25 GB cumulative**，瞬时驻留更小。这是踩在 90% 阈值上的根因。
- **Combiner 在 1d 档真的削了 5.4% 记录**，和 7d 那次"削掉 0%"形成鲜明对比。原因推测：1d 单 spill 块里同一 (vidA,vidB) 出现频率更高（局部聚集），而 7d 数据稀疏后局部去重失效。这一对照数据正是 e3848df 清理 Combiner 的支撑——**它在 1d 还有点用，但收益不抵 CPU 成本，且在目标规模（7d）上完全零收益**。

## 4. 当时的工作流（cbe21c8 时点）

```
Stage0a Freq → Stage0b Filter
        ↓
Stage1 j1a (含 within-slot, 8 reducers) ────┐
Stage1 j1b (跳过 within-slot, 8 reducers)──┴→ Stage2 (Map → Combiner(局部削 5.4%) → Reduce, 8 reducers)
                                                  ↓
                                      Stage3 Sort (TotalOrderPartitioner)
                                                  ↓
                                            Stage3 TopN
```

与 49d70f2 7d 跑（[[docs/runs/7d-49d70f2/report.md]]）的差异：
- **reducer 数都是 8**（cbe21c8 还没引入按档配置；env.sh `REDUCERS_1D=8` 是 49d70f2 才加，但 1d 档本来也是 8，结果等价）。
- 其余流水线一致：j1b 跳过 within-slot 已上线、Stage1 桶级去重已上线、Stage2 Combiner 仍在。

## 5. 产出文件

- `_metrics.json` — Stage3 TopN 阶段写出的指标快照：`pair_total=23,071,570`，count 直方图（3 次最多 17.24M，10-99 次 4047，最高桶 100-999 次为 0 — 1d 档热 pair 上限远低于 7d）。
- `top_n.csv` — TopN 结果，10000 行，列：`vidA,vidB,count`，按 count 降序。最高 count = 42（7d 那次最高 count = 307）。
