# 7d 全量跑运行报告 — `weichenyin-49d70f2-20260531134458`

提交于 commit **49d70f2**（在 `e3848df` 清理 Stage2 多余 Combiner **之前**），所以 Stage2 当时仍然挂着那个无效 Combiner。本目录同时归档 `_metrics.json`（Stage3 TopN 阶段产出指标）与 `top_n.csv`（10000 行 Top-N pair）。

## 1. 时间线（UTC，2026-05-31）

| 阶段 | application | 开始 → 结束 | 用时 |
|---|---|---|---|
| Stage0a Freq | `_0111` | 13:44:42 → 13:47:33 | 2m51s |
| Stage0b Filter | `_0112` | 13:47:39 → 13:52:02 | 4m23s |
| Stage1 j1a | `_0113` | 13:52:12 → 14:24:01 | 31m50s |
| Stage1 j1b | `_0114` | 14:24:03 → 14:36:22 | 12m19s |
| **Stage2** | `_0115` | 14:36:33 → **17:08:47** | **2h32m14s** |
| (gap: InputSampler 本地跑) | — | 17:08 → 17:23 | ~15m |
| Stage3 Sort | `_0116` | 17:23:52 → 17:37:15 | 13m23s |
| Stage3 TopN | `_0117` | 17:37:17 → 17:46:33 | 9m16s |

- **端到端 wall-clock：4h 01m 51s**（Stage0a 起 → Stage3 TopN 止）
- Stage2 一阶段就占了全程 63%，主因正是下文的"combiner 全空转 + 大量 spill"。

## 2. 数据体量（counter cross-check，确认确为 7d 档）

- 入口 Stage0a 输入 66.89M 条 raw，Stage0b 过滤后 **61.68M** behavior（与 cbe21c8 7d 跑同一数量级；1d 基线只有 ~12M）。
- Stage1 输出：**j1a 7.49B + j1b 2.53B = 10.02B** pair-witness（比 cbe21c8 的 11.03B 少 ~9%，桶级去重 `70aedf2` 上线后的收益）。
- 等价于 1d 基线的 **5.2×**。

## 3. HDFS / 本地磁盘峰值

### HDFS（压缩后，replication=1）

| 时刻 | 同时驻留 HDFS 的中间产物 | 大小 |
|---|---|---|
| Stage1 跑完、Stage2 开跑前 | Stage0a freq + Stage0b filtered + j1a + j1b | **~27.3 GB**（j1a 20.08 + j1b 6.70 为主） |
| Stage2 输出落地 | + Stage2 sorted | +3.07 GB |

2 个 DN 平摊 → 每 DN 增量约 **13–14 GB**。

### Worker 本地（NM scratch + userlogs）— 真正的瓶颈

NM 日志直接证据（`/opt/module/hadoop-3.3.6/logs/hadoop-root-nodemanager-worker2.log`）：

```
2026-05-31 16:48:48,499 WARN DirectoryCollection: Directory .../nm-local-dir
    error, used space above threshold of 95.0%, removing from list ...
2026-05-31 16:52:47,660 WARN ...（同上，再次触发）
```

- 触发时间 16:48 / 16:52 UTC 正落在 Stage2 reduce 阶段中段。
- 阈值已在 cbe21c8 失败后调到 95%，即 worker2 `/`（50 GB）被打到 **≥ 47.5 GB**。
- worker1 NM 日志窗口内**没有**触发记录 → 偏斜：worker2 紧、worker1 宽松。
- 与失败的 cbe21c8 跑相比，本次靠"双 volume + 95% 阈值 + 桶级去重"刚好压在红线上侧未爆。

### Stage2 cumulative spill（解释为什么 worker2 这么紧）

`mapred job -status job_0115` 关键 counter：

| counter | 值 |
|---|---|
| Map output records | 10.02 B |
| Map output bytes（未压缩） | 125.5 GB |
| Map output materialized（shuffle 后压缩） | 82.6 GB |
| FILE bytes written（map spill + reduce shuffle 落盘累计） | **297.6 GB** |
| Spilled Records | 36.4 B |
| **Combine input records** | **20.03 B** |
| **Combine output records** | **20.03 B** ← 完全没削掉一条 |

- 297.6 GB cumulative / 2 worker ≈ 每 worker **~149 GB cumulative**，并发驻留瞬时可达数十 GB，正是把 worker2 `/` 打到 95% 的原因。
- Combine input = Combine output：多余的 Stage2 Combiner 真实读数——CPU 烧了 200 亿条 key 比较 + 序列化反序列化，0 削减。`e3848df` 切掉它有数据支持，不是拍脑袋。

## 4. 当时的工作流（49d70f2 时点）

```
Stage0a Freq → Stage0b Filter
        ↓
Stage1 j1a (含 within-slot) ────┐
Stage1 j1b (跳过 within-slot)──┴→ Stage2 (Map → Combiner(空转) → Reduce)
                                      ↓
                          Stage3 Sort (TotalOrderPartitioner)
                                      ↓
                                Stage3 TopN
```

与当前 `main` 的差异只有一处：**Stage2 的 Combiner 还在**。Stage1 二轮偏移分区（boundary gap）和 j1b 桶级去重当时都已上线。

## 5. 产出文件

- `_metrics.json` — Stage3 TopN 阶段写出的指标快照（pair_total、count 直方图、TOPN_EMITTED 计数等）。
- `top_n.csv` — TopN 结果，10000 行，列：`vidA,vidB,count`，按 count 降序。
