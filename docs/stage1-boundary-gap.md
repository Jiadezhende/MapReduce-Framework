# Stage1 跨 Slot 边界配对遗漏

## TL;DR

Stage1 用 `hash(loc, slot/2)` 分区，`2k+1 → 2k+2` 的跨边界配对永久丢失，单轮覆盖率仅 50%。这是 **Stage1 当前实现的缺陷**——边界丢 ~20% 配对，`k.min=3` 把这些边缘 pair 大面积淘汰，最终少算 ~70%（baseline 53,946 vs MR 37）。修复方案：加一轮偏移分区的 J1b（详见 §5）。

baseline 默认跑理想语义，不做多余的切分和丢弃，给的就是 Stage1 应该产出的目标。当前 ~1400x 的 gap 就是 J1b 上线前 Stage1 真实少算的规模，不要替它找补成"结构性约束"或"理想不可达"。

---

## 1. 问题现象

`diff_baseline.py` 比较 MR 与 baseline 输出，差距巨大：

| | pairs |
|---|---|
| MR 输出 | ~37 |
| Baseline（理想语义） | ~53,946 |

**baseline 给出的就是 pipeline 应该产出的目标，MR 少算了 99.93%**。这个 gap 的来源见 §3。

---

## 2. 根因：分区隔离

Stage1 要对同一 `loc` 下时间差 ≤ `delta.t` 的车辆两两配对。但 MR Reducer 按分区隔离——一条记录只能进一个 reducer，时间窗口却可能跨 slot 边界。

`hash(loc, slot/2)` 让每个 reducer 拿到一对相邻 slot `{2k, 2k+1}`：

```
slot:   0   1 | 2   3 | 4   5 | 6   7
part:   P0      P1      P2      P3
        0→1 ✓   2→3 ✓   4→5 ✓   6→7 ✓   ← reducer 内部，tail buffer 覆盖
            ✗       ✗       ✗            ← 1→2 / 3→4 / 5→6 跨 partition，丢失
```

reducer 处理 `slot=2k` 后把末尾 `delta.t` 秒的记录存入 tail buffer，处理 `slot=2k+1` 时与之配对。
**但奇→偶（`2k+1 → 2k+2`）的边界落在两个 partition，reducer 看不到对方，配对永久丢失。**

→ 单轮覆盖率 50%。`slot.size = delta.t = 300` 时遗漏窗口窄，实际遗漏约占总配对 **1-3%**。

---

## 3. gap 的构成：边界丢失 + k.min 放大

baseline 按 `loc` 全局分组，跑理想语义——任何同 loc、时间差 ≤ delta.t 都配对，不切分、不丢弃。Stage1 当前实现的两个动作把这个目标压到了 37：

1. **分区截断丢 ~20% 配对**：奇→偶 slot 边界落在两个 reducer，配对永久丢失（10k 行实验下 31,242 → 25,218）。
2. **`k.min=3` 把丢失放大成 70% pair 淘汰**：伴随判定需要 ≥3 个不同 (loc, slot) 同时出现。大量 pair 卡在「刚好 3 次」边缘，少一次就过不了线（125 → 37）。
3. **数据规模放大**：全量数据下边缘 pair 更多，叠加更狠，最终观测到 37 vs 53,946。

> 一句话：边界丢 20% 配对，"≥3 次" 门槛把每个边缘 pair 判了死刑，规模越大差距越夸张。

**这 53,946 vs 37 的 gap 就是 Stage1 当前缺陷的真实尺度**——baseline 没算多，是 Stage1 算少了。修复见 §5；J1b 上线后这个 gap 应该收敛到接近 0。

---

## 4. 两个判据各管什么

- **`FixtureGenerator` + `cluster_test.sh`**：fixture 表达**理想 Stage1 语义**（按 `loc` 全局滑窗、无 `slot/2` 切分、无边界丢失，详见 [docs/reference-semantics.md](reference-semantics.md) §4）。所以 stage0/2/3 的 cluster_test 期望逐字节绿；**stage1 的 cluster_test 在 J1b 上线前持续红**——这是诚实暴露 boundary gap 实测规模的方式，不再用"design-conformance"的措辞规避。golden vs cluster 的缺失行数比例应落在 15–25% 区间，与 §3 估算吻合。
- **baseline**：端到端语义 + 性能分析。baseline 默认理想语义、与 stage1 fixture 同源；`--mirror-stage1-limits` 可临时镜像 production 缺陷做诊断对照。

两个判据互补，但各管各的：cluster_test 是 per-stage 中间结果对账；baseline 是整链路 diff + 大数据量性能验证。J1b 落地时不需要重生 fixture（语义已经是 ideal），只需 stage1 cluster_test 自动从红转绿；若不转绿说明 J1b 没完全收敛 boundary loss。`Stage1JobTest` 内联期望值（`CROSS_SLOT_PAIRS=2` 等）也要在 J1b 上线时同步更新——它当前断言的是缺陷设计的行为。

---

## 5. 修复方案：J1b 第二轮补偿

第二轮把分区偏移 1 个 slot，让原来被切开的 `2k+1 → 2k+2` 落入同一 partition：

```
J1a (当前):  hash(loc, slot/2)       覆盖 0→1 2→3 4→5
J1b (补偿):  hash(loc, (slot+1)/2)   覆盖 1→2 3→4
J1a ∪ J1b:  所有相邻 slot 边界全覆盖 ✓
```

### 改动点

| 文件 | 改动 |
|---|---|
| `SkewAwarePartitioner` | 分区偏移：`(slot + offset) / 2`，`offset = isJ1b ? 1 : 0`，由 `companion.salt.seed` 决定 |
| `Stage1Reducer.canUseTail` | J1b 下 tail buffer 方向反转：`lastSlot % 2 == 1`（奇→偶） |
| `scripts/cluster_run.sh` | 提交两轮，`getmerge` 合并到同一 `pair_loc_slot/` |
| `FixtureGenerator` | **无需改动**（已是 ideal 语义）；`Stage1JobTest` 的 `PAIRS_EMITTED` / `CROSS_SLOT_PAIRS` 内联期望值需更新以反映 J1a+J1b 合并后的结果 |

**去重**：两轮 slot 内部配对会重叠，但 Stage2 计数是 `distinct (loc, slot)`，天然去重，无需额外逻辑。

**代价**：Stage1 计算量与 shuffle 翻倍（2x），两轮串行 ~2T wall-clock。

### 渐进交付

| 里程碑 | 建议 |
|---|---|
| M1 (1d) | 单轮 J1a 可能够用，先跑 baseline diff 看实际 recall |
| M2 (7d) | recall < 99% 则加 J1b |
| M3 (31d) | J1b 必须上线 |

### 备选方案（均不推荐）

- **Mapper 双发**（每条发 slot + slot-1）：数据翻倍 + Stage2 需去重。
- **`hash(loc)` 全局分区**：热点 loc OOM，失去并行度。

---

## 6. 如何读 baseline diff 报告

baseline 默认跑理想语义：全局 `(loc, t_norm)` 配对，不做额外的分区切分和 loc 热点丢弃。它就是 pipeline 应该产出的目标，也是衡量 J1b 上线前 Stage1 少算多少的标尺。

读报告时：

- `missing_in_mr` 全部都是真缺失——Stage1 没产出但 baseline 算出来的 pair。J1b 之前这个集合规模会很大（数万级），不要尝试把它解释成"分区天然产不出，可以忽略"。
- `recall = mr_pairs / baseline_pairs` 反映的是 Stage1 当前少算的比例，不是实现 bug 指标。实现层面的正确性走 §4 的 `cluster_test` 路径。
- 想复现 §3 那个「Stage1 当前实现下 baseline 会缩水到多少」的对照实验，加 `--mirror-stage1-limits` 跑一次（镜像 reducer 分区 + loc 热点截断）。这是诊断 J1b 前后影响的工具，不作默认。
