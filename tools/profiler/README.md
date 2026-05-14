# tools/profiler - 长尾分布统计

**Owner**: R2（详见 [docs/roles.md](../../docs/roles.md)）

profiler 用来在正式跑流水线前了解数据分布，尤其是车辆和地点的长尾情况。它的结果会影响 Stage 0 的过滤策略、Stage 1 的热点桶处理，以及 bench 的 skew 分析。

## 本工具要解决什么

它需要生成三类统计：

| 统计 | 用途 |
|---|---|
| 每个 vid 的出现次数分布 | 判断单次车过滤能减少多少数据 |
| 每个 loc 或 `(loc, slot)` 的流量分布 | 找出 Stage 1 可能爆炸的热点桶 |
| 每天的记录数 | 检查 1d / 7d / 31d 切片是否均衡 |

## 输出

默认输出到：

```text
hdfs:///companion/profile/
```

建议同时输出 JSON 和 CSV：

- JSON 供脚本和报告直接读取。
- CSV 供人工检查、画图或导入表格工具。

## 验收信号

- 能列出 top 热点 `loc` 或 `(loc, slot)`。
- 能给出单次车数量和占比。
- bench 可以直接消费 profiler 输出生成 skew 图或热点表。
