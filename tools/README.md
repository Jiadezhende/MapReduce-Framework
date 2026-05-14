# tools - 共享辅助工具

`tools/` 放不属于某个固定 stage 的辅助脚本或一次性 MapReduce Job。它们可以依赖 `common`，但不应该依赖 `stage0` 到 `stage3`，避免形成反向依赖。

## 子目录

| 目录 | 作用 |
|---|---|
| `profiler/` | 统计 vid、loc、day 的长尾分布，供 Stage 1 热点处理和 bench 报告使用 |

## 约定

- 工具应尽量自包含。
- 可以依赖 `common-*.jar`。
- 不依赖 stage 模块。
- 输出路径和格式需要在各工具 README 中写清楚。
