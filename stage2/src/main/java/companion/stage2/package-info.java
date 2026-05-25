/**
 * Stage 2 — 共现计数与阈值过滤。
 *
 * <p>按 pair 去重统计 distinct (loc, slot) 见证数，过滤 count &lt; companion.k.min 的结果，
 * 输出 {@code vidA,vidB,count}。详细契约见 stage2/README.md 与 docs/fixtures.md。
 *
 * <p>实现入口是 {@link companion.stage2.Stage2Job}。
 */
package companion.stage2;
