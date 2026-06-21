/**
 * Companion — 共享基础库。
 *
 * <p>跨 stage 复用的接口层：自定义 Writable（{@link companion.io}）、
 * 配置封装（{@link companion.conf.CompanionConf}）、Job 基类
 * （{@link companion.job.AbstractCompanionJob}）与工具函数（{@link companion.util}）。
 * 字段编码、配置键与 Counter 命名等跨模块契约见 common/README.md 与 docs/architecture.md。
 *
 * <p>本模块仅依赖 Hadoop 与 slf4j，不依赖任何 stage。
 */
package companion;
