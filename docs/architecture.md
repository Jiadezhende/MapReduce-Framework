# Architecture — Companion Vehicle Mining

This document is the contract between modules. R1 maintains it; every change requires sign-off from owners of affected stages. Full role allocation lives in [roles.md](roles.md).

## 1. HDFS layout

One tree under `${COMPANION_ROOT}` (default `/companion`), with four kinds of subtree:

1. **Shared, read-only** (`input/`, `profile/`, `snapshots/`) — only data maintainers and the integration owner write here.
2. **Per-run** under `runs/<run_id>/` — every `scripts/cluster_run.sh` invocation creates a fresh `run_id`; runs are isolated by `run_id` (no per-user segment), so concurrent runs do not collide.
3. **Isolation tests** under `test/<stage>-<ts>/` — `scripts/cluster_test.sh` single-stage runs, kept apart from prod data and torn down by default.

```
${COMPANION_ROOT}/
├── input/raw/{1d,7d,31d}.csv         # Raw CSV (owner: R1 upload_to_hdfs.sh)
├── snapshots/current/                # Stable cross-stage snapshots (v2, integration-owner only)
│   ├── filtered/{phase}/
│   ├── pair_loc_slot/{phase}/
│   └── companions/{phase}/
├── profile/                          # Long-tail histograms (owner: R2)
├── runs/<run_id>/                    # Per-run prod workspace (cluster_run.sh)
│   ├── vid_freq/{phase}/             # Stage0a BloomFilter SequenceFile output
│   ├── filtered/{phase}/             # Stage0b output (SequenceFile)
│   ├── pair_loc_slot/{phase}/        # Stage1 output (SequenceFile)
│   ├── companions/{phase}/           # Stage2 output (CSV after threshold)
│   └── final/{phase}/                # Stage3 output (sorted CSV + TopN + metrics)
│       ├── companions.csv
│       ├── top_n.csv
│       └── _metrics.json
└── test/<stage>-<ts>/                # Single-stage isolation tests (cluster_test.sh)
    ├── in/
    ├── vid_freq/                     # stage0 only
    └── out/
```

`run_id = ${USER}-${git_sha}-${ts}` (the user is part of the id for traceability, not a path segment). A run is resumable: each stage is skipped when its output already carries a Hadoop `_SUCCESS` marker, so re-running with the same `--run-id` continues from the breakpoint.

`{phase} ∈ {1d, 7d, 31d}` tags the dataset scale; the same pipeline runs at three scales. Stage0 reads `${COMPANION_ROOT}/input/raw/${phase}.csv` directly — v1 does not depend on a pre-sliced `input/{phase}/` directory.

## 2. Configuration keys

All keys live in `common/src/main/resources/companion-conf.xml` and are accessed exclusively via `companion.conf.CompanionConf` typed getters. No stage code should hard-code a key name.

| Key | Default | Owner |
|---|---|---|
| `companion.t0` | 1420041600 | R1 |
| `companion.delta.t` | 300 | R3 (consumer), R6 (sweep) |
| `companion.k.min` | 3 | R4 (consumer), R6 (sweep) |
| `companion.slot.size` | 300 | R3 |
| `companion.loc.skew.cap` | 200000 | R3 |
| `companion.pair.salt.n` | 16 | R4 |
| `companion.top.n` | 10000 | R5 |
| `companion.stage1.reducers` | 8 (1d) / 32 (7d) / 128 (31d) | R3, R6 |
| `companion.stage2.reducers` | 8 (1d) / 32 (7d) / 128 (31d) | R4, R6 |
| `companion.salt.seed` | 20260514 | R3 (J1a / J1b) |
| `companion.vid_freq.path` | empty | R2 (Stage0b) |
| `companion.run.tag` | empty | infra (cluster_run.sh) |

Override on submit with `-D companion.delta.t=600 …`.

## 3. Counter naming

Each stage uses one group; counters are upper snake-case nouns.

| Group | Counter | Emit point |
|---|---|---|
| `STAGE0` | `RAW_RECORDS` | every parsed input line |
| `STAGE0` | `PARSE_FAIL` | bad CSV line |
| `STAGE0` | `SINGLETON_VIDS` | vids dropped by frequency filter |
| `STAGE0` | `KEPT_RECORDS` | records surviving J0 |
| `STAGE1` | `INPUT_RECORDS` | per filtered record read by mapper |
| `STAGE1` | `PAIRS_EMITTED` | per pair emitted by reducer |
| `STAGE1` | `CROSS_SLOT_PAIRS` | pairs produced from tail buffer |
| `STAGE1` | `SKEW_DROP` | records dropped due to deque overflow |
| `STAGE1` | `HOT_LOCS_SLICED` | (loc, slot) groups that triggered salt slicing |
| `STAGE2` | `PAIRS_INPUT` | per witness read |
| `STAGE2` | `PAIRS_OUTPUT` | per pair surviving threshold |
| `STAGE2` | `HLL_FALLBACK_COUNT` | pairs that switched to HLL |
| `STAGE3` | `TOPN_EMITTED` | per row in TopN output |

R6's `parse_counters.py` is the single consumer; it expects this exact naming.

## 4. Writable contracts

| Type | Bytes | Use site | Notes |
|---|---|---|---|
| `companion.io.CompositeKey` | 12 (3 × int) | Stage1 key | Big-endian. `FullKeyComparator` for sort; `LocSlotGroupComparator` for grouping. |
| `companion.io.PairKey` | 8 (2 × int) | Stage1 out / Stage2 in/out | Always `vidA < vidB`; `set()` enforces it. |
| `companion.io.RecordWritable` | 12 (3 × int) | Stage0 out / Stage1 in | Fixed-width SequenceFile value. |
| `companion.io.LocSlotWritable` | 1–10 (VInt × 2) | Stage1 out / Stage2 in | Variable width — most ids small. |

Stage0a writes `vid_freq/{phase}` as `SequenceFile<NullWritable, org.apache.hadoop.util.bloom.BloomFilter>`. Stage0b consumes those part files through DistributedCache; the BloomFilter must have no false negatives for vids with frequency `>= 2`.

When changing any of the above, bump `companion-parent` minor version and notify all stage owners.

## 5. Job submission convention

Every Job extends `companion.job.AbstractCompanionJob` (a `Configured Tool`).

```
hadoop jar <stageX-jar> <fully-qualified-job-class> \
    <input-path> <output-path> \
    [-D companion.delta.t=600] [-D mapreduce.job.reduces=32] …
```

The first two positional args are input/output; everything else is config override.

Each stage jar is a **shaded fat jar**: `maven-shade-plugin` bundles `companion:common` (custom Writables, `CompanionConf`, `AbstractCompanionJob`) into it, while `hadoop-client` stays `provided` (supplied by the cluster). Submission therefore needs no `-libjars` and no extra `HADOOP_CLASSPATH` — test and prod submit identically.

`scripts/cluster_run.sh` additionally passes `-D companion.run.tag=<run_id>`; `AbstractCompanionJob` folds it into the YARN job name (`<JobName> [<run_id>]`) so a run's apps can be located and killed (`scripts/cluster_cancel.sh`, or Ctrl-C, which the launcher traps).

## 6. Dependencies between modules

```
common (R1)  →  stage0 (R2)  →  stage1 (R3)  →  stage2 (R4)  →  stage3 (R5)
                                                                  ↓
                                                  baseline (R5)  ←  cross-check
                                                  bench    (R6)  ←  metrics + counters
```

No back-edges. Common has zero internal deps (only Hadoop + slf4j). Stage modules import only `common`.
