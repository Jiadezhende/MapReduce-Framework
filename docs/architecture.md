# Architecture — Companion Vehicle Mining

This document is the contract between modules. R1 maintains it; every change requires sign-off from owners of affected stages. Full role allocation lives in [roles.md](roles.md).

## 1. HDFS layout

All paths are relative to `${COMPANION_ROOT}` (default `/companion`).

```
${COMPANION_ROOT}/
├── input/raw/                       # Raw 31d.csv chunks (owner: R1 upload_to_hdfs.sh)
├── input/{1d,7d,31d}/               # Per-phase slices (owner: R2 split_by_day.sh)
├── filtered/{1d,7d,31d}/            # SequenceFile after J0 (owner: R2)
├── pair_loc_slot/{1d,7d,31d}/       # SequenceFile after J1 (owner: R3)
├── companions/{1d,7d,31d}/          # CSV after J2 threshold filter (owner: R4)
├── final/{1d,7d,31d}/               # Sorted CSV + TopN + metrics (owner: R5)
│   ├── companions.csv
│   ├── top_n.csv
│   └── _metrics.json
└── profile/                         # Long-tail histograms (owner: R2)
```

`{phase} ∈ {1d, 7d, 31d}` tags the dataset slice; the same pipeline runs at three scales.

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

When changing any of the above, bump `companion-parent` minor version and notify all stage owners.

## 5. Job submission convention

Every Job extends `companion.job.AbstractCompanionJob` (a `Configured Tool`).

```
hadoop jar <stageX-jar> <fully-qualified-job-class> \
    <input-path> <output-path> \
    [-D companion.delta.t=600] [-D mapreduce.job.reduces=32] …
```

The first two positional args are input/output; everything else is config override.

## 6. Dependencies between modules

```
common (R1)  →  stage0 (R2)  →  stage1 (R3)  →  stage2 (R4)  →  stage3 (R5)
                                                                  ↓
                                                  baseline (R5)  ←  cross-check
                                                  bench    (R6)  ←  metrics + counters
```

No back-edges. Common has zero internal deps (only Hadoop + slf4j). Stage modules import only `common`.
