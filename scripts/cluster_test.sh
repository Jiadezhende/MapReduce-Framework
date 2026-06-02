#!/usr/bin/env bash
# Remote single-stage isolation test on the real cluster.
#
# Mirrors the local StageXFixtureTest, but on real YARN/HDFS: stage the upstream
# golden fixture to an isolated HDFS path, run that one stage via `hadoop jar`
# with >1 reducer, decode both the cluster output and the golden through the same
# `hadoop fs -text`/`-cat`, and diff. This catches what LocalJobRunner cannot:
# multi-reducer partitioning, jar packaging / classpath, HDFS committer.
#
# Everything lands under /companion/test/<stage>-<ts>/ so runs never collide.
#
# Usage:
#   scripts/cluster_test.sh --stage {stage0|stage1|stage2|stage3}
#                           [--build] [--reducers N] [--keep] [--dry-run]
#                           [-Dkey=value ...]

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

usage() { sed -n '2,18p' "$0"; }

STAGE=""
DO_BUILD=false
REDUCERS=2
KEEP=false
DRY_RUN=false
EXTRA_CONF=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --stage)    STAGE="$2"; shift 2 ;;
        --build)    DO_BUILD=true; shift ;;
        --reducers) REDUCERS="$2"; shift 2 ;;
        --keep)     KEEP=true; shift ;;
        --dry-run)  DRY_RUN=true; shift ;;
        -D*)        EXTRA_CONF+=("$1"); shift ;;
        -h|--help)  usage; exit 0 ;;
        *)          echo "ERROR: unknown arg $1" >&2; usage; exit 2 ;;
    esac
done

FIXTURES="${LOCAL_DATA_DIR}/tests/data/fixtures"
MINI_CSV="${LOCAL_DATA_DIR}/tests/data/mini.csv"

# Per-stage contract: module, golden fixture, decode (text|cat), compare
# (multiset=sort | set=sort -u | ordered=cat). Input + job classes handled below.
# NOTE stage0: fixture is the precise ideal set; production Stage0 uses a BloomFilter and may
# emit a bounded number of FP-promoted extras. We assert superset + delta bound (not byte-equal).
# NOTE stage1: fixture expresses *ideal* Stage1 semantics (full loc grouping, no boundary loss).
# Production Stage1Job still ships the (loc, slot/2) partition design, so this compare is
# EXPECTED-FAIL until J1b lands. See docs/stage1-boundary-gap.md and docs/reference-semantics.md.
# Now fixed.
# NOTE stage2: this is a SINGLE-PASS isolation test (companion.stage2.rounds=1, the default).
# It asserts Stage2Job output == golden companions.csv byte-for-byte (as a set). Do NOT pass
# `-D companion.stage2.rounds=K` (K>1) here: pair-hash sharding makes one sub-job emit only the
# ~1/K of pairs whose mix(vidA,vidB)%K==round, so a single submission yields a strict subset and
# this diff FAILS spuriously. The K-round sharded path belongs to cluster_run.sh, which runs K
# sequential sub-jobs into companions/<phase>/r{k}/ and unions them (Stage3 reads recursively).
# That disjoint-partition union == single-pass equivalence is covered by Stage2JobTest's
# shardingPartitionsPairsDisjointlyAndUnionMatchesSingleRound.
case "${STAGE}" in
    stage0) MODULE=stage0; GOLDEN="filtered.seq";       DECODE=text; COMPARE=multiset ;;
    stage1) MODULE=stage1; GOLDEN="pair_loc_slot.seq";  DECODE=text; COMPARE=set ;;
    stage2) MODULE=stage2; GOLDEN="companions.csv";     DECODE=cat;  COMPARE=set; OUT_SUBDIR="" ;;
    stage3) MODULE=stage3; GOLDEN="companions.csv";     DECODE=cat;  COMPARE=ordered; OUT_SUBDIR="companions.csv" ;;
    "")     echo "ERROR: --stage is required" >&2; usage; exit 2 ;;
    *)      echo "ERROR: invalid --stage '${STAGE}' (expect stage0|stage1|stage2|stage3)" >&2; exit 2 ;;
esac

TS=$(date +%Y%m%d%H%M%S)
HDFS_TEST_ROOT="${HDFS_TEST_ROOT_BASE}/${STAGE}-${TS}"
REMOTE_SUBMIT_DIR="${REMOTE_SUBMIT_BASE}/test-${STAGE}-${TS}"
REMOTE_JAR_DIR="${REMOTE_SUBMIT_DIR}/jars"
REMOTE_GOLDEN="${REMOTE_SUBMIT_DIR}/golden/${GOLDEN}"
REMOTE_STAGE_JAR="${REMOTE_JAR_DIR}/${MODULE}.jar"

# local temp files; emptied here so the EXIT trap can reference them safely
OUT_TXT=""
GOLDEN_TXT=""

echo "stage     = ${STAGE}"
echo "master    = ${MASTER_HOST}"
echo "reducers  = ${REDUCERS}"
echo "hdfs test = ${HDFS_TEST_ROOT}"
echo "remote    = ${REMOTE_SUBMIT_DIR}"
echo

# run a command, or just echo it under --dry-run
run() {
    echo "+ $*"
    if [[ "${DRY_RUN}" == "false" ]]; then
        "$@"
    fi
}

# Best-effort teardown: local temps + isolated HDFS / remote staging dirs.
cleanup() {
    rm -f "${OUT_TXT}" "${GOLDEN_TXT}" 2>/dev/null || true
    if [[ "${KEEP}" == "true" ]]; then
        echo
        echo "--keep: left ${HDFS_TEST_ROOT} and ${REMOTE_SUBMIT_DIR} in place"
        return
    fi
    if [[ "${DRY_RUN}" == "false" ]]; then
        echo
        echo "+ cleanup ${HDFS_TEST_ROOT} ${REMOTE_SUBMIT_DIR}"
        ssh "${MASTER_HOST}" \
            "${HADOOP_BIN} fs -rm -r -f -skipTrash ${HDFS_TEST_ROOT} >/dev/null 2>&1 || true; rm -rf ${REMOTE_SUBMIT_DIR}" \
            || true
    fi
}
trap cleanup EXIT

# 1. Local build
if [[ "${DO_BUILD}" == "true" ]]; then
    run "mvn" -B -f "${LOCAL_DATA_DIR}/pom.xml" -DskipTests package
fi

# 2. Resolve the shaded stage jar (fat: bundles common's custom Writables)
resolve_jar() {
    if [[ "${DRY_RUN}" == "true" ]] && ! ls "${LOCAL_DATA_DIR}/$1/target/$1-"*.jar >/dev/null 2>&1; then
        echo "${LOCAL_DATA_DIR}/$1/target/$1-<version>.jar"
    else
        companion_jar "$1"
    fi
}
LOCAL_STAGE_JAR=$(resolve_jar "${MODULE}")

# 3. Prepare local input fixture for this stage
case "${STAGE}" in
    stage0) LOCAL_INPUT="${MINI_CSV}";                    INPUT_NAME="mini.csv" ;;
    stage1) LOCAL_INPUT="${FIXTURES}/filtered.seq";       INPUT_NAME="filtered.seq" ;;
    stage2) LOCAL_INPUT="${FIXTURES}/pair_loc_slot.seq";  INPUT_NAME="pair_loc_slot.seq" ;;
    stage3) LOCAL_INPUT="${FIXTURES}/companions.csv";     INPUT_NAME="companions.csv" ;;
esac
LOCAL_GOLDEN="${FIXTURES}/${GOLDEN}"

# 4. Stage jar + input + golden on master, then push input into isolated HDFS path
run ssh "${MASTER_HOST}" "mkdir -p ${REMOTE_JAR_DIR} ${REMOTE_SUBMIT_DIR}/golden ${REMOTE_SUBMIT_DIR}/in"
run scp "${LOCAL_STAGE_JAR}"  "${MASTER_HOST}:${REMOTE_STAGE_JAR}"
run scp "${LOCAL_GOLDEN}"     "${MASTER_HOST}:${REMOTE_GOLDEN}"
run scp "${LOCAL_INPUT}"      "${MASTER_HOST}:${REMOTE_SUBMIT_DIR}/in/${INPUT_NAME}"
run ssh "${MASTER_HOST}" \
    "${HADOOP_BIN} fs -mkdir -p ${HDFS_TEST_ROOT}/in && ${HADOOP_BIN} fs -put -f ${REMOTE_SUBMIT_DIR}/in/${INPUT_NAME} ${HDFS_TEST_ROOT}/in/${INPUT_NAME}"

# 5. Submit the stage. The shaded stage jar carries common's custom Writables,
#    so submission matches prod exactly: no -libjars, no extra classpath.
submit() {
    local jobclass="$1"; shift
    local in="$1"; shift
    local out="$1"; shift   # remaining args = stage-specific -D overrides
    local extra=""
    if (( ${#EXTRA_CONF[@]} > 0 )); then extra=" ${EXTRA_CONF[*]}"; fi
    run ssh "${MASTER_HOST}" \
        "${HADOOP_BIN} jar ${REMOTE_STAGE_JAR} ${jobclass} ${in} ${out} $*${extra}"
}

RED_CONF="-D companion.stage0a.reducers=${REDUCERS} -D companion.stage1.reducers=${REDUCERS} -D companion.stage2.reducers=${REDUCERS} -D companion.stage3.reducers=${REDUCERS}"
case "${STAGE}" in
    stage0)
        submit companion.stage0.Stage0aFreqJob \
            "${HDFS_TEST_ROOT}/in" "${HDFS_TEST_ROOT}/vid_freq" "${RED_CONF}"
        submit companion.stage0.Stage0bFilterJob \
            "${HDFS_TEST_ROOT}/in" "${HDFS_TEST_ROOT}/out" \
            "-D companion.vid_freq.path=${HDFS_TEST_ROOT}/vid_freq" ;;
    stage1)
        submit companion.stage1.Stage1Job \
            "${HDFS_TEST_ROOT}/in" "${HDFS_TEST_ROOT}/out" "${RED_CONF}" ;;
    stage2)
        # Single-pass isolation: companion.stage2.rounds defaults to 1, so don't
        # pass it. Do NOT pass -D companion.stage2.rounds=K (K>1) — see NOTE above.
        submit companion.stage2.Stage2Job \
            "${HDFS_TEST_ROOT}/in" "${HDFS_TEST_ROOT}/out" "${RED_CONF}" ;;
    stage3)
        submit companion.stage3.Stage3SortJob \
            "${HDFS_TEST_ROOT}/in" "${HDFS_TEST_ROOT}/out" "${RED_CONF}" ;;
esac

# 6. Decode both sides through the same reader, then diff.
#    Custom Writables (RecordWritable/PairKey/LocSlotWritable) have deterministic
#    toString(), so `fs -text` of the .seq is self-consistent across both sides.
decode() {  # <hdfs-or-file-path> -> remote command emitting canonical lines
    local path="$1"
    if [[ "${DECODE}" == "text" ]]; then
        # fs -text needs the custom Writable classes; the shaded stage jar has them.
        echo "HADOOP_CLASSPATH=${REMOTE_STAGE_JAR} ${HADOOP_BIN} fs -text ${path}"
    else
        echo "${HADOOP_BIN} fs -cat ${path}"
    fi
}
post() {  # comparison normalizer for the captured lines
    # Strip CR first: a Windows checkout (core.autocrlf=true) leaves the .csv
    # golden with CRLF endings, but Hadoop TextOutputFormat emits LF. Without
    # this, diff -u flags all rows as changed even when the data is identical
    # (the byte-for-byte difference is just a trailing \r on the golden side).
    case "${COMPARE}" in
        multiset) echo "tr -d '\\r' | sort" ;;
        set)      echo "tr -d '\\r' | sort -u" ;;
        ordered)  echo "tr -d '\\r' | cat" ;;
    esac
}

if [[ -n "${OUT_SUBDIR:-}" ]]; then
    OUT_CMD="$(decode "${HDFS_TEST_ROOT}/out/${OUT_SUBDIR}/part-*") | $(post)"
else
    OUT_CMD="$(decode "${HDFS_TEST_ROOT}/out/part-*") | $(post)"
fi
GOLDEN_CMD="$(decode "file://${REMOTE_GOLDEN}") | $(post)"

echo
echo "+ compare (decode=${DECODE}, compare=${COMPARE})"
echo "+ cluster: ssh ${MASTER_HOST} \"${OUT_CMD}\""
echo "+ golden : ssh ${MASTER_HOST} \"${GOLDEN_CMD}\""

if [[ "${DRY_RUN}" == "true" ]]; then
    echo "(dry-run: skipping submit/compare)"
    exit 0
fi

OUT_TXT=$(mktemp -t companion-clustertest-out.XXXXXX)
GOLDEN_TXT=$(mktemp -t companion-clustertest-golden.XXXXXX)
ssh "${MASTER_HOST}" "${OUT_CMD}"    >"${OUT_TXT}"
ssh "${MASTER_HOST}" "${GOLDEN_CMD}" >"${GOLDEN_TXT}"

echo
# Stage 0 uses a superset assertion: production Stage0 may bloom-FP-promote extra rows
# (production output ⊇ fixture; bounded delta). See docs/reference-semantics.md §7.
if [[ "${STAGE}" == "stage0" ]]; then
    G=$(wc -l <"${GOLDEN_TXT}" | tr -d ' ')
    C=$(wc -l <"${OUT_TXT}" | tr -d ' ')
    MISSING=$(comm -23 "${GOLDEN_TXT}" "${OUT_TXT}" | wc -l | tr -d ' ')
    DELTA=$((C - G))
    BLOOM_FP_DELTA_BOUND=100
    if [[ "${MISSING}" -ne 0 ]]; then
        echo "FAIL stage0: ${MISSING} golden record(s) missing from cluster output"
        echo "  golden lines = ${G}, cluster lines = ${C}"
        echo "  (fixture is the precise ideal set; cluster must be a superset)"
        comm -23 "${GOLDEN_TXT}" "${OUT_TXT}" | head -5 | sed 's/^/    /'
        exit 1
    fi
    if [[ "${DELTA}" -lt 0 ]]; then
        echo "FAIL stage0: cluster output has fewer rows than golden (delta=${DELTA})"
        exit 1
    fi
    if [[ "${DELTA}" -gt "${BLOOM_FP_DELTA_BOUND}" ]]; then
        echo "FAIL stage0: bloom FP delta ${DELTA} exceeds bound ${BLOOM_FP_DELTA_BOUND}"
        echo "  Either Stage0Bloom params shrank, mini.csv grew, or something else changed."
        echo "  Inspect: comm -13 golden cluster"
        exit 1
    fi
    echo "PASS stage0: cluster output is a superset of golden ${GOLDEN}"
    echo "  golden lines = ${G}, cluster lines = ${C}, delta = ${DELTA} (bound ${BLOOM_FP_DELTA_BOUND})"
    exit 0
fi

if diff -u "${GOLDEN_TXT}" "${OUT_TXT}"; then
    echo "PASS ${STAGE}: cluster output matches golden ${GOLDEN} ($(wc -l <"${OUT_TXT}" | tr -d ' ') records)"
    exit 0
else
    G=$(wc -l <"${GOLDEN_TXT}" | tr -d ' ')
    C=$(wc -l <"${OUT_TXT}" | tr -d ' ')
    echo "FAIL ${STAGE}: cluster output differs from golden ${GOLDEN}"
    echo "  golden lines = ${G}, cluster lines = ${C}"
    echo "  (above: --- golden / +++ cluster)"
    exit 1
fi
