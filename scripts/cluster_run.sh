#!/usr/bin/env bash
# Local-driven, non-login cluster submission for the companion pipeline.
#
# Developer machine builds the stage jars locally, scp's them to the master,
# and triggers `hadoop jar` via non-interactive ssh. The master never sees
# project source or scripts; only stage jars under a per-run temp dir.
#
# All HDFS outputs land in /tmp/${USER}/companion/runs/<run_id>/, so concurrent
# developers do not collide on the shared /companion/ tree.
#
# Usage:
#   scripts/cluster_run.sh --days {1|7|31} [--build]
#                          [--from stageX] [--until stageY] [--stage stageX]
#                          [--run-id <id>] [--dry-run] [-Dkey=value ...]

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

usage() { sed -n '2,15p' "$0"; }

PHASE=""
DO_BUILD=false
FROM_STAGE="stage0"
UNTIL_STAGE="stage3"
RUN_ID=""
DRY_RUN=false
EXTRA_CONF=()

stage_index() {
    case "$1" in
        stage0) echo 0 ;;
        stage1) echo 1 ;;
        stage2) echo 2 ;;
        stage3) echo 3 ;;
        *) echo "ERROR: invalid stage '$1' (expect stage0|stage1|stage2|stage3)" >&2; exit 2 ;;
    esac
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --days)
            case "$2" in
                1)  PHASE="1d" ;;
                7)  PHASE="7d" ;;
                31) PHASE="31d" ;;
                *)  echo "ERROR: --days must be 1, 7, or 31" >&2; exit 2 ;;
            esac
            shift 2 ;;
        --build)      DO_BUILD=true; shift ;;
        --from)       FROM_STAGE="$2"; shift 2 ;;
        --until)      UNTIL_STAGE="$2"; shift 2 ;;
        --stage)      FROM_STAGE="$2"; UNTIL_STAGE="$2"; shift 2 ;;
        --run-id)     RUN_ID="$2"; shift 2 ;;
        --dry-run)    DRY_RUN=true; shift ;;
        -D*)          EXTRA_CONF+=("$1"); shift ;;
        -h|--help)    usage; exit 0 ;;
        *)            echo "ERROR: unknown arg $1" >&2; usage; exit 2 ;;
    esac
done

if [[ -z "${PHASE}" ]]; then
    echo "ERROR: --days is required" >&2
    exit 2
fi

FROM_IDX=$(stage_index "${FROM_STAGE}")
UNTIL_IDX=$(stage_index "${UNTIL_STAGE}")
if (( FROM_IDX > UNTIL_IDX )); then
    echo "ERROR: --from ${FROM_STAGE} comes after --until ${UNTIL_STAGE}" >&2
    exit 2
fi

if [[ -z "${RUN_ID}" ]]; then
    git_sha=$(cd "${LOCAL_DATA_DIR}" && git rev-parse --short HEAD 2>/dev/null || echo "nogit")
    RUN_ID="${USER}-${git_sha}-$(date +%Y%m%d%H%M%S)"
fi

case "${PHASE}" in
    1d)  S1_RED="${STAGE1_REDUCERS_1D}";  S2_RED="${STAGE2_REDUCERS_1D}"  ;;
    7d)  S1_RED="${STAGE1_REDUCERS_7D}";  S2_RED="${STAGE2_REDUCERS_7D}"  ;;
    31d) S1_RED="${STAGE1_REDUCERS_31D}"; S2_RED="${STAGE2_REDUCERS_31D}" ;;
esac

HDFS_RUN_ROOT="${HDFS_RUN_ROOT_BASE}/${RUN_ID}"
REMOTE_SUBMIT_DIR="${REMOTE_SUBMIT_BASE}/${RUN_ID}"
REMOTE_JAR_DIR="${REMOTE_SUBMIT_DIR}/jars"

echo "run_id        = ${RUN_ID}"
echo "phase         = ${PHASE}"
echo "stages        = ${FROM_STAGE}..${UNTIL_STAGE}"
echo "master        = ${MASTER_HOST}"
echo "hdfs run root = ${HDFS_RUN_ROOT}"
echo "remote submit = ${REMOTE_SUBMIT_DIR}"
echo

# run a command, or just echo it under --dry-run
run() {
    echo "+ $*"
    if [[ "${DRY_RUN}" == "false" ]]; then
        "$@"
    fi
}

# 1. Local build
if [[ "${DO_BUILD}" == "true" ]]; then
    run "mvn" -B -f "${LOCAL_DATA_DIR}/pom.xml" -DskipTests package
fi

# 2. Resolve jars for the stages in the [from, until] window. stage0 lives in
#    one jar that holds both Stage0aFreqJob and Stage0bFilterJob.
declare -a NEEDED_MODULES
for idx in $(seq "${FROM_IDX}" "${UNTIL_IDX}"); do
    NEEDED_MODULES+=("stage${idx}")
done

declare -A MODULE_JAR
for module in "${NEEDED_MODULES[@]}"; do
    if [[ "${DRY_RUN}" == "true" ]] && ! ls "${LOCAL_DATA_DIR}/${module}/target/${module}-"*.jar >/dev/null 2>&1; then
        MODULE_JAR["${module}"]="${LOCAL_DATA_DIR}/${module}/target/${module}-<version>.jar"
    else
        MODULE_JAR["${module}"]=$(companion_jar "${module}")
    fi
done

# 3. Stage area on master + scp jars
run ssh "${MASTER_HOST}" "mkdir -p ${REMOTE_JAR_DIR}"
for module in "${NEEDED_MODULES[@]}"; do
    local_jar="${MODULE_JAR[${module}]}"
    run scp "${local_jar}" "${MASTER_HOST}:${REMOTE_JAR_DIR}/${module}.jar"
done

# 4. Upstream dependency checks
hdfs_exists() {
    # returns 0 if the path exists in HDFS
    local path="$1"
    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "+ ssh ${MASTER_HOST} ${HADOOP_BIN} fs -test -e ${path}"
        return 0
    fi
    ssh "${MASTER_HOST}" "${HADOOP_BIN} fs -test -e ${path}"
}

if (( FROM_IDX == 0 )); then
    if ! hdfs_exists "${HDFS_INPUT_ROOT}/${PHASE}.csv"; then
        echo "ERROR: raw input missing: ${HDFS_INPUT_ROOT}/${PHASE}.csv" >&2
        echo "hint:  run scripts/upload_to_hdfs.sh ${PHASE}.csv first" >&2
        exit 3
    fi
else
    case "${FROM_STAGE}" in
        stage1) need="${HDFS_RUN_ROOT}/filtered/${PHASE}" ;;
        stage2) need="${HDFS_RUN_ROOT}/pair_loc_slot/${PHASE}" ;;
        stage3) need="${HDFS_RUN_ROOT}/companions/${PHASE}" ;;
    esac
    if ! hdfs_exists "${need}"; then
        echo "ERROR: missing upstream: ${need}" >&2
        echo "hint:  rerun with --from stage0 or pass an existing --run-id" >&2
        exit 3
    fi
fi

# 5. Submit each Job in the window via non-interactive ssh.
submit() {
    local module="$1"; shift
    local class="$1"; shift
    local cmd="${HADOOP_BIN} jar ${REMOTE_JAR_DIR}/${module}.jar ${class} $*"
    if (( ${#EXTRA_CONF[@]} > 0 )); then
        cmd="${cmd} ${EXTRA_CONF[*]}"
    fi
    run ssh "${MASTER_HOST}" "${cmd}"
}

if (( FROM_IDX <= 0 && UNTIL_IDX >= 0 )); then
    submit stage0 companion.stage0.Stage0aFreqJob \
        "${HDFS_INPUT_ROOT}/${PHASE}.csv" \
        "${HDFS_RUN_ROOT}/vid_freq/${PHASE}"
    submit stage0 companion.stage0.Stage0bFilterJob \
        "${HDFS_INPUT_ROOT}/${PHASE}.csv" \
        "${HDFS_RUN_ROOT}/filtered/${PHASE}" \
        "-D companion.vid_freq.path=${HDFS_RUN_ROOT}/vid_freq/${PHASE}"
fi

if (( FROM_IDX <= 1 && UNTIL_IDX >= 1 )); then
    submit stage1 companion.stage1.Stage1Job \
        "${HDFS_RUN_ROOT}/filtered/${PHASE}" \
        "${HDFS_RUN_ROOT}/pair_loc_slot/${PHASE}" \
        "-D mapreduce.job.reduces=${S1_RED}"
fi

if (( FROM_IDX <= 2 && UNTIL_IDX >= 2 )); then
    submit stage2 companion.stage2.Stage2Job \
        "${HDFS_RUN_ROOT}/pair_loc_slot/${PHASE}" \
        "${HDFS_RUN_ROOT}/companions/${PHASE}" \
        "-D mapreduce.job.reduces=${S2_RED}"
fi

if (( FROM_IDX <= 3 && UNTIL_IDX >= 3 )); then
    submit stage3 companion.stage3.Stage3SortJob \
        "${HDFS_RUN_ROOT}/companions/${PHASE}" \
        "${HDFS_RUN_ROOT}/final/${PHASE}"
fi

echo
echo "Done. run_id=${RUN_ID}"
echo "Inspect with:"
echo "  scripts/cluster_status.sh ${RUN_ID}"
echo "  scripts/cluster_head.sh ${RUN_ID} final ${PHASE}"
