#!/usr/bin/env bash
# Local-driven, non-login cluster submission for the companion pipeline.
#
# Developer machine builds the stage jars locally, scp's them to the master,
# and triggers `hadoop jar` via non-interactive ssh. The master never sees
# project source or scripts; only stage jars under a per-run temp dir.
#
# All HDFS outputs land in /companion/runs/<run_id>/, isolated by run_id so
# concurrent runs do not collide.
#
# Resumable: each stage is skipped when its output already carries a Hadoop
# _SUCCESS marker, so re-running with the same --run-id continues from the
# breakpoint. Interruptible: Ctrl-C (or --until early exit) kills this run's
# YARN apps via the run_id tag instead of orphaning them.
#
# Usage:
#   scripts/cluster_run.sh --days {1|7|31} [--build]
#                          [--from stageX] [--until stageY] [--stage stageX]
#                          [--run-id <id>] [--force] [--dry-run] [-Dkey=value ...]

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

usage() { sed -n '2,21p' "$0"; }

PHASE=""
DO_BUILD=false
FROM_STAGE="stage0"
UNTIL_STAGE="stage3"
RUN_ID=""
DRY_RUN=false
FORCE=false
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
        --force)      FORCE=true; shift ;;
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
    # $USER isn't set on Windows/Git Bash (it uses $USERNAME); fall back so
    # `set -u` doesn't abort the run.
    RUN_ID="${USER:-${USERNAME:-user}}-${git_sha}-$(date +%Y%m%d%H%M%S)"
fi

case "${PHASE}" in
    1d)  RED="${REDUCERS_1D}";  TUNE="${TUNE_1D}";  S2_ROUNDS="${STAGE2_ROUNDS_1D}"  ;;
    7d)  RED="${REDUCERS_7D}";  TUNE="${TUNE_7D}";  S2_ROUNDS="${STAGE2_ROUNDS_7D}"  ;;
    31d) RED="${REDUCERS_31D}"; TUNE="${TUNE_31D}"; S2_ROUNDS="${STAGE2_ROUNDS_31D}" ;;
esac
RED_CONF="-D companion.stage0a.reducers=${RED} -D companion.stage1.reducers=${RED} -D companion.stage2.reducers=${RED} -D companion.stage3.reducers=${RED} ${TUNE}"

HDFS_RUN_ROOT="${HDFS_RUNS_ROOT}/${RUN_ID}"
REMOTE_SUBMIT_DIR="${REMOTE_SUBMIT_BASE}/${RUN_ID}"
REMOTE_JAR_DIR="${REMOTE_SUBMIT_DIR}/jars"

# Interruptible: on Ctrl-C / TERM, kill this run's YARN apps (tagged with RUN_ID)
# rather than leaving them orphaned when the local ssh channel dies.
on_interrupt() {
    echo
    echo "interrupted — cancelling YARN apps for run_id=${RUN_ID}"
    cancel_run "${RUN_ID}"
    exit 130
}
if [[ "${DRY_RUN}" == "false" ]]; then
    trap on_interrupt INT TERM
fi

echo "run_id        = ${RUN_ID}"
echo "phase         = ${PHASE}"
echo "stages        = ${FROM_STAGE}..${UNTIL_STAGE}"
echo "force rerun   = ${FORCE}"
echo "master        = ${MASTER_HOST}"
echo "hdfs run root = ${HDFS_RUN_ROOT}"
echo "remote submit = ${REMOTE_SUBMIT_DIR}"
echo

# Pin Java to 1.8 before any build or submit.
assert_java8 "${DRY_RUN}"
echo

# The master subnet rate-limits by source IP — a burst of fresh ssh/scp
# DROP-bans the whole IP for ~5 min. The setup phase below (mkdir + scp×N +
# HDFS checks + prepare_out) is exactly such a burst, so its idempotent calls
# go through remote_ssh/remote_scp (env.sh), which throttle and retry-on-ban.
# The long-running `hadoop jar` submits stay plain ssh: they're minutes apart
# (no burst) and must NOT auto-retry — a re-submit would duplicate the YARN app.

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

# 2. Enumerate the stage modules in the [from, until] window. stage0 lives in
#    one jar that holds both Stage0aFreqJob and Stage0bFilterJob.
declare -a NEEDED_MODULES
for idx in $(seq "${FROM_IDX}" "${UNTIL_IDX}"); do
    NEEDED_MODULES+=("stage${idx}")
done

# 3. Stage area on master + scp each stage jar (resolved on demand, so no
#    bash-4 associative array is needed — keeps this runnable under macOS bash 3.2).
run remote_ssh "mkdir -p ${REMOTE_JAR_DIR}"
for module in "${NEEDED_MODULES[@]}"; do
    if [[ "${DRY_RUN}" == "true" ]] && ! ls "${LOCAL_DATA_DIR}/${module}/target/${module}-"*.jar >/dev/null 2>&1; then
        local_jar="${LOCAL_DATA_DIR}/${module}/target/${module}-<version>.jar"
    else
        local_jar=$(companion_jar "${module}")
    fi
    run remote_scp "${local_jar}" "${REMOTE_JAR_DIR}/${module}.jar"
done

# 4. Upstream dependency checks
hdfs_exists() {
    # returns 0 if the path exists in HDFS
    local path="$1"
    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "+ ssh ${MASTER_HOST} ${HADOOP_BIN} fs -test -e ${path}"
        return 0
    fi
    remote_ssh "${HADOOP_BIN} fs -test -e ${path}"
}

# returns 0 if <out_dir>/_SUCCESS exists, i.e. that stage already completed.
# --force always reports "not done"; dry-run reports "not done" so the plan
# shows the full submit sequence.
stage_done() {
    local out="$1"
    [[ "${FORCE}" == "true" ]] && return 1
    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "+ ssh ${MASTER_HOST} ${HADOOP_BIN} fs -test -e ${out}/_SUCCESS"
        return 1
    fi
    remote_ssh "${HADOOP_BIN} fs -test -e ${out}/_SUCCESS" 2>/dev/null
}

# Clear any previous/partial output dir so Hadoop accepts the (re)run. Called
# right before (re)submitting a stage whose _SUCCESS is absent (failed run) or
# bypassed (--force).
prepare_out() {
    local out="$1"
    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "+ ssh ${MASTER_HOST} ${HADOOP_BIN} fs -rm -r -f ${out}  # if exists"
        return 0
    fi
    if remote_ssh "${HADOOP_BIN} fs -test -e ${out}" 2>/dev/null; then
        run remote_ssh "${HADOOP_BIN} fs -rm -r -f -skipTrash ${out}"
    fi
}

if (( FROM_IDX == 0 )); then
    if ! hdfs_exists "${HDFS_INPUT_ROOT}/${PHASE}.csv"; then
        echo "ERROR: raw input missing: ${HDFS_INPUT_ROOT}/${PHASE}.csv" >&2
        echo "hint:  upload it first, e.g.:" >&2
        echo "         scp ${PHASE}.csv ${MASTER_HOST}:/tmp/ && ssh ${MASTER_HOST} \"${HADOOP_BIN} fs -put -f /tmp/${PHASE}.csv ${HDFS_INPUT_ROOT}/${PHASE}.csv\"" >&2
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
# Hadoop's GenericOptionsParser uses stopAtNonOption=true: any -D placed after
# a positional arg is silently dropped. We split caller args into -D options
# vs. positionals and always emit -D first.
submit() {
    local module="$1"; shift
    local class="$1"; shift
    local d_opts="-D companion.run.tag=${RUN_ID}"
    local positional=()
    for arg in "$@"; do
        if [[ "${arg}" == -D* ]]; then
            d_opts="${d_opts} ${arg}"
        else
            positional+=("${arg}")
        fi
    done
    if (( ${#EXTRA_CONF[@]} > 0 )); then
        d_opts="${d_opts} ${EXTRA_CONF[*]}"
    fi
    local cmd="${HADOOP_BIN} jar ${REMOTE_JAR_DIR}/${module}.jar ${class} ${d_opts} ${positional[*]}"
    run ssh "${MASTER_HOST}" "${cmd}"
}

if (( FROM_IDX <= 0 && UNTIL_IDX >= 0 )); then
    s0_freq="${HDFS_RUN_ROOT}/vid_freq/${PHASE}"
    s0_out="${HDFS_RUN_ROOT}/filtered/${PHASE}"
    if stage_done "${s0_freq}" && stage_done "${s0_out}"; then
        echo "skip stage0 (already _SUCCESS)"
    else
        prepare_out "${s0_freq}"
        prepare_out "${s0_out}"
        submit stage0 companion.stage0.Stage0aFreqJob \
            "${HDFS_INPUT_ROOT}/${PHASE}.csv" \
            "${s0_freq}" \
            "${RED_CONF}"
        submit stage0 companion.stage0.Stage0bFilterJob \
            "${HDFS_INPUT_ROOT}/${PHASE}.csv" \
            "${s0_out}" \
            "-D companion.vid_freq.path=${s0_freq}"
    fi
fi

if (( FROM_IDX <= 1 && UNTIL_IDX >= 1 )); then
    s1_out="${HDFS_RUN_ROOT}/pair_loc_slot/${PHASE}"
    if stage_done "${s1_out}"; then
        echo "skip stage1 (already _SUCCESS)"
    else
        prepare_out "${s1_out}"
        submit stage1 companion.stage1.Stage1Job \
            "${HDFS_RUN_ROOT}/filtered/${PHASE}" \
            "${s1_out}" \
            "${RED_CONF}"
    fi
fi

if (( FROM_IDX <= 2 && UNTIL_IDX >= 2 )); then
    s2_base="${HDFS_RUN_ROOT}/companions/${PHASE}"
    s2_in="${HDFS_RUN_ROOT}/pair_loc_slot/${PHASE}"
    if (( S2_ROUNDS <= 1 )); then
        # Single pass: flat output companions/<phase>/, unchanged layout.
        if stage_done "${s2_base}"; then
            echo "skip stage2 (already _SUCCESS)"
        else
            prepare_out "${s2_base}"
            submit stage2 companion.stage2.Stage2Job "${s2_in}" "${s2_base}" "${RED_CONF}" "${OUT_COMPRESS}"
        fi
    else
        # Pair-hash sharding (caps per-node shuffle peak at 1/S2_ROUNDS): K
        # sequential sub-jobs, each into companions/<phase>/r{k}/. Each round has
        # its own _SUCCESS so a re-run resumes at the first unfinished round.
        # Stage3 reads them via input.dir.recursive=true.
        for (( r = 0; r < S2_ROUNDS; r++ )); do
            s2_out="${s2_base}/r${r}"
            if stage_done "${s2_out}"; then
                echo "skip stage2 round ${r}/${S2_ROUNDS} (already _SUCCESS)"
            else
                prepare_out "${s2_out}"
                submit stage2 companion.stage2.Stage2Job "${s2_in}" "${s2_out}" \
                    "${RED_CONF}" \
                    "${OUT_COMPRESS}" \
                    "-D companion.stage2.rounds=${S2_ROUNDS}" \
                    "-D companion.stage2.round=${r}"
            fi
        done
    fi
fi

if (( FROM_IDX <= 3 && UNTIL_IDX >= 3 )); then
    s3_out="${HDFS_RUN_ROOT}/final/${PHASE}"
    if stage_done "${s3_out}"; then
        echo "skip stage3 (already _SUCCESS)"
    else
        prepare_out "${s3_out}"
        submit stage3 companion.stage3.Stage3SortJob \
            "${HDFS_RUN_ROOT}/companions/${PHASE}" \
            "${s3_out}" \
            "${RED_CONF}" \
            "${OUT_COMPRESS}"
    fi
fi

echo
echo "Done. run_id=${RUN_ID}"
echo "Inspect with:"
echo "  scripts/cluster_status.sh ${RUN_ID}"
echo "  scripts/cluster_fetch.sh ${RUN_ID} ${PHASE}   # pull TopN + metrics to ./out/${RUN_ID}/"
echo "Resume (skips completed stages):"
echo "  scripts/cluster_run.sh --days ${PHASE%d} --run-id ${RUN_ID}"
echo "Cancel running jobs:"
echo "  scripts/cluster_cancel.sh ${RUN_ID}"
