# shellcheck shell=bash
# Centralized environment for the companion pipeline.
# Sourced by every other script in scripts/.

: "${COMPANION_ROOT:=/companion}"
: "${HADOOP_CONF_DIR:=/etc/hadoop/conf}"
: "${HADOOP_BIN:=hadoop}"
: "${LOCAL_DATA_DIR:=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Non-login submit: every cluster operation goes through `ssh ${MASTER_HOST}`.
# Override MASTER_HOST in ~/.ssh/config or via env if your alias differs.
: "${MASTER_HOST:=master}"
: "${HDFS_INPUT_ROOT:=${COMPANION_ROOT}/input/raw}"
# Single /companion tree: prod runs under runs/<run_id>, isolation tests under
# test/<stage>-<ts>. run_id/timestamp provide isolation, so no per-user segment.
: "${HDFS_RUNS_ROOT:=${COMPANION_ROOT}/runs}"
: "${HDFS_TEST_ROOT_BASE:=${COMPANION_ROOT}/test}"
# Master-side local staging for jars (not HDFS).
: "${REMOTE_SUBMIT_BASE:=/tmp/companion/submit}"

# Submit-side knobs. Override in a per-user wrapper if needed.
# A single per-phase reducer count drives stage0a/1/2/3 in cluster_run.sh —
# scale-by-data-volume, not per-stage. cluster_test.sh has its own --reducers.
: "${YARN_QUEUE:=default}"
: "${REDUCERS_1D:=8}"
: "${REDUCERS_7D:=32}"
: "${REDUCERS_31D:=32}"

# Per-phase container + spill tuning.
# Default 7d sizing fits the 16 GB container pool; 31d needs smaller per-container
# memory to fit more concurrent reducers into the post-master-NM 20 GB pool
# (5 → 9 concurrent), plus a bigger map sort buffer so Stage2 nm-local-dir peak
# stays under the 95% disk-health threshold. See docs/space-optimization.md §8.2.
: "${TUNE_1D:=}"
: "${TUNE_7D:=}"
: "${TUNE_31D:=-D mapreduce.map.memory.mb=1536 -D mapreduce.map.java.opts=-Xmx1024m -D mapreduce.reduce.memory.mb=2048 -D mapreduce.reduce.java.opts=-Xmx1536m -D mapreduce.task.io.sort.mb=400 -D companion.hll.threshold=100000}"

export COMPANION_ROOT HADOOP_CONF_DIR HADOOP_BIN LOCAL_DATA_DIR YARN_QUEUE
export MASTER_HOST HDFS_INPUT_ROOT HDFS_RUNS_ROOT HDFS_TEST_ROOT_BASE REMOTE_SUBMIT_BASE
export REDUCERS_1D REDUCERS_7D REDUCERS_31D TUNE_1D TUNE_7D TUNE_31D

# Resolve the freshly-built shaded jar for a given module.
companion_jar() {
    local module="$1"
    local jar
    jar=$(ls "${LOCAL_DATA_DIR}/${module}/target/${module}-"*.jar 2>/dev/null | head -n1)
    if [[ -z "${jar}" ]]; then
        echo "ERROR: no jar found under ${module}/target/. Did you run 'mvn package'?" >&2
        return 1
    fi
    echo "${jar}"
}

# Kill every RUNNING/ACCEPTED YARN app whose name carries this run_id tag.
# Jobs tag themselves via `-D companion.run.tag=<run_id>`, which AbstractCompanionJob
# folds into the YARN job name as "<JobName> [<run_id>]".
cancel_run() {
    local run_id="$1"
    local apps
    apps=$(ssh "${MASTER_HOST}" \
        "${HADOOP_BIN%hadoop}yarn application -appStates RUNNING,ACCEPTED,SUBMITTED -list 2>/dev/null" \
        | grep -F "[${run_id}]" | awk '{print $1}' || true)
    if [[ -z "${apps}" ]]; then
        echo "no running YARN apps for run_id=${run_id}"
        return 0
    fi
    local app
    for app in ${apps}; do
        echo "+ yarn application -kill ${app}"
        ssh "${MASTER_HOST}" "${HADOOP_BIN%hadoop}yarn application -kill ${app}" || true
    done
}
