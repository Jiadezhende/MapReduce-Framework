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
: "${YARN_QUEUE:=default}"
: "${STAGE1_REDUCERS_1D:=8}"
: "${STAGE1_REDUCERS_7D:=32}"
: "${STAGE1_REDUCERS_31D:=128}"
: "${STAGE2_REDUCERS_1D:=8}"
: "${STAGE2_REDUCERS_7D:=32}"
: "${STAGE2_REDUCERS_31D:=128}"

export COMPANION_ROOT HADOOP_CONF_DIR HADOOP_BIN LOCAL_DATA_DIR YARN_QUEUE
export MASTER_HOST HDFS_INPUT_ROOT HDFS_RUNS_ROOT HDFS_TEST_ROOT_BASE REMOTE_SUBMIT_BASE
export STAGE1_REDUCERS_1D STAGE1_REDUCERS_7D STAGE1_REDUCERS_31D
export STAGE2_REDUCERS_1D STAGE2_REDUCERS_7D STAGE2_REDUCERS_31D

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
