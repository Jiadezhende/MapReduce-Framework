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

# --- Rate-limit-safe remote command wrappers ----------------------------------
# The master subnet rate-limits by source IP: a burst of fresh connections
# DROP-bans the whole IP for ~5 min (ICMP included). On Windows/Git-MSYS, ssh
# multiplexing is broken (mux socket falls back to a fresh connection every
# time), so every ssh/scp is effectively a new connection and a full run's
# setup phase can trip the limiter. These wrappers (a) throttle — space
# connections out — and (b) retry on a *connection* failure (waiting out a ban)
# without retrying on a command-level non-zero exit.
#
# Tunable via env. SSH_THROTTLE_SECS spaces successive connections; the observed
# ban clears in ~5 min, so the retry budget (waits × retries) is sized to ride
# one ban out.
: "${SSH_THROTTLE_SECS:=1.5}"
: "${SSH_MAX_RETRIES:=6}"
: "${SSH_BAN_WAIT_SECS:=120}"
export SSH_THROTTLE_SECS SSH_MAX_RETRIES SSH_BAN_WAIT_SECS

# remote_ssh: ssh to the master with throttle + retry-on-connection-failure.
# ssh exits 255 on its *own* errors (connect timeout / reset / ban); any other
# exit code is the remote command's own status (e.g. `hadoop fs -test -e`
# returns 1 for "missing") and is returned as-is, NOT retried.
remote_ssh() {
    local tries=0 rc
    while :; do
        sleep "${SSH_THROTTLE_SECS}"
        ssh "${MASTER_HOST}" "$@"
        rc=$?
        (( rc != 255 )) && return "${rc}"
        tries=$(( tries + 1 ))
        if (( tries >= SSH_MAX_RETRIES )); then
            echo "ERROR: ssh to ${MASTER_HOST} failed ${tries}× (rc=255); giving up" >&2
            return 255
        fi
        echo "  ssh connection failed (rc=255) — likely rate-limit ban; waiting ${SSH_BAN_WAIT_SECS}s then retry ${tries}/${SSH_MAX_RETRIES}" >&2
        sleep "${SSH_BAN_WAIT_SECS}"
    done
}

# remote_scp: scp <local> to a master path with throttle + retry. scp has no
# "expected non-zero" semantics (it either copies or errors), so retry on any
# failure. Usage: remote_scp <local_file> <remote_abs_path>
remote_scp() {
    local local_file="$1" remote_path="$2" tries=0 rc
    while :; do
        sleep "${SSH_THROTTLE_SECS}"
        scp "${local_file}" "${MASTER_HOST}:${remote_path}"
        rc=$?
        (( rc == 0 )) && return 0
        tries=$(( tries + 1 ))
        if (( tries >= SSH_MAX_RETRIES )); then
            echo "ERROR: scp ${local_file} → ${MASTER_HOST}:${remote_path} failed ${tries}×; giving up" >&2
            return "${rc}"
        fi
        echo "  scp failed (rc=${rc}) — likely rate-limit ban; waiting ${SSH_BAN_WAIT_SECS}s then retry ${tries}/${SSH_MAX_RETRIES}" >&2
        sleep "${SSH_BAN_WAIT_SECS}"
    done
}

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

# Verify the local Java toolchain (which the mvn build uses) is 1.8 — the
# project pins maven.compiler.source/target to 1.8 to match the cluster. If
# JAVA8_HOME is set, select it for this process first. Pass "true" to only warn
# (e.g. under --dry-run); otherwise a wrong version hard-fails (exit 4).
assert_java8() {
    local dry="${1:-false}"
    if [[ -n "${JAVA8_HOME:-}" ]]; then
        export JAVA_HOME="${JAVA8_HOME}"
        export PATH="${JAVA8_HOME}/bin:${PATH}"
    fi
    local ver
    ver=$(java -version 2>&1 | head -n1)
    if [[ "${ver}" == *'version "1.8'* ]]; then
        echo "java          = 1.8 (verified)"
        return 0
    fi
    local msg="ERROR: Java 1.8 required, but 'java -version' reported: ${ver}
Set JAVA8_HOME to a JDK 8 install, or put Java 8 first on PATH."
    if [[ "${dry}" == "true" ]]; then
        echo "WARNING: ${msg}" >&2
        return 0
    fi
    echo "${msg}" >&2
    exit 4
}

# Kill every RUNNING/ACCEPTED YARN app whose name carries this run_id tag.
# Jobs tag themselves via `-D companion.run.tag=<run_id>`, which AbstractCompanionJob
# folds into the YARN job name as "<JobName> [<run_id>]".
cancel_run() {
    local run_id="$1"
    local apps
    apps=$(remote_ssh \
        "${HADOOP_BIN%hadoop}yarn application -appStates RUNNING,ACCEPTED,SUBMITTED -list 2>/dev/null" \
        | grep -F "[${run_id}]" | awk '{print $1}' || true)
    if [[ -z "${apps}" ]]; then
        echo "no running YARN apps for run_id=${run_id}"
        return 0
    fi
    local app
    for app in ${apps}; do
        echo "+ yarn application -kill ${app}"
        remote_ssh "${HADOOP_BIN%hadoop}yarn application -kill ${app}" || true
    done
}
