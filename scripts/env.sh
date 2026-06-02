# shellcheck shell=bash
# Centralized environment for the companion pipeline.
# Sourced by every other script in scripts/.

: "${COMPANION_ROOT:=/companion}"
: "${HADOOP_CONF_DIR:=/etc/hadoop/conf}"
: "${HADOOP_BIN:=hadoop}"
: "${LOCAL_DATA_DIR:=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Non-login submit: every cluster operation goes through `ssh ${MASTER_HOST}`.
# Override MASTER_HOST in ~/.ssh/config or via env if your alias differs.
# Single-node deploy: this one host runs every Hadoop role (NN/DN/RM/NM/JHS);
# point the `master` ssh alias (or MASTER_HOST) at that server — nothing else
# in the submit flow changes. See docs/single-node-deploy.md.
: "${MASTER_HOST:=speed}"
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
#
# Single-node sizing (32c/64G/500G): the NM offers ~28 vcores (see
# deploy/single-node/yarn-site.xml), so reducer counts are capped to leave
# headroom for the AM + concurrent maps. More reducers than the vcore pool only
# adds spill files without extra parallelism.
: "${YARN_QUEUE:=default}"
: "${REDUCERS_1D:=8}"
: "${REDUCERS_7D:=24}"
: "${REDUCERS_31D:=24}"

# Stage2 pair-hash sharding (cluster_run.sh): split Stage2 into K sequential
# sub-jobs, each emitting only 1/K of the pairs, so the nm-local-dir shuffle
# RESIDENT peak is capped at ~1/K of the single-pass map output.
#
# Sizing is grounded in the MEASURED 31d run docs/runs/31d-cf1f2f6-k3gzip/:
#   - single-pass Stage2 map output ≈ 319 GB (gzip), and it stays RESIDENT until
#     that round's _SUCCESS — it does NOT drain as reduce progresses (§2.1), so
#     io.sort.mb / slowstart do NOT move this peak; only K and the gzip codec do.
#   - single-node (N=1, one node carries it all) shuffle peak
#       ≈ 320 / (N=1 × K) × 1.3        (×1.3 = skew + reduce scratch)
#     → K=2 ≈ 208 GB, K=3 ≈ 139 GB, K=4 ≈ 104 GB.
#   - HDFS /companion is only ~118 GB total (pair_loc_slot 103.5 GB dominates),
#     and EVERY round re-reads that 103.5 GB input. So a bigger K buys a lower
#     peak at the cost of K×103.5 GB redundant reads + near-linear wall-time:
#     the report shows K=6 ≈ 2× the runtime of K=3 for NO benefit → do NOT
#     over-shard. K=6 was wrong; K=3 is the single-node default.
#
# With a dedicated ~300 GB shuffle disk: K=3 sits at ~46% (rock solid), K=2 at
# ~69% (faster, one fewer input re-read). Default K=3; drop to 2 to speed up once
# a run shows the shuffle disk staying cool. 7d single-pass map output is only
# ~83 GB → peak ~107 GB at K=1, fits one pass. See docs/single-node-deploy.md §5.
: "${STAGE2_ROUNDS_1D:=1}"
: "${STAGE2_ROUNDS_7D:=1}"
: "${STAGE2_ROUNDS_31D:=3}"

# Per-phase container + spill tuning.
# On the single 32c/64G node the NM pool is ~48 GB / 28 vcores (see
# deploy/single-node/yarn-site.xml), so vcores — not memory — is the concurrency
# limiter. That lets us use *larger* containers (map 2 GB / reduce 3 GB) and a
# bigger map sort buffer to cut spill *re-write* passes, while still filling all
# 28 vcores. io.sort.mb does NOT shrink the shuffle *resident* peak (= total map
# output, retained till job end) — that peak is controlled by the gzip shuffle
# codec (denser than default Snappy) + STAGE2_ROUNDS above, which are the real
# disk levers. See docs/single-node-deploy.md and docs/space-optimization.md §8.2.
: "${TUNE_1D:=}"
: "${TUNE_7D:=-D mapreduce.task.io.sort.mb=256}"
: "${TUNE_31D:=-D mapreduce.map.memory.mb=2048 -D mapreduce.map.java.opts=-Xmx1536m -D mapreduce.reduce.memory.mb=3072 -D mapreduce.reduce.java.opts=-Xmx2560m -D mapreduce.task.io.sort.mb=512 -D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.GzipCodec -D companion.hll.threshold=100000}"

# Final HDFS-output gzip for the two disk-heavy stages only: stage2 companions
# (folded witnesses) + stage3 sorted companions.csv. This is the FINAL output
# codec (mapreduce.output.fileoutputformat.compress) — distinct from the
# map-output/shuffle codec in TUNE_* above. Text gzips ~4–5×, keeping the
# stage2-output + stage3-sorted-copy peak well under HDFS free space.
#
# Applied to stage2/stage3 ONLY (see cluster_run.sh), NOT stage0a/stage1:
# stage1's pair_loc_slot is stage2's INPUT, and gzip is non-splittable — gzipping
# it would force one map per file and collapse stage2 parallelism. Stage2 emits
# one .gz part per reducer (K×reducers files), so stage3 still gets plenty of
# splittable units; TextInputFormat auto-decompresses .gz transparently.
: "${OUT_COMPRESS:=-D mapreduce.output.fileoutputformat.compress=true -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec}"

export COMPANION_ROOT HADOOP_CONF_DIR HADOOP_BIN LOCAL_DATA_DIR YARN_QUEUE
export MASTER_HOST HDFS_INPUT_ROOT HDFS_RUNS_ROOT HDFS_TEST_ROOT_BASE REMOTE_SUBMIT_BASE
export REDUCERS_1D REDUCERS_7D REDUCERS_31D TUNE_1D TUNE_7D TUNE_31D OUT_COMPRESS
export STAGE2_ROUNDS_1D STAGE2_ROUNDS_7D STAGE2_ROUNDS_31D

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
#
# Single-node deploy: if the new server does NOT rate-limit by source IP, set
# `export SSH_THROTTLE_SECS=0.2` (or 0) to speed up jar staging. The wrappers and
# the launch flow are unchanged either way — only the spacing shrinks.
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
