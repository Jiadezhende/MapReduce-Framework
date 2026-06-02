#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Single-node Hadoop 3.3.6 bring-up — RUN THIS ON THE SERVER (not the laptop).
#
# Renders the deploy/single-node/*.xml templates into $HADOOP_HOME/etc/hadoop/,
# formats the NameNode (first time only), starts all daemons, and creates the
# /companion HDFS layout. Idempotent: safe to re-run (re-renders configs, skips
# an already-formatted NN, restarts daemons cleanly).
#
# Usage — single disk (HDFS + shuffle share one volume):
#   HADOOP_HOME=/opt/module/hadoop-3.3.6 \
#   JAVA8_HOME=/opt/module/jdk1.8.0_xxx \
#   DATA_VOL=/data/hadoop \
#   SERVER_HOST=$(hostname) \
#   bash deploy/single-node/bootstrap.sh
#
# Usage — two disks (RECOMMENDED, see docs/single-node-deploy.md §5): put HDFS on
# one disk and the Stage2 shuffle spill on another, so a shuffle burst can never
# threaten HDFS. Set HDFS_VOL and SHUFFLE_VOL instead of DATA_VOL:
#   HADOOP_HOME=/opt/module/hadoop-3.3.6 \
#   JAVA8_HOME=/opt/module/jdk1.8.0_xxx \
#   HDFS_VOL=/data/hdfs SHUFFLE_VOL=/data/shuffle \
#   SERVER_HOST=$(hostname) \
#   bash deploy/single-node/bootstrap.sh
#
# After this, upload the input CSVs (see docs/single-node-deploy.md), then submit
# jobs from the laptop with scripts/cluster_run.sh as usual.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- 0. Inputs --------------------------------------------------------------
: "${HADOOP_HOME:?set HADOOP_HOME to the Hadoop install dir on this server}"
: "${JAVA8_HOME:?set JAVA8_HOME to a JDK 8 install (project is pinned to Java 1.8)}"
: "${SERVER_HOST:=$(hostname)}"
# Two-disk layout: HDFS_VOL (HDFS data) + SHUFFLE_VOL (Stage2 spill). For a
# single shared disk, set DATA_VOL and both default to it.
: "${HDFS_VOL:=${DATA_VOL:-}}"
: "${SHUFFLE_VOL:=${DATA_VOL:-}}"
[[ -n "${HDFS_VOL}" ]]    || { echo "ERROR: set HDFS_VOL (+SHUFFLE_VOL) for two disks, or DATA_VOL for one" >&2; exit 1; }
[[ -n "${SHUFFLE_VOL}" ]] || { echo "ERROR: set SHUFFLE_VOL, or DATA_VOL for a single shared disk" >&2; exit 1; }

CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export JAVA_HOME="${JAVA8_HOME}"
export PATH="${JAVA8_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"

echo "=== single-node bring-up ==="
echo "  HADOOP_HOME = ${HADOOP_HOME}"
echo "  JAVA8_HOME  = ${JAVA8_HOME}"
echo "  HDFS_VOL    = ${HDFS_VOL}"
echo "  SHUFFLE_VOL = ${SHUFFLE_VOL}$( [[ "${HDFS_VOL}" == "${SHUFFLE_VOL}" ]] && echo '  (single shared disk)' )"
echo "  SERVER_HOST = ${SERVER_HOST}"
echo "  CONF_DIR    = ${CONF_DIR}"

# --- 1. Preconditions -------------------------------------------------------
[[ -d "${HADOOP_HOME}" ]]    || { echo "ERROR: HADOOP_HOME does not exist: ${HADOOP_HOME}" >&2; exit 1; }
[[ -x "${JAVA8_HOME}/bin/java" ]] || { echo "ERROR: no java under JAVA8_HOME: ${JAVA8_HOME}" >&2; exit 1; }
if ! "${JAVA8_HOME}/bin/java" -version 2>&1 | head -n1 | grep -q 'version "1.8'; then
    echo "ERROR: JAVA8_HOME is not Java 1.8:" >&2
    "${JAVA8_HOME}/bin/java" -version >&2 || true
    exit 1
fi
# start-dfs.sh/start-yarn.sh ssh into every name in the `workers` file (= SERVER_HOST)
# AND into localhost for the NN/RM — both must be passwordless.
for h in localhost "${SERVER_HOST}"; do
    echo "+ ssh ${h} (passwordless check)"
    if ! ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "${h}" true 2>/dev/null; then
        echo "ERROR: 'ssh ${h}' is not passwordless. start-dfs.sh/start-yarn.sh need it." >&2
        echo "       Fix: ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys" >&2
        exit 1
    fi
done

# --- 2. Scratch dirs on the volume(s) ---------------------------------------
echo "+ mkdir HDFS dirs under ${HDFS_VOL}"
mkdir -p "${HDFS_VOL}/tmp" "${HDFS_VOL}/dfs/name" "${HDFS_VOL}/dfs/data" "${HDFS_VOL}/logs"
echo "+ mkdir shuffle dirs under ${SHUFFLE_VOL}"
mkdir -p "${SHUFFLE_VOL}/nm-local" "${SHUFFLE_VOL}/nm-logs"

# --- 3. Render config templates → $HADOOP_HOME/etc/hadoop -------------------
render() {
    local src="$1" dst="$2"
    sed -e "s#__SERVER_HOST__#${SERVER_HOST}#g" \
        -e "s#__HDFS_VOL__#${HDFS_VOL}#g" \
        -e "s#__SHUFFLE_VOL__#${SHUFFLE_VOL}#g" \
        -e "s#__HADOOP_HOME__#${HADOOP_HOME}#g" \
        -e "s#__JAVA8_HOME__#${JAVA8_HOME}#g" \
        "${src}" > "${dst}"
    echo "  rendered ${dst}"
}
echo "+ render site configs"
render "${SCRIPT_DIR}/core-site.xml"   "${CONF_DIR}/core-site.xml"
render "${SCRIPT_DIR}/hdfs-site.xml"   "${CONF_DIR}/hdfs-site.xml"
render "${SCRIPT_DIR}/yarn-site.xml"   "${CONF_DIR}/yarn-site.xml"
render "${SCRIPT_DIR}/mapred-site.xml" "${CONF_DIR}/mapred-site.xml"
# workers: strip comments, keep the single host line.
echo "${SERVER_HOST}" > "${CONF_DIR}/workers"
echo "  wrote ${CONF_DIR}/workers"

# hadoop-env.sh: append the heap snippet once (marker-guarded for idempotency).
ENV_MARKER="# >>> companion single-node heaps >>>"
if ! grep -qF "${ENV_MARKER}" "${CONF_DIR}/hadoop-env.sh" 2>/dev/null; then
    {
        echo ""
        echo "${ENV_MARKER}"
        sed -e "s#__JAVA8_HOME__#${JAVA8_HOME}#g" \
            -e "s#__HDFS_VOL__#${HDFS_VOL}#g" \
            "${SCRIPT_DIR}/hadoop-env.sh.snippet"
        echo "# <<< companion single-node heaps <<<"
    } >> "${CONF_DIR}/hadoop-env.sh"
    echo "  appended heap snippet to ${CONF_DIR}/hadoop-env.sh"
else
    echo "  hadoop-env.sh already has the heap snippet — skipping"
fi

# --- 4. Format NameNode (first time only) -----------------------------------
if [[ -f "${HDFS_VOL}/dfs/name/current/VERSION" ]]; then
    echo "+ NameNode already formatted (${HDFS_VOL}/dfs/name/current/VERSION) — skipping format"
else
    echo "+ formatting NameNode"
    "${HADOOP_HOME}/bin/hdfs" namenode -format -nonInteractive -force "companion-single"
fi

# --- 5. Start daemons -------------------------------------------------------
echo "+ start HDFS / YARN / JobHistory"
"${HADOOP_HOME}/sbin/start-dfs.sh"
"${HADOOP_HOME}/sbin/start-yarn.sh"
"${HADOOP_HOME}/bin/mapred" --daemon start historyserver || true

# Give the DataNode a moment to register before we touch HDFS.
sleep 5

# --- 6. Create the /companion HDFS layout -----------------------------------
echo "+ create /companion HDFS layout"
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p \
    /companion/input/raw \
    /companion/runs \
    /companion/_stage3

# --- 7. Report --------------------------------------------------------------
echo ""
echo "=== jps ==="
jps | grep -E 'NameNode|DataNode|ResourceManager|NodeManager|JobHistoryServer|SecondaryNameNode' || true
echo ""
echo "=== hdfs dfsadmin -report (summary) ==="
"${HADOOP_HOME}/bin/hdfs" dfsadmin -report | grep -E 'Live datanodes|Configured Capacity|DFS Remaining|Name:' || true
echo ""
echo "Bring-up done. Next: upload input CSVs, then run scripts/cluster_run.sh from the laptop."
echo "See docs/single-node-deploy.md for the upload + end-to-end steps."
