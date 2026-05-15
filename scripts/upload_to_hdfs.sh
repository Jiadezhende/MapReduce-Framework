#!/usr/bin/env bash
# Upload local CSV(s) to HDFS ${HDFS_INPUT_ROOT} via non-interactive ssh.
# The local machine does NOT need a hadoop client; we scp the file to master
# then run `hadoop fs -put` there.
#
# Usage: scripts/upload_to_hdfs.sh [--dry-run] <local-csv> [<local-csv> ...]

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

DRY_RUN=false
FILES=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help) sed -n '2,8p' "$0"; exit 0 ;;
        *)         FILES+=("$1"); shift ;;
    esac
done

if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "Usage: $0 [--dry-run] <local-csv> [<local-csv> ...]" >&2
    exit 2
fi

run() {
    echo "+ $*"
    if [[ "${DRY_RUN}" == "false" ]]; then
        "$@"
    fi
}

REMOTE_STAGE="${REMOTE_SUBMIT_BASE}/upload"
run ssh "${MASTER_HOST}" "mkdir -p ${REMOTE_STAGE} && ${HADOOP_BIN} fs -mkdir -p ${HDFS_INPUT_ROOT}"

for f in "${FILES[@]}"; do
    if [[ ! -f "${f}" ]]; then
        echo "ERROR: file not found: ${f}" >&2
        exit 1
    fi
    base=$(basename "${f}")
    run scp "${f}" "${MASTER_HOST}:${REMOTE_STAGE}/${base}"
    run ssh "${MASTER_HOST}" \
        "${HADOOP_BIN} fs -put -f ${REMOTE_STAGE}/${base} ${HDFS_INPUT_ROOT}/${base} && rm -f ${REMOTE_STAGE}/${base}"
done

run ssh "${MASTER_HOST}" "${HADOOP_BIN} fs -ls -h ${HDFS_INPUT_ROOT}"
