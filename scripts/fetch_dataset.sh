#!/usr/bin/env bash
# Fetch a raw CSV sample from HDFS to the local repo root via non-interactive ssh.
# The local machine does NOT need a hadoop client; we stream
# `ssh master hadoop fs -cat ...` into a local file.
#
# Usage:
#   ./fetch_dataset.sh [1d|7d|31d] [--dry-run]    # default: 1d
#
# - 1d   : 195 MB, UTC 2015-01-01 slice. M1 development & local correctness work.
# - 7d   : 1.4 GB, UTC 2015-01-01..07. M2 local debugging and baseline diff.
# - 31d  : 5.8 GB, full dataset. Only pull if you really need it locally;
#          MapReduce jobs can read straight from HDFS without a local copy.
#
# `mini` is NOT handled here — tests/data/mini.csv ships with the repo.
#
# Source of truth on HDFS: ${HDFS_INPUT_ROOT}/<name>.csv

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

SIZE="1d"
DRY_RUN=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        1d|7d|31d) SIZE="$1"; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
        *) echo "ERROR: unknown arg $1" >&2; exit 2 ;;
    esac
done

remote="${HDFS_INPUT_ROOT}/${SIZE}.csv"
local_path="${LOCAL_DATA_DIR}/${SIZE}.csv"

echo "remote: ssh://${MASTER_HOST} ${remote}"
echo "local:  ${local_path}"

# Existence check.
if [[ "${DRY_RUN}" == "false" ]]; then
    if ! ssh "${MASTER_HOST}" "${HADOOP_BIN} fs -test -e ${remote}"; then
        echo "ERROR: ${remote} not found on HDFS." >&2
        echo "       Ask the maintainer to run scripts/upload_to_hdfs.sh ${SIZE}.csv" >&2
        exit 1
    fi
fi

# Compare sizes. `hadoop fs -du` first column is bytes.
if [[ "${DRY_RUN}" == "true" ]]; then
    echo "+ ssh ${MASTER_HOST} ${HADOOP_BIN} fs -du ${remote}"
    echo "+ ssh ${MASTER_HOST} ${HADOOP_BIN} fs -cat ${remote} > ${local_path}"
    exit 0
fi

remote_size=$(ssh "${MASTER_HOST}" "${HADOOP_BIN} fs -du ${remote}" | awk '{print $1; exit}')
if [[ -f "${local_path}" ]]; then
    local_size=$(stat -c '%s' "${local_path}")
    if [[ "${local_size}" == "${remote_size}" ]]; then
        echo "${local_path} already present (${local_size} bytes), skipping."
        exit 0
    fi
    echo "Local ${local_size} != remote ${remote_size}, re-downloading."
    rm -f "${local_path}"
fi

echo "Streaming ${remote_size} bytes ..."
ssh "${MASTER_HOST}" "${HADOOP_BIN} fs -cat ${remote}" > "${local_path}"

got=$(stat -c '%s' "${local_path}")
if [[ "${got}" != "${remote_size}" ]]; then
    echo "ERROR: size mismatch after download (got=${got}, expected=${remote_size})" >&2
    exit 1
fi
echo "OK: ${local_path} (${got} bytes)"
