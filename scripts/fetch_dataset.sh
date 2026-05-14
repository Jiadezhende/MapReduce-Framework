#!/usr/bin/env bash
# Fetch a raw CSV sample from HDFS to the local repo root.
#
# Usage:
#   ./fetch_dataset.sh [mini|1d|31d]    # default: 1d
#
# - mini : already shipped with the repo at tests/data/mini.csv (no download).
# - 1d   : 195 MB, UTC 2015-01-01 slice. Suitable for M1 development.
# - 31d  : 5.8 GB, full dataset. Only pull if you really need it locally;
#          MapReduce jobs can read straight from HDFS without a local copy.
#
# Source of truth on HDFS: ${COMPANION_ROOT}/input/raw/<name>.csv
# Maintainer publishes by running scripts/upload_to_hdfs.sh on the master node.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

SIZE="${1:-1d}"

case "${SIZE}" in
    mini)
        local_path="${LOCAL_DATA_DIR}/tests/data/mini.csv"
        if [[ -f "${local_path}" ]]; then
            echo "mini sample already in repo: ${local_path}"
            exit 0
        fi
        echo "ERROR: tests/data/mini.csv missing. Re-clone or run a fresh git pull." >&2
        exit 1
        ;;
    1d|31d)
        ;;
    *)
        echo "ERROR: size must be one of: mini, 1d, 31d" >&2
        exit 2
        ;;
esac

remote="${COMPANION_ROOT}/input/raw/${SIZE}.csv"
local_path="${LOCAL_DATA_DIR}/${SIZE}.csv"

if ! "${HADOOP_BIN}" fs -test -e "${remote}"; then
    echo "ERROR: ${remote} not found on HDFS." >&2
    echo "       Ask the maintainer to run scripts/upload_to_hdfs.sh ${SIZE}.csv" >&2
    exit 1
fi

remote_size=$("${HADOOP_BIN}" fs -stat '%b' "${remote}")
if [[ -f "${local_path}" ]]; then
    local_size=$(stat -c '%s' "${local_path}")
    if [[ "${local_size}" == "${remote_size}" ]]; then
        echo "${local_path} already present (${local_size} bytes), skipping download."
        exit 0
    fi
    echo "Local size ${local_size} != remote ${remote_size}, re-downloading."
    rm -f "${local_path}"
fi

echo "Fetching hdfs://${remote} → ${local_path} (${remote_size} bytes)"
"${HADOOP_BIN}" fs -get "${remote}" "${local_path}"

got=$(stat -c '%s' "${local_path}")
if [[ "${got}" != "${remote_size}" ]]; then
    echo "ERROR: size mismatch after download (got=${got}, expected=${remote_size})" >&2
    exit 1
fi
echo "OK: ${local_path} (${got} bytes)"
