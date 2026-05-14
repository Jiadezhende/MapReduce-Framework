#!/usr/bin/env bash
# Fetch a raw CSV sample from HDFS to the local repo root.
#
# Usage:
#   ./fetch_dataset.sh [1d|7d|31d]    # default: 1d
#
# - 1d   : 195 MB, UTC 2015-01-01 slice. M1 development & local correctness work.
# - 7d   : 1.4 GB, UTC 2015-01-01..07. M2 local debugging and baseline diff.
# - 31d  : 5.8 GB, full dataset. Only pull if you really need it locally;
#          MapReduce jobs can read straight from HDFS without a local copy.
#
# `mini` is NOT handled here — tests/data/mini.csv ships with the repo and is
# only for automated tests, not for fetching or development.
#
# Source of truth on HDFS: ${COMPANION_ROOT}/input/raw/<name>.csv
# Maintainer publishes by running scripts/upload_to_hdfs.sh on the master node.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

SIZE="${1:-1d}"

case "${SIZE}" in
    1d|7d|31d)
        ;;
    *)
        echo "ERROR: size must be 1d, 7d, or 31d (mini is test-only, ships in repo)" >&2
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
