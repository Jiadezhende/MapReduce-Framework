#!/usr/bin/env bash
# Pull a run's final TopN result (and metrics) from HDFS to the local machine.
#
# Stage3 already emits a single, bounded `top_n.csv` (the globally-sorted TopN)
# plus `_metrics.json` under final/<phase>/. The local box has no hadoop client,
# so we stream each file over non-interactive ssh (`fs -cat` → local redirect),
# same transport as the rest of scripts/.
#
# Usage: scripts/cluster_fetch.sh <run_id> <phase> [dest_dir]
#   phase    ∈ {1d,7d,31d}
#   dest_dir local output dir (default: <repo>/out/<run_id>/)

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Usage: $0 <run_id> <phase> [dest_dir]" >&2
    exit 2
fi

run_id="$1"
phase="$2"
case "${phase}" in
    1d|7d|31d) ;;
    *) echo "ERROR: phase must be 1d, 7d, or 31d (got '${phase}')" >&2; exit 2 ;;
esac
dest_dir="${3:-${LOCAL_DATA_DIR}/out/${run_id}}"

remote_dir="${HDFS_RUNS_ROOT}/${run_id}/final/${phase}"

# Stream one HDFS file to a local path, failing cleanly if it's absent so we
# never leave a truncated/empty file behind.
fetch_file() {
    local name="$1"
    local src="${remote_dir}/${name}"
    local dst="${dest_dir}/${name}"
    if ! ssh "${MASTER_HOST}" "${HADOOP_BIN} fs -test -e ${src}" 2>/dev/null; then
        echo "WARNING: skip ${src} (not found on HDFS)" >&2
        return 1
    fi
    ssh "${MASTER_HOST}" "${HADOOP_BIN} fs -cat ${src}" > "${dst}"
    echo "  ${dst}"
}

mkdir -p "${dest_dir}"
echo "fetching ${run_id} / ${phase} → ${dest_dir}"
fetch_file "top_n.csv" || true
fetch_file "_metrics.json" || true

# Quick on-terminal recap: how many rows landed + the first few.
topn_local="${dest_dir}/top_n.csv"
if [[ -s "${topn_local}" ]]; then
    rows=$(wc -l < "${topn_local}" | tr -d ' ')
    echo
    echo "top_n.csv: ${rows} rows (vidA,vidB,count, desc). preview:"
    head -n 10 "${topn_local}" | sed 's/^/  /'
fi
