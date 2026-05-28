#!/usr/bin/env bash
# Show HDFS contents of a run root via non-interactive ssh.
#
# Usage: scripts/cluster_status.sh <run_id>

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <run_id>" >&2
    exit 2
fi

run_id="$1"
ssh "${MASTER_HOST}" "${HADOOP_BIN} fs -ls -R -h ${HDFS_RUNS_ROOT}/${run_id}"
