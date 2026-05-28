#!/usr/bin/env bash
# Peek at the head of a stage output via non-interactive ssh.
#
# Usage: scripts/cluster_head.sh <run_id> <subdir> <phase> [N]
#   subdir ∈ {vid_freq,filtered,pair_loc_slot,companions,final}
#   phase  ∈ {1d,7d,31d}
#   N      number of lines (default 50)

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "Usage: $0 <run_id> <subdir> <phase> [N]" >&2
    exit 2
fi

run_id="$1"
subdir="$2"
phase="$3"
n="${4:-50}"

target="${HDFS_RUNS_ROOT}/${run_id}/${subdir}/${phase}"
ssh "${MASTER_HOST}" "${HADOOP_BIN} fs -cat ${target}/part-* | head -${n}"
