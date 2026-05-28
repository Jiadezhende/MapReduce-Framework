#!/usr/bin/env bash
# Cancel a run's in-flight YARN apps via non-interactive ssh.
#
# Jobs tag themselves with `-D companion.run.tag=<run_id>` (set by cluster_run.sh),
# which AbstractCompanionJob folds into the YARN job name as "<JobName> [<run_id>]".
# This script lists RUNNING/ACCEPTED apps, matches that tag, and kills them.
# Output already-produced (with _SUCCESS) is untouched, so the run stays
# resumable via `cluster_run.sh --run-id <run_id>`.
#
# Usage: scripts/cluster_cancel.sh <run_id>

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <run_id>" >&2
    exit 2
fi

cancel_run "$1"
