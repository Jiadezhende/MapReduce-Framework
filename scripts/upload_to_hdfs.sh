#!/usr/bin/env bash
# Upload the raw CSV(s) to HDFS at ${COMPANION_ROOT}/input/raw/.
# Idempotent: re-uploading the same file overwrites in place.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <local-csv> [<local-csv> ...]" >&2
    exit 2
fi

dst="${COMPANION_ROOT}/input/raw"
"${HADOOP_BIN}" fs -mkdir -p "${dst}"

for f in "$@"; do
    if [[ ! -f "${f}" ]]; then
        echo "ERROR: file not found: ${f}" >&2
        exit 1
    fi
    echo "Uploading ${f} → hdfs://${dst}/$(basename "${f}")"
    "${HADOOP_BIN}" fs -put -f "${f}" "${dst}/$(basename "${f}")"
done

"${HADOOP_BIN}" fs -ls -h "${dst}"
