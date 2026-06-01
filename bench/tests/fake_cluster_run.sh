#!/usr/bin/env bash
# 本地测试用的假 cluster_run.sh：不连集群，只复制真实 .jhist fixture 到 BENCH_RESULT_DIR。

set -euo pipefail

if [[ -z "${BENCH_RESULT_DIR:-}" ]]; then
    echo "BENCH_RESULT_DIR is required" >&2
    exit 2
fi

mkdir -p "${BENCH_RESULT_DIR}"
cp "$(dirname "$0")"/fixtures/history/*.jhist "${BENCH_RESULT_DIR}/"

ORIGINAL_ARGS="$*"
RUN_ID="fake-${BENCH_PHASE:-unknown}-${BENCH_DELTA:-0}-${BENCH_K:-0}-${BENCH_REDUCERS:-0}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --run-id) RUN_ID="$2"; shift 2 ;;
        *) shift ;;
    esac
done

echo "fake cluster run"
echo "phase=${BENCH_PHASE:-}"
echo "delta=${BENCH_DELTA:-}"
echo "k=${BENCH_K:-}"
echo "reducers=${BENCH_REDUCERS:-}"
echo "args=${ORIGINAL_ARGS}"
echo "Done. run_id=${RUN_ID}"
