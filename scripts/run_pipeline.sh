#!/usr/bin/env bash
# End-to-end driver for the companion pipeline.
#
# Phase 0 skeleton: prints out the Job submission commands but does NOT
# execute them. As R2/R3/R4 land their Job classes, replace the `echo` calls
# below with real `hadoop jar` invocations.
#
# Usage:
#   ./run_pipeline.sh --days {1|7|31} [--from {stage0|stage1|stage2|stage3}]
#                     [--dry-run] [-Dkey=value ...]

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./env.sh
source "${SCRIPT_DIR}/env.sh"

PHASE="1d"
FROM_STAGE="stage0"
DRY_RUN=false
EXTRA_CONF=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --days)
            case "$2" in
                1)  PHASE="1d" ;;
                7)  PHASE="7d" ;;
                31) PHASE="31d" ;;
                *)  echo "ERROR: --days must be 1, 7, or 31" >&2; exit 2 ;;
            esac
            shift 2
            ;;
        --from)
            FROM_STAGE="$2"; shift 2 ;;
        --dry-run)
            DRY_RUN=true; shift ;;
        -D*)
            EXTRA_CONF+=("$1"); shift ;;
        -h|--help)
            sed -n '2,12p' "$0"; exit 0 ;;
        *)
            echo "ERROR: unknown arg $1" >&2; exit 2 ;;
    esac
done

case "${PHASE}" in
    1d)  S1_RED="${STAGE1_REDUCERS_1D}";  S2_RED="${STAGE2_REDUCERS_1D}"  ;;
    7d)  S1_RED="${STAGE1_REDUCERS_7D}";  S2_RED="${STAGE2_REDUCERS_7D}"  ;;
    31d) S1_RED="${STAGE1_REDUCERS_31D}"; S2_RED="${STAGE2_REDUCERS_31D}" ;;
esac

submit() {
    local module="$1"; shift
    local class="$1"; shift
    local jar
    if [[ "${DRY_RUN}" == "true" ]]; then
        jar="${LOCAL_DATA_DIR}/${module}/target/${module}-<version>.jar"
    else
        if ! jar=$(companion_jar "${module}"); then return 1; fi
    fi

    local cmd=( "${HADOOP_BIN}" jar "${jar}" "${class}" "$@" "${EXTRA_CONF[@]}" )
    echo "+ ${cmd[*]}"
    if [[ "${DRY_RUN}" == "false" ]]; then
        "${cmd[@]}"
    fi
}

# --- Stage 0: preprocess + vid filter (owner: R2) ---
if [[ "${FROM_STAGE}" == "stage0" ]]; then
    submit stage0 companion.stage0.Stage0aFreqJob \
        "${COMPANION_ROOT}/input/${PHASE}" \
        "${COMPANION_ROOT}/vid_freq/${PHASE}"
    submit stage0 companion.stage0.Stage0bFilterJob \
        "${COMPANION_ROOT}/input/${PHASE}" \
        "${COMPANION_ROOT}/filtered/${PHASE}" \
        "-D companion.vid_freq.path=${COMPANION_ROOT}/vid_freq/${PHASE}"
    FROM_STAGE="stage1"
fi

# --- Stage 1: sliding-window pair generation (owner: R3) ---
if [[ "${FROM_STAGE}" == "stage1" ]]; then
    submit stage1 companion.stage1.Stage1Job \
        "${COMPANION_ROOT}/filtered/${PHASE}" \
        "${COMPANION_ROOT}/pair_loc_slot/${PHASE}" \
        "-D mapreduce.job.reduces=${S1_RED}"
    FROM_STAGE="stage2"
fi

# --- Stage 2: pair counting + threshold filter (owner: R4) ---
if [[ "${FROM_STAGE}" == "stage2" ]]; then
    submit stage2 companion.stage2.Stage2Job \
        "${COMPANION_ROOT}/pair_loc_slot/${PHASE}" \
        "${COMPANION_ROOT}/companions/${PHASE}" \
        "-D mapreduce.job.reduces=${S2_RED}"
    FROM_STAGE="stage3"
fi

# --- Stage 3: sort + metrics export (owner: R4) ---
if [[ "${FROM_STAGE}" == "stage3" ]]; then
    submit stage3 companion.stage3.Stage3SortJob \
        "${COMPANION_ROOT}/companions/${PHASE}" \
        "${COMPANION_ROOT}/final/${PHASE}"
fi

echo "Pipeline complete for phase=${PHASE}."
