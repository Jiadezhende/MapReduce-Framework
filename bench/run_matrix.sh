#!/usr/bin/env bash
# 按 R6 文档串行执行性能参数矩阵。
#
# Usage:
#   bench/run_matrix.sh --phase {1d|7d|31d} [--baseline-only]
#                       [--dry-run] [--fake-runner PATH]
#                       [--no-monitor] [--no-plot] [--monitor-interval N]

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
# shellcheck source=../scripts/env.sh
source "${REPO_ROOT}/scripts/env.sh"

RESULT_ROOT="${SCRIPT_DIR}/results"
: "${BENCH_HISTORY_ROOT:=/tmp/hadoop-yarn/staging/history/done}"
: "${BENCH_HISTORY_LOOKBACK_DAYS:=7}"
: "${BENCH_HISTORY_FETCH_RETRIES:=6}"
: "${BENCH_HISTORY_FETCH_SLEEP:=10}"
: "${BENCH_MONITOR_INTERVAL:=5}"

PHASE=""
BASELINE_ONLY=false
DRY_RUN=false
FAKE_RUNNER=""
ENABLE_MONITOR=true
ENABLE_PLOT=true
ACTIVE_MONITOR_PID=""
ACTIVE_MONITOR_RESULT_DIR=""

usage() {
    sed -n '2,7p' "$0"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --phase) PHASE="$2"; shift 2 ;;
        --baseline-only) BASELINE_ONLY=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --fake-runner) FAKE_RUNNER="$2"; shift 2 ;;
        --result-root) RESULT_ROOT="$2"; shift 2 ;;
        --no-monitor) ENABLE_MONITOR=false; shift ;;
        --no-plot) ENABLE_PLOT=false; shift ;;
        --monitor-interval) BENCH_MONITOR_INTERVAL="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "ERROR: unknown arg $1" >&2; usage; exit 2 ;;
    esac
done

case "${PHASE}" in
    1d) DAYS=1; LOW_REDUCERS=4; MID_REDUCERS=8; HIGH_REDUCERS=16 ;;
    7d) DAYS=7; LOW_REDUCERS=16; MID_REDUCERS=32; HIGH_REDUCERS=64 ;;
    31d) DAYS=31; LOW_REDUCERS=64; MID_REDUCERS=128; HIGH_REDUCERS=256 ;;
    "") echo "ERROR: --phase is required" >&2; usage; exit 2 ;;
    *) echo "ERROR: --phase must be 1d, 7d, or 31d" >&2; exit 2 ;;
esac

RUNNER="${FAKE_RUNNER:-${REPO_ROOT}/scripts/cluster_run.sh}"
if [[ ! -x "${RUNNER}" ]]; then
    echo "ERROR: runner is not executable: ${RUNNER}" >&2
    exit 2
fi

mkdir -p "${RESULT_ROOT}"

validate_history_fetch_config() {
    if ! [[ "${BENCH_HISTORY_LOOKBACK_DAYS}" =~ ^[0-9]+$ ]] || [[ "${BENCH_HISTORY_LOOKBACK_DAYS}" -lt 1 ]]; then
        echo "ERROR: BENCH_HISTORY_LOOKBACK_DAYS must be a positive integer" >&2
        exit 2
    fi
    if ! [[ "${BENCH_HISTORY_FETCH_RETRIES}" =~ ^[0-9]+$ ]] || [[ "${BENCH_HISTORY_FETCH_RETRIES}" -lt 1 ]]; then
        echo "ERROR: BENCH_HISTORY_FETCH_RETRIES must be a positive integer" >&2
        exit 2
    fi
    if ! [[ "${BENCH_HISTORY_FETCH_SLEEP}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: BENCH_HISTORY_FETCH_SLEEP must be a non-negative integer" >&2
        exit 2
    fi
    if ! [[ "${BENCH_MONITOR_INTERVAL}" =~ ^[0-9]+$ ]] || [[ "${BENCH_MONITOR_INTERVAL}" -lt 1 ]]; then
        echo "ERROR: BENCH_MONITOR_INTERVAL/--monitor-interval must be a positive integer" >&2
        exit 2
    fi
}

make_run_id() {
    local timestamp="$1"
    local index="$2"
    local git_sha
    git_sha=$(cd "${REPO_ROOT}" && git rev-parse --short HEAD 2>/dev/null || echo "nogit")
    printf "%s-%s-%s-%02d" "${USER:-bench}" "${git_sha}" "${timestamp}" "${index}"
}

remote_history_query() {
    local run_id="$1"
    local q_root q_pattern q_days
    printf -v q_root "%q" "${BENCH_HISTORY_ROOT}"
    printf -v q_pattern "%q" "[${run_id}]"
    printf -v q_days "%q" "${BENCH_HISTORY_LOOKBACK_DAYS}"

    # 只在 master 上做只读定位：grep 匹配 job name 里的 [run_id]，
    # 真正解析 counters/skew 仍全部在本地 bench 目录完成。
    printf 'root=%s; pattern=%s; days=%s; if [ ! -d "$root" ]; then exit 0; fi; find "$root" -type f -name "*.jhist" -mtime -"$days" -print 2>/dev/null | sort | while IFS= read -r f; do if grep -a -q -F "$pattern" "$f" 2>/dev/null; then printf "%%s\\n" "$f"; fi; done' \
        "${q_root}" "${q_pattern}" "${q_days}"
}

fetch_history_for_run() {
    local result_dir="$1"
    local run_id="$2"

    [[ "${DRY_RUN}" == "true" ]] && return 0
    [[ -n "${FAKE_RUNNER}" ]] && return 0
    if compgen -G "${result_dir}/*.jhist" >/dev/null; then
        return 0
    fi

    local remote_cmd
    remote_cmd=$(remote_history_query "${run_id}")

    local history_files=()
    local attempt
    for attempt in $(seq 1 "${BENCH_HISTORY_FETCH_RETRIES}"); do
        history_files=()
        local query_output
        if query_output=$(ssh "${MASTER_HOST}" "${remote_cmd}"); then
            local remote_path
            while IFS= read -r remote_path; do
                [[ -n "${remote_path}" ]] && history_files+=("${remote_path}")
            done <<<"${query_output}"
        else
            echo "WARN: failed to query JobHistory files on ${MASTER_HOST} for run_id=${run_id}" >&2
            history_files=()
        fi
        if [[ ${#history_files[@]} -gt 0 ]]; then
            break
        fi
        if [[ "${attempt}" -lt "${BENCH_HISTORY_FETCH_RETRIES}" ]]; then
            echo "waiting for JobHistory .jhist files for run_id=${run_id} (${attempt}/${BENCH_HISTORY_FETCH_RETRIES})"
            sleep "${BENCH_HISTORY_FETCH_SLEEP}"
        fi
    done

    if [[ ${#history_files[@]} -eq 0 ]]; then
        echo "ERROR: no .jhist files found for run_id=${run_id} under ${MASTER_HOST}:${BENCH_HISTORY_ROOT}" >&2
        echo "hint: set BENCH_HISTORY_ROOT if the cluster stores JobHistory elsewhere." >&2
        return 1
    fi

    : >"${result_dir}/history_sources.txt"
    local remote_path local_name q_remote
    for remote_path in "${history_files[@]}"; do
        local_name=$(basename "${remote_path}")
        printf -v q_remote "%q" "${remote_path}"
        ssh "${MASTER_HOST}" "cat ${q_remote}" >"${result_dir}/${local_name}"
        echo "${remote_path}" >>"${result_dir}/history_sources.txt"
    done
    echo "fetched ${#history_files[@]} JobHistory files into ${result_dir}"
}

start_monitor() {
    local result_dir="$1"

    if [[ "${ENABLE_MONITOR}" == "false" || "${DRY_RUN}" == "true" || -n "${FAKE_RUNNER}" ]]; then
        return 0
    fi

    local monitor_log="${result_dir}/monitor.log"
    echo "starting monitor_cluster.sh interval=${BENCH_MONITOR_INTERVAL}s" >>"${result_dir}/run.log"
    if command -v setsid >/dev/null 2>&1; then
        setsid "${SCRIPT_DIR}/monitor_cluster.sh" \
            --interval "${BENCH_MONITOR_INTERVAL}" \
            --output "${result_dir}/cluster_metrics.csv" >"${monitor_log}" 2>&1 &
    else
        "${SCRIPT_DIR}/monitor_cluster.sh" \
            --interval "${BENCH_MONITOR_INTERVAL}" \
            --output "${result_dir}/cluster_metrics.csv" >"${monitor_log}" 2>&1 &
    fi
    echo "$!"
}

stop_monitor() {
    local monitor_pid="$1"
    local result_dir="$2"

    [[ -z "${monitor_pid}" ]] && return 0
    if kill -0 "${monitor_pid}" 2>/dev/null; then
        echo "stopping monitor_cluster.sh pid=${monitor_pid}" >>"${result_dir}/run.log"
        if command -v setsid >/dev/null 2>&1; then
            kill -TERM -- "-${monitor_pid}" 2>/dev/null || kill -TERM "${monitor_pid}" 2>/dev/null || true
        else
            kill -TERM "${monitor_pid}" 2>/dev/null || true
        fi
        wait "${monitor_pid}" 2>/dev/null || true
    fi
}

cleanup_active_monitor() {
    if [[ -n "${ACTIVE_MONITOR_PID}" && -n "${ACTIVE_MONITOR_RESULT_DIR}" ]]; then
        stop_monitor "${ACTIVE_MONITOR_PID}" "${ACTIVE_MONITOR_RESULT_DIR}"
        ACTIVE_MONITOR_PID=""
        ACTIVE_MONITOR_RESULT_DIR=""
    fi
}

on_signal() {
    cleanup_active_monitor
    exit 130
}

plot_skew_if_possible() {
    local result_dir="$1"
    local skew_csv="${result_dir}/skew.csv"
    local skew_png="${result_dir}/skew.png"

    [[ "${ENABLE_PLOT}" == "false" ]] && return 0
    [[ -f "${skew_csv}" ]] || return 0

    local row_count
    row_count=$(awk 'END {print NR + 0}' "${skew_csv}")
    if [[ "${row_count}" -le 1 ]]; then
        echo "WARN: skip skew plot because ${skew_csv} has no reducer rows" >>"${result_dir}/run.log"
        return 0
    fi

    local mpl_config_dir="${TMPDIR:-/tmp}/r6-matplotlib-${USER:-bench}"
    mkdir -p "${mpl_config_dir}"
    if MPLCONFIGDIR="${mpl_config_dir}" python3 "${SCRIPT_DIR}/plot_skew.py" --input "${skew_csv}" --output "${skew_png}" >>"${result_dir}/run.log" 2>&1; then
        echo "wrote ${skew_png}" >>"${result_dir}/run.log"
    else
        echo "WARN: failed to generate skew.png; CSV outputs remain valid" >>"${result_dir}/run.log"
    fi
}

emit_cases() {
    echo "baseline 300 3 ${MID_REDUCERS}"
    if [[ "${BASELINE_ONLY}" == "false" ]]; then
        echo "delta180 180 3 ${MID_REDUCERS}"
        echo "delta600 600 3 ${MID_REDUCERS}"
        echo "k2 300 2 ${MID_REDUCERS}"
        echo "k5 300 5 ${MID_REDUCERS}"
        echo "reducers_low 300 3 ${LOW_REDUCERS}"
        echo "reducers_high 300 3 ${HIGH_REDUCERS}"
    fi
}

write_empty_metrics() {
    local dir="$1"
    # 即使真实 runner 暂时没有产出某类指标，也先写出文档约定的表头，
    # 保证每个 bench/results 子目录都能被报告脚本稳定扫描。
    [[ -f "${dir}/counters.csv" ]] || echo "job_id,job_name,stage,counter_group,counter_name,value" >"${dir}/counters.csv"
    [[ -f "${dir}/wall_clock.csv" ]] || echo "job_id,job_name,stage,submit_time_ms,launch_time_ms,finish_time_ms,wall_clock_ms" >"${dir}/wall_clock.csv"
    [[ -f "${dir}/skew.csv" ]] || echo "reducer_id,records,shuffle_bytes,wall_ms,status,attempt_id" >"${dir}/skew.csv"
    [[ -f "${dir}/skew_summary.csv" ]] || echo "metric,count,mean,p50,p95,max,max_mean_ratio" >"${dir}/skew_summary.csv"
    [[ -f "${dir}/cluster_metrics.csv" ]] || echo "timestamp,host,ok,load1,cpu_user_pct,cpu_system_pct,cpu_idle_pct,mem_total_kb,mem_available_kb,net_rx_bytes,net_tx_bytes,disk_read_kb,disk_write_kb,warning" >"${dir}/cluster_metrics.csv"
}

reducer_env_prefix() {
    local reducers="$1"
    # cluster_run.sh 从 scripts/env.sh 读取不同 phase 的 reducer 环境变量；
    # 扫表时只覆盖当前 phase，避免影响其它规模默认值。
    case "${PHASE}" in
        1d) echo "STAGE1_REDUCERS_1D=${reducers} STAGE2_REDUCERS_1D=${reducers}" ;;
        7d) echo "STAGE1_REDUCERS_7D=${reducers} STAGE2_REDUCERS_7D=${reducers}" ;;
        31d) echo "STAGE1_REDUCERS_31D=${reducers} STAGE2_REDUCERS_31D=${reducers}" ;;
    esac
}

run_case() {
    local label="$1"
    local delta="$2"
    local k_min="$3"
    local reducers="$4"
    local index="$5"
    local timestamp
    timestamp=$(date +%Y%m%d-%H%M%S)
    local run_id
    run_id=$(make_run_id "${timestamp}" "${index}")
    local result_dir="${RESULT_ROOT}/${PHASE}_${delta}_${k_min}_${reducers}_${timestamp}_pid$$_${index}_${label}"
    mkdir -p "${result_dir}"

    cat >"${result_dir}/params.env" <<EOF
phase=${PHASE}
days=${DAYS}
label=${label}
companion_delta_t=${delta}
companion_k_min=${k_min}
reducers=${reducers}
runner=${RUNNER}
run_id=${run_id}
dry_run=${DRY_RUN}
monitor_enabled=${ENABLE_MONITOR}
monitor_interval=${BENCH_MONITOR_INTERVAL}
plot_enabled=${ENABLE_PLOT}
EOF

    local reducer_env
    reducer_env=$(reducer_env_prefix "${reducers}")
    local cmd_desc="${reducer_env} BENCH_RESULT_DIR=${result_dir} BENCH_PHASE=${PHASE} BENCH_DELTA=${delta} BENCH_K=${k_min} BENCH_REDUCERS=${reducers} ${RUNNER} --days ${DAYS} --run-id ${run_id} -Dcompanion.delta.t=${delta} -Dcompanion.k.min=${k_min}"

    echo "== ${label}: ${PHASE} delta=${delta} k=${k_min} reducers=${reducers}"
    echo "+ ${cmd_desc}" >"${result_dir}/run.log"

    if [[ "${DRY_RUN}" == "true" ]]; then
        echo "(dry-run: not invoking runner)" >>"${result_dir}/run.log"
    else
        local monitor_pid
        monitor_pid=$(start_monitor "${result_dir}")
        ACTIVE_MONITOR_PID="${monitor_pid}"
        ACTIVE_MONITOR_RESULT_DIR="${result_dir}"

        # reducer_env 是由 reducer_env_prefix 生成的受控 "K=V K=V" 字符串；
        # 这里需要按空格展开给 env，不能整体加引号。
        # shellcheck disable=SC2086
        set +e
        env ${reducer_env} \
            BENCH_RESULT_DIR="${result_dir}" \
            BENCH_PHASE="${PHASE}" \
            BENCH_DELTA="${delta}" \
            BENCH_K="${k_min}" \
            BENCH_REDUCERS="${reducers}" \
            "${RUNNER}" --days "${DAYS}" \
            --run-id "${run_id}" \
            "-Dcompanion.delta.t=${delta}" \
            "-Dcompanion.k.min=${k_min}" >>"${result_dir}/run.log" 2>&1
        local runner_status=$?
        set -e

        stop_monitor "${monitor_pid}" "${result_dir}"
        ACTIVE_MONITOR_PID=""
        ACTIVE_MONITOR_RESULT_DIR=""
        if [[ "${runner_status}" -ne 0 ]]; then
            echo "ERROR: runner failed with status ${runner_status}" >>"${result_dir}/run.log"
            write_empty_metrics "${result_dir}"
            return "${runner_status}"
        fi

        fetch_history_for_run "${result_dir}" "${run_id}" >>"${result_dir}/run.log" 2>&1
    fi

    local history_args=()
    for history_file in "${result_dir}"/*.jhist; do
        [[ -e "${history_file}" ]] || continue
        history_args+=(--input "${history_file}")
    done
    if [[ ${#history_args[@]} -eq 0 && -f "${result_dir}/history.txt" ]]; then
        # history.txt 只用于 fake runner / 手写文本 fixture；真实集群产物按
        # bench/README.md 应放原始 `.jhist`，上面的循环会优先消费。
        history_args=(--input "${result_dir}/history.txt")
    fi
    if [[ ${#history_args[@]} -gt 0 ]]; then
        python3 "${SCRIPT_DIR}/parse_counters.py" \
            "${history_args[@]}" \
            --counters-out "${result_dir}/counters.csv" \
            --wall-out "${result_dir}/wall_clock.csv" \
            --reducer-attempts-out "${result_dir}/reducer_attempts.csv"
    fi
    if [[ -f "${result_dir}/reducer_attempts.csv" ]]; then
        python3 "${SCRIPT_DIR}/reducer_skew.py" \
            --input "${result_dir}/reducer_attempts.csv" \
            --detail-out "${result_dir}/skew.csv" \
            --summary-out "${result_dir}/skew_summary.csv"
    fi
    plot_skew_if_possible "${result_dir}"
    write_empty_metrics "${result_dir}"
}

validate_history_fetch_config
trap cleanup_active_monitor EXIT
trap on_signal INT TERM

case_index=0
emit_cases | while read -r label delta k_min reducers; do
    case_index=$((case_index + 1))
    run_case "${label}" "${delta}" "${k_min}" "${reducers}" "${case_index}"
done
