#!/usr/bin/env bash
# 通过 SSH 采样轻量集群指标，输出 R6 文档约定的 cluster_metrics.csv。
#
# Usage:
#   bench/monitor_cluster.sh [--interval N] [--duration N] [--output FILE]
#
# 采样目标来自 BENCH_HOSTS。脚本只要求本地能 ssh 到 BENCH_MONITOR_GATEWAY
# （默认 scripts/env.sh 里的 MASTER_HOST），worker 采样由 gateway 在集群内转发。

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
# shellcheck source=../scripts/env.sh
source "${REPO_ROOT}/scripts/env.sh"

INTERVAL=5
DURATION=0
OUTPUT="cluster_metrics.csv"
: "${BENCH_SSH_CONNECT_TIMEOUT:=15}"

usage() {
    sed -n '2,8p' "$0"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --interval) INTERVAL="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --output) OUTPUT="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "ERROR: unknown arg $1" >&2; usage; exit 2 ;;
    esac
done

if ! [[ "${INTERVAL}" =~ ^[0-9]+$ ]] || [[ "${INTERVAL}" -lt 1 ]]; then
    echo "ERROR: --interval must be a positive integer" >&2
    exit 2
fi
if ! [[ "${DURATION}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --duration must be a non-negative integer" >&2
    exit 2
fi
if ! [[ "${BENCH_SSH_CONNECT_TIMEOUT}" =~ ^[0-9]+$ ]] || [[ "${BENCH_SSH_CONNECT_TIMEOUT}" -lt 1 ]]; then
    echo "ERROR: BENCH_SSH_CONNECT_TIMEOUT must be a positive integer" >&2
    exit 2
fi

HOSTS="${BENCH_HOSTS:-master worker1 worker2}"
GATEWAY_HOST="${BENCH_MONITOR_GATEWAY:-${MASTER_HOST}}"
START_EPOCH=$(date +%s)

mkdir -p "$(dirname "${OUTPUT}")"
if [[ ! -f "${OUTPUT}" ]]; then
    echo "timestamp,host,ok,load1,cpu_user_pct,cpu_system_pct,cpu_idle_pct,mem_total_kb,mem_available_kb,net_rx_bytes,net_tx_bytes,disk_read_kb,disk_write_kb,warning" >"${OUTPUT}"
fi

csv_escape() {
    local value="$1"
    value=${value//$'\r'/ }
    value=${value//$'\n'/ }
    value=${value//\"/\"\"}
    printf '"%s"' "${value}"
}

remote_script='
read_cpu() {
  awk "/^cpu / {print \$2, \$4, \$5}" /proc/stat
}
# CPU 采样用 1 秒前后两次 /proc/stat 做差，避免单点累计值误判。
set -- $(read_cpu 2>/dev/null || echo "0 0 0")
u1=$1; s1=$2; i1=$3
t1=$((u1 + s1 + i1))
sleep 1
set -- $(read_cpu 2>/dev/null || echo "0 0 0")
u2=$1; s2=$2; i2=$3
t2=$((u2 + s2 + i2))
dt=$((t2 - t1))
if [ "$dt" -gt 0 ]; then
  cpu_user=$(awk -v v=$((u2-u1)) -v t=$dt "BEGIN {printf \"%.2f\", 100*v/t}")
  cpu_system=$(awk -v v=$((s2-s1)) -v t=$dt "BEGIN {printf \"%.2f\", 100*v/t}")
  cpu_idle=$(awk -v v=$((i2-i1)) -v t=$dt "BEGIN {printf \"%.2f\", 100*v/t}")
else
  cpu_user=""; cpu_system=""; cpu_idle=""
fi
load1=$(awk "{print \$1}" /proc/loadavg 2>/dev/null || true)
mem_total=$(awk "/^MemTotal:/ {print \$2}" /proc/meminfo 2>/dev/null || true)
mem_available=$(awk "/^MemAvailable:/ {print \$2}" /proc/meminfo 2>/dev/null || true)
net=$(awk -F"[: ]+" '\''$2 != "lo" && NF > 10 {rx += $3; tx += $11} END {printf "%s %s", rx+0, tx+0}'\'' /proc/net/dev 2>/dev/null || echo "0 0")
set -- $net
net_rx=$1; net_tx=$2
disk=$(awk '\''$3 !~ /^(loop|ram)/ {read += $6; write += $10} END {printf "%d %d", read*512/1024, write*512/1024}'\'' /proc/diskstats 2>/dev/null || echo "0 0")
set -- $disk
disk_read=$1; disk_write=$2
printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" "$load1" "$cpu_user" "$cpu_system" "$cpu_idle" "$mem_total" "$mem_available" "$net_rx" "$net_tx" "$disk_read" "$disk_write"
'
remote_script_b64=$(printf "%s" "${remote_script}" | base64 | tr -d "\n")

sample_command_for_host() {
    local host="$1"
    local target_cmd="printf %s ${remote_script_b64} | base64 -d | bash"

    # master 本机指标直接在 gateway 上读 /proc；worker 指标由 gateway
    # 再 ssh 到 worker 读 /proc，避免要求本地能直连 worker 内网地址。
    if [[ "${host}" == "${GATEWAY_HOST}" || "${host}" == "master" ]]; then
        printf "%s" "${target_cmd}"
        return 0
    fi

    local q_host
    printf -v q_host "%q" "${host}"
    printf "ssh -o BatchMode=yes -o ConnectTimeout=%s %s '%s'" "${BENCH_SSH_CONNECT_TIMEOUT}" "${q_host}" "${target_cmd}"
}

while true; do
    timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    for host in ${HOSTS}; do
        stdout_file=$(mktemp)
        stderr_file=$(mktemp)
        remote_cmd=$(sample_command_for_host "${host}")
        if ssh -o BatchMode=yes -o ConnectTimeout="${BENCH_SSH_CONNECT_TIMEOUT}" "${GATEWAY_HOST}" "${remote_cmd}" >"${stdout_file}" 2>"${stderr_file}"; then
            sample=$(awk 'NF {line=$0} END {print line}' "${stdout_file}")
            warning=$(csv_escape "$(cat "${stderr_file}")")
            if [[ -n "${sample}" ]]; then
                echo "${timestamp},${host},1,${sample},${warning}" >>"${OUTPUT}"
            else
                warning=$(csv_escape "empty metric output $(cat "${stderr_file}")")
                echo "${timestamp},${host},0,,,,,,,,,,,${warning}" >>"${OUTPUT}"
            fi
        else
            # SSH 失败也写一行，报告里能区分“节点不可达”和“没有采样”。
            warning=$(csv_escape "$(cat "${stdout_file}" "${stderr_file}")")
            echo "${timestamp},${host},0,,,,,,,,,,,${warning}" >>"${OUTPUT}"
        fi
        rm -f "${stdout_file}" "${stderr_file}"
    done

    if [[ "${DURATION}" -gt 0 ]]; then
        now=$(date +%s)
        elapsed=$((now - START_EPOCH))
        if [[ "${elapsed}" -ge "${DURATION}" ]]; then
            break
        fi
    fi
    sleep "${INTERVAL}"
done
