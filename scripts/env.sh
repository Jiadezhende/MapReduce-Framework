# shellcheck shell=bash
# Centralized environment for the companion pipeline.
# Sourced by every other script in scripts/.

: "${COMPANION_ROOT:=/companion}"
: "${HADOOP_CONF_DIR:=/etc/hadoop/conf}"
: "${HADOOP_BIN:=hadoop}"
: "${LOCAL_DATA_DIR:=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Submit-side knobs. Override in a per-user wrapper if needed.
: "${YARN_QUEUE:=default}"
: "${STAGE1_REDUCERS_1D:=8}"
: "${STAGE1_REDUCERS_7D:=32}"
: "${STAGE1_REDUCERS_31D:=128}"
: "${STAGE2_REDUCERS_1D:=8}"
: "${STAGE2_REDUCERS_7D:=32}"
: "${STAGE2_REDUCERS_31D:=128}"

export COMPANION_ROOT HADOOP_CONF_DIR HADOOP_BIN LOCAL_DATA_DIR YARN_QUEUE
export STAGE1_REDUCERS_1D STAGE1_REDUCERS_7D STAGE1_REDUCERS_31D
export STAGE2_REDUCERS_1D STAGE2_REDUCERS_7D STAGE2_REDUCERS_31D

# Resolve the freshly-built shaded jar for a given module.
companion_jar() {
    local module="$1"
    local jar
    jar=$(ls "${LOCAL_DATA_DIR}/${module}/target/${module}-"*.jar 2>/dev/null | head -n1)
    if [[ -z "${jar}" ]]; then
        echo "ERROR: no jar found under ${module}/target/. Did you run 'mvn package'?" >&2
        return 1
    fi
    echo "${jar}"
}
