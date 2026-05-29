#!/usr/bin/env bash
set -euo pipefail

REPO=$(pwd)
OUT=/tmp/local_baseline_mirror.csv
REPORT=/tmp/baseline_mirror_report.json

python3 baseline/single_machine.py --input tests/data/mini.csv --output ${OUT} --mirror-stage1-limits
python3 baseline/diff_baseline.py --mr-output tests/data/fixtures/companions.csv --baseline-output ${OUT} --report ${REPORT} --phase mini

cat ${REPORT}

# check for missing/extra
python3 - <<PY
import json
r=json.load(open('${REPORT}'))
missing=r.get('missing_in_mr', [])
extra=r.get('extra_in_mr', [])
if missing or extra:
    print('Integration check FAILED: differences found')
    print('missing:', len(missing))
    print('extra:', len(extra))
    raise SystemExit(2)
print('Integration check PASSED: baseline matches fixtures')
PY
