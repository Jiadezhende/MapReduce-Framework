#!/usr/bin/env bash
# Regenerate canonical golden fixtures under tests/data/fixtures/ from tests/data/mini.csv.
# Runs a single-process reference implementation of Stage 0/1/2 (companion.io.FixtureGenerator).
#
# Re-run after:
#   - changing byte layout of RecordWritable / PairKey / LocSlotWritable
#   - changing Stage 0/1/2 semantics declared in docs/fixtures.md
#   - changing companion.t0 / delta.t / slot.size / k.min defaults

set -euo pipefail

cd "$(dirname "$0")/.."

mvn -pl common -am -q test-compile

CP_FILE=$(mktemp)
trap 'rm -f "$CP_FILE"' EXIT
mvn -pl common -q dependency:build-classpath \
    -DincludeScope=test \
    -Dmdep.outputFile="$CP_FILE"

CP="common/target/test-classes:common/target/classes:$(cat "$CP_FILE")"

java -cp "$CP" companion.io.FixtureGenerator "$(pwd)"

echo
echo "regenerated:"
ls -la tests/data/fixtures/*.seq tests/data/fixtures/companions.csv
