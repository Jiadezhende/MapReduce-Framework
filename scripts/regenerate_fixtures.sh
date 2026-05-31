#!/usr/bin/env bash
# Regenerate canonical golden fixtures under tests/data/fixtures/ from tests/data/mini.csv.
# Runs a single-process reference implementation of Stage 0/1/2 (companion.io.FixtureGenerator).
# Stage 1 here is the IDEAL semantics (group by loc only, no boundary loss); see
# docs/reference-semantics.md for the spec both this generator and baseline/single_machine.py
# implement against.
#
# Re-run after:
#   - changing byte layout of RecordWritable / PairKey / LocSlotWritable
#   - changing Stage 0/1/2 semantics declared in docs/reference-semantics.md or docs/fixtures.md
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

# Drop stale Hadoop ChecksumFileSystem sidecars before regen — FixtureGenerator
# writes via RawLocalFileSystem (no .crc), so any pre-existing .crc would
# silently desync from the new .seq and break local tests with ChecksumException.
find tests/data/fixtures/ -name '.*.crc' -delete

java -cp "$CP" companion.io.FixtureGenerator "$(pwd)"

echo
echo "regenerated:"
ls -la tests/data/fixtures/*.seq tests/data/fixtures/companions.csv
