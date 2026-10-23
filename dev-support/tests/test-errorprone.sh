#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TEST_DIR=$(mktemp -d)
trap 'rm -rf "$TEST_DIR"' EXIT
export TEST_DIR
export OUTPUT_DIR="${TEST_DIR}/report"
export MAVEN="${TEST_DIR}/mvn"

cat > "$MAVEN" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" > "${TEST_DIR}/args"
# A clean build removes reports created before Maven runs.
rm -rf "$OUTPUT_DIR"
cat "${TEST_DIR}/stdout"
cat "${TEST_DIR}/stderr" >&2
exit "${MAVEN_EXIT_CODE}"
EOF
chmod +x "$MAVEN"

run_check() {
  local expected=$1
  local actual=0
  bash "${DIR}/../checks/errorprone.sh" -Dtest.option=true > "${TEST_DIR}/output" 2>&1 || actual=$?
  if [[ "$actual" -ne "$expected" ]]; then
    cat "${TEST_DIR}/output"
    echo "Expected exit $expected, got $actual" >&2
    exit 1
  fi
}

export MAVEN_EXIT_CODE=1
cat > "${TEST_DIR}/stdout" <<'EOF'
[WARNING] /src/Legacy.java:[1,1] [JdkObsolete] warning finding
[ERROR] /src/Format.java:[2,1] [FormatString] error finding
[ERROR] /src/Format.java:[2,1] [FormatString] error finding
[ERROR] Failed to execute goal compiler:compile
EOF
cat > "${TEST_DIR}/stderr" <<'EOF'
/src/Raw.java:3: warning: [Custom-Rule_1] raw warning
/src/Raw.java:4: error: [RawError] raw error
EOF
run_check 1
[[ $(wc -l < "$OUTPUT_DIR/diagnostics.txt") -eq 4 ]]
[[ $(wc -l < "$OUTPUT_DIR/summary.txt") -eq 2 ]]
[[ $(cat "$OUTPUT_DIR/failures") -eq 2 ]]
grep -q 'JdkObsolete' "$OUTPUT_DIR/diagnostics.txt"
grep -q 'RawError' "$OUTPUT_DIR/summary.txt"
! grep -q 'warning\|WARNING\|Failed to execute' "$OUTPUT_DIR/summary.txt"
for arg in -Perrorprone -DskipTests clean test-compile -Dtest.option=true; do
  grep -qx -- "$arg" "${TEST_DIR}/args"
done

export MAVEN_EXIT_CODE=0
printf '%s\n' '[WARNING] /src/Legacy.java:[1,1] [JdkObsolete] warning finding' > "${TEST_DIR}/stdout"
: > "${TEST_DIR}/stderr"
run_check 0
[[ $(wc -l < "$OUTPUT_DIR/diagnostics.txt") -eq 1 ]]
[[ ! -s "$OUTPUT_DIR/summary.txt" ]]
[[ $(cat "$OUTPUT_DIR/failures") -eq 0 ]]

export MAVEN_EXIT_CODE=1
printf '%s\n' '[ERROR] Dependency resolution failed' > "${TEST_DIR}/stdout"
run_check 1
[[ ! -s "$OUTPUT_DIR/diagnostics.txt" ]]
grep -qx 'Unknown failure, check output.log' "$OUTPUT_DIR/summary.txt"
[[ $(cat "$OUTPUT_DIR/failures") -eq 1 ]]

echo 'Error Prone check tests passed'
