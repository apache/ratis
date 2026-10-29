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
mkdir -p "${TEST_DIR}/project/dev-support/checks/install" "${TEST_DIR}/bin"
cp "${DIR}/../checks/infer.sh" "${DIR}/../checks/_lib.sh" "${TEST_DIR}/project/dev-support/checks/"
cp "${DIR}/../checks/install/infer.sh" "${TEST_DIR}/project/dev-support/checks/install/"
cp "${DIR}/../find_maven.sh" "${TEST_DIR}/project/dev-support/"
export PATH="${TEST_DIR}/bin:${PATH}"
export MAVEN="${TEST_DIR}/bin/mvn"
export RATIS_PREFER_LOCAL_TOOL=true
export OUTPUT_DIR="${TEST_DIR}/project/target/infer"
export TOOLS_DIR="${TEST_DIR}/tools"

cat > "$MAVEN" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" > "${TEST_DIR}/maven-args"
rm -rf target
echo 'Maven output'
echo 'Maven stderr' >&2
exit "${MAVEN_EXIT_CODE:-0}"
EOF

cat > "${TEST_DIR}/bin/infer" <<'EOF'
#!/usr/bin/env bash
if [[ "$1" == debug ]]; then
  if [[ "${INFER_EMPTY_CAPTURE:-false}" != true ]]; then echo src/main/java/Example.java; fi
  exit "${INFER_DEBUG_EXIT_CODE:-0}"
fi
printf '%s\n' "$@" > "${TEST_DIR}/infer-args"
while [[ "$1" != -- ]]; do shift; done
shift
mkdir -p infer-out/tmp
"$@" > infer-out/tmp/maven.stdout 2>&1 || exit $?
if [[ "${INFER_NO_REPORT:-false}" != true ]]; then
  mkdir -p infer-out/stats
  printf '%s\n' 'Infer report' > infer-out/report.txt
  printf '%s\n' "${INFER_JSON-[]}" > infer-out/report.json
  echo 'Infer log' > infer-out/logs
  echo '{}' > infer-out/stats/analysis.json
  touch infer-out/capture.db
fi
echo 'Infer output'
if [[ -n "${INFER_REPORT_WRITE_FAILURE:-}" ]]; then
  mkdir -p "${OUTPUT_DIR}/${INFER_REPORT_WRITE_FAILURE}"
fi
exit "${INFER_EXIT_CODE:-0}"
EOF
chmod +x "$MAVEN" "${TEST_DIR}/bin/infer"

run_check() {
  local expected=$1
  local actual=0
  bash "${TEST_DIR}/project/dev-support/checks/infer.sh" -Dtest.option=true > "${TEST_DIR}/output" 2>&1 || actual=$?
  if [[ "$actual" -ne "$expected" ]]; then
    cat "${TEST_DIR}/output"
    echo "Expected exit $expected, got $actual" >&2
    exit 1
  fi
}

assert_failure() {
  grep -q "$1" "${OUTPUT_DIR}/summary.txt"
  [[ $(cat "${OUTPUT_DIR}/failures") -eq 1 ]]
}

run_check 0
[[ $(cat "${OUTPUT_DIR}/failures") -eq 0 ]]
grep -qx 'Infer analysis complete. Found 0 issues.' "${OUTPUT_DIR}/summary.txt"
grep -q 'Maven output' "${OUTPUT_DIR}/output.log"
grep -q 'Maven stderr' "${OUTPUT_DIR}/output.log"
grep -q 'Infer output' "${OUTPUT_DIR}/output.log"
[[ -s "${OUTPUT_DIR}/captured-sources.txt" ]]
[[ -f "${OUTPUT_DIR}/logs" && -f "${OUTPUT_DIR}/stats/analysis.json" ]]
[[ ! -e "${OUTPUT_DIR}/capture.db" ]]
for arg in clean test-compile -DskipTests -Djacoco.skip -Ddevelocity.cache.local.enabled=false -Ddevelocity.cache.remote.enabled=false -Dtest.option=true; do
  grep -qx -- "$arg" "${TEST_DIR}/maven-args"
done
grep -qx -- '--java-version-reset' "${TEST_DIR}/infer-args"
grep -qx -- '--skip-analysis-in-path' "${TEST_DIR}/infer-args"

export INFER_JSON='[{"severity":"ERROR"},{"severity":"WARNING"},{"suppressed":true},{"censored_reason":"filtered"}]'
run_check 1
[[ $(cat "${OUTPUT_DIR}/failures") -eq 2 ]]
grep -qx 'Infer analysis complete. Found 2 issues.' "${OUTPUT_DIR}/summary.txt"
! grep -q 'Infer check failed' "${OUTPUT_DIR}/summary.txt"
cmp "${TEST_DIR}/project/infer-out/report.json" "${OUTPUT_DIR}/report.json"

export INFER_JSON='[{"suppressed":true},{"censored_reason":"filtered"}]'
run_check 0
[[ $(cat "${OUTPUT_DIR}/failures") -eq 0 ]]
grep -qx 'Infer analysis complete. Found 0 issues.' "${OUTPUT_DIR}/summary.txt"
unset INFER_JSON

export INFER_EXIT_CODE=5
run_check 1
assert_failure 'Infer exited with code 5'
[[ -f "${OUTPUT_DIR}/report.json" ]]
unset INFER_EXIT_CODE

export MAVEN_EXIT_CODE=3
run_check 1
assert_failure 'Infer exited with code 3'
[[ ! -e "${OUTPUT_DIR}/report.json" ]]
unset MAVEN_EXIT_CODE

export INFER_NO_REPORT=true
run_check 1
assert_failure 'no report'
[[ ! -e "${OUTPUT_DIR}/report.json" ]]
unset INFER_NO_REPORT

export INFER_EMPTY_CAPTURE=true
run_check 1
assert_failure 'no source files'
unset INFER_EMPTY_CAPTURE

export INFER_DEBUG_EXIT_CODE=1
run_check 1
assert_failure 'no source files'
unset INFER_DEBUG_EXIT_CODE

for report in 'invalid JSON' '' '[] []' '{}' 'null'; do
  export INFER_JSON="$report"
  run_check 1
  assert_failure 'invalid JSON report'
done
unset INFER_JSON

for report in summary.txt failures; do
  export INFER_REPORT_WRITE_FAILURE="$report"
  run_check 1
  rmdir "${OUTPUT_DIR}/${report}"
done
unset INFER_REPORT_WRITE_FAILURE

cat > "${TEST_DIR}/bin/curl" <<'EOF'
#!/usr/bin/env bash
echo 'download failed' >&2
exit 22
EOF
chmod +x "${TEST_DIR}/bin/curl"
export RATIS_PREFER_LOCAL_TOOL=false
run_check 1
assert_failure 'Failed to install infer'
grep -q 'download failed' "${OUTPUT_DIR}/output.log"
! grep -q 'unbound variable' "${OUTPUT_DIR}/output.log"

cat > "${TEST_DIR}/bin/uname" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  -s) echo "${TEST_OS}" ;;
  -m) echo "${TEST_ARCH}" ;;
esac
EOF
cat > "${TEST_DIR}/bin/curl" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" > "${TEST_DIR}/curl-args"
echo archive
EOF
cat > "${TEST_DIR}/bin/tar" <<'EOF'
#!/usr/bin/env bash
cat > /dev/null
while [[ "$1" != -C ]]; do shift; done
shift
mkdir -p "$1/bin"
cp "${TEST_DIR}/bin/infer" "$1/bin/infer"
EOF
chmod +x "${TEST_DIR}/bin/uname" "${TEST_DIR}/bin/tar"
export INFER_VERSION=1.3.0
for platform in linux-x86_64 osx-arm64; do
  case "$platform" in
    linux-x86_64) export TEST_OS=Linux TEST_ARCH=x86_64 ;;
    osx-arm64) export TEST_OS=Darwin TEST_ARCH=arm64 ;;
  esac
  export TOOLS_DIR="${TEST_DIR}/tools-${platform}"
  run_check 0
  grep -qx "https://github.com/facebook/infer/releases/download/v1.3.0/infer-${platform}-v1.3.0.tar.xz" "${TEST_DIR}/curl-args"
  [[ -x "${TOOLS_DIR}/infer/infer-v1.3.0/bin/infer" ]]
done

export TEST_OS=Linux TEST_ARCH=aarch64
export TOOLS_DIR="${TEST_DIR}/tools-unsupported"
run_check 1
assert_failure 'Failed to install infer'
grep -q 'Unsupported Infer platform: Linux aarch64' "${OUTPUT_DIR}/output.log"

echo 'Infer check tests passed'
