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

set -u -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../.." || exit 1

source "${DIR}/../find_maven.sh"

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../target/infer"}
REPORT_FILE="$REPORT_DIR/summary.txt"
OUTPUT_LOG=$(mktemp) || exit 1
analysis_complete=false

save_results() {
  local rc=$?
  mkdir -p "$REPORT_DIR" || exit 1
  if ! mv "$OUTPUT_LOG" "${REPORT_DIR}/output.log"; then
    rc=1
    analysis_complete=false
  fi
  if [[ ${rc} -ne 0 && "$analysis_complete" != true ]]; then
    echo "Infer check failed. See output.log for details." >> "$REPORT_FILE"
    echo 1 > "${REPORT_DIR}/failures"
  fi
  exit "${rc}"
}
trap save_results EXIT

mkdir -p "$REPORT_DIR" || exit 1
rm -f "${REPORT_DIR}/report.txt" "${REPORT_DIR}/report.json" "${REPORT_DIR}/captured-sources.txt" \
  infer-out/report.txt infer-out/report.json || exit 1
: > "$REPORT_FILE"
echo 0 > "${REPORT_DIR}/failures"

source "${DIR}/_lib.sh"
source "${DIR}/install/infer.sh" >> "$OUTPUT_LOG" 2>&1

MAVEN_OPTIONS='-B -DskipTests -Djacoco.skip --no-transfer-progress'
MAVEN_OPTIONS+=' -Ddevelocity.cache.local.enabled=false -Ddevelocity.cache.remote.enabled=false'

# Keep the log outside target because clean removes it. Preserve Maven's Java release settings.
#shellcheck disable=SC2086
infer run --java-version-reset --report-console-limit 0 \
  --skip-analysis-in-path '.*/src/test/.*' \
  --skip-analysis-in-path '.*/target/generated-test-sources/.*' \
  --skip-analysis-in-path '.*/target/generated-sources/.*' \
  -- "${MVN}" ${MAVEN_OPTIONS} clean test-compile "$@" 2>&1 | tee -a "$OUTPUT_LOG"
rc=$?

# Infer stores Maven output separately instead of forwarding it to the console.
for capture_log in infer-out/tmp/*stdout; do
  if [[ -f "$capture_log" ]]; then
    cat "$capture_log" >> "$OUTPUT_LOG" || exit 1
  fi
done

mkdir -p "$REPORT_DIR" || exit 1
for report in report.txt report.json logs stats; do
  if [[ -e "infer-out/${report}" ]]; then
    cp -R "infer-out/${report}" "${REPORT_DIR}/" || exit 1
  fi
done

if [[ ${rc} -ne 0 ]]; then
  echo "Infer exited with code ${rc}." >> "$REPORT_FILE"
  exit 1
fi
if [[ ! -f "${REPORT_DIR}/report.txt" || ! -f "${REPORT_DIR}/report.json" ]]; then
  echo "Infer analysis produced no report." >> "$REPORT_FILE"
  exit 1
fi
if ! infer debug --source-files > "${REPORT_DIR}/captured-sources.txt" 2>> "$OUTPUT_LOG" \
    || ! grep -q '[^[:space:]]' "${REPORT_DIR}/captured-sources.txt"; then
  echo "Infer captured no source files or could not read the capture." >> "$REPORT_FILE"
  exit 1
fi

issue_count=$(jq -es '
  if length == 1 and (.[0] | type) == "array" then
    [.[0][] | select(.suppressed != true and .censored_reason == null)] | length
  else error("Expected a single JSON report array") end
' "${REPORT_DIR}/report.json" 2>> "$OUTPUT_LOG") || {
  echo "Infer produced an invalid JSON report." >> "$REPORT_FILE"
  exit 1
}
echo "Infer analysis complete. Found ${issue_count} issues." | tee "$REPORT_FILE" || exit 1
echo "${issue_count}" > "${REPORT_DIR}/failures" || exit 1

analysis_complete=true
# Suppressed and censored findings are excluded above; every remaining finding blocks CI.
[[ ${issue_count} -eq 0 ]]
