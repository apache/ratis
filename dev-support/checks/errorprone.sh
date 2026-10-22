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

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../target/errorprone"}
OUTPUT_LOG=$(mktemp)
trap 'rm -f "$OUTPUT_LOG"' EXIT

MAVEN_OPTIONS='-B -fae -Perrorprone -DskipTests -Djacoco.skip'
# Cached compilation does not emit compiler diagnostics.
MAVEN_OPTIONS="$MAVEN_OPTIONS -Ddevelocity.cache.local.enabled=false"
# Timestamp prefixes would prevent matching diagnostics.
MAVEN_OPTIONS="$MAVEN_OPTIONS -Dorg.slf4j.simpleLogger.showDateTime=false"

#shellcheck disable=SC2086
${MVN} ${MAVEN_OPTIONS} clean test-compile "$@" 2>&1 | tee "$OUTPUT_LOG"
rc=$?

# Create reports after clean, which removes the default report directory.
mkdir -p "$REPORT_DIR"
mv "$OUTPUT_LOG" "${REPORT_DIR}/output.log"
trap - EXIT

MAVEN_DIAGNOSTIC_PATTERN='^\[(ERROR|WARNING)\] .*:\[[0-9]+,[0-9]+\] \[[^]]+\]'
JAVAC_DIAGNOSTIC_PATTERN='^.*:[0-9]+: (error|warning): \[[^]]+\]'
grep -E "${MAVEN_DIAGNOSTIC_PATTERN}|${JAVAC_DIAGNOSTIC_PATTERN}" "${REPORT_DIR}/output.log" \
  | awk '!seen[$0]++' > "${REPORT_DIR}/diagnostics.txt"

grep -E '^\[ERROR\]|:[0-9]+: error:' "${REPORT_DIR}/diagnostics.txt" > "${REPORT_DIR}/summary.txt" || true
if [[ ${rc} -ne 0 && ! -s "${REPORT_DIR}/summary.txt" ]]; then
  echo 'Unknown failure, check output.log' > "${REPORT_DIR}/summary.txt"
fi
wc -l < "${REPORT_DIR}/summary.txt" | tr -d ' ' > "${REPORT_DIR}/failures"

if [[ -s "${REPORT_DIR}/summary.txt" ]]; then
  exit 1
fi
exit ${rc}
