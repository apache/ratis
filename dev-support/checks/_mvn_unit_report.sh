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

REPORT_DIR=${REPORT_DIR:-$PWD}

_realpath() {
  if realpath "$@" > /dev/null; then
    realpath "$@"
  else
    local relative_to
    relative_to=$(realpath "${1/--relative-to=/}") || return 1
    realpath "$2" | sed -e "s@${relative_to}/@@"
  fi
}

## generate summary txt file
find "." -not -path '*/iteration*' -name 'TEST*.xml' -print0 \
    | xargs -n1 -0 "grep" -l -E "<failure|<error" \
    | awk -F/ '{sub("'"TEST-"'",""); sub(".xml",""); print $NF}' \
    | tee "$REPORT_DIR/summary.txt"

#Copy heap dump and dump leftovers
find "." -not -path '*/iteration*' \
    \( -name "*.hprof" \
    -or -name "*.dump" \
    -or -name "*.dumpstream" \
    -or -name "hs_err_*.log" \) \
  -exec mv {} "$REPORT_DIR/" \;

## Add the tests where the JVM is crashed
grep -A1 'Crashed tests' "${REPORT_DIR}/output.log" \
  | grep -v -e 'Crashed tests' -e '--' \
  | cut -f2- -d' ' \
  | sort -u \
  | tee -a "${REPORT_DIR}/summary.txt"

# Check for tests that started but were not finished
if grep -q 'There was a timeout or other error in the fork' "${REPORT_DIR}/output.log"; then
  diff -uw \
    <(grep -e 'Running org' "${REPORT_DIR}/output.log" \
      | sed -e 's/.* \(org[^ ]*\)/\1/' \
      | uniq -c \
      | sort -u -k2) \
    <(grep -e 'Tests run: .* in org' "${REPORT_DIR}/output.log" \
      | sed -e 's/.* \(org[^ ]*\)/\1/' \
      | uniq -c \
      | sort -u -k2) \
    | grep '^- ' \
    | awk '{ print $3 }' \
    | tee -a "${REPORT_DIR}/summary.txt"
fi

#Collect of all of the report files of FAILED tests
for failed_test in $(< ${REPORT_DIR}/summary.txt); do
  for file in $(find "." -not -path '*/iteration*' \
      \( -name "${failed_test}.txt" -or -name "${failed_test}-output.txt" -or -name "TEST-${failed_test}.xml" \)); do
    dir=$(dirname "${file}")
    dest_dir=$(_realpath --relative-to="${PWD}" "${dir}/../..") || continue
    mkdir -p "${REPORT_DIR}/${dest_dir}"
    mv "${file}" "${REPORT_DIR}/${dest_dir}"/
  done
done

## Check if Maven was killed
if grep -q "Killed.* ${MVN} .* test " "${REPORT_DIR}/output.log"; then
  echo 'Maven test run was killed' >> "${REPORT_DIR}/summary.txt"
fi

## generate summary markdown file
export SUMMARY_FILE="$REPORT_DIR/summary.md"
for TEST_RESULT_FILE in $(find "$REPORT_DIR" -name "*.txt" | grep -v output); do

    FAILURES=$(grep FAILURE "$TEST_RESULT_FILE" | grep "Tests run" | awk '{print $18}' | sort | uniq)

    for FAILURE in $FAILURES; do
        TEST_RESULT_LOCATION="$(_realpath --relative-to="$REPORT_DIR" "$TEST_RESULT_FILE")"
        TEST_OUTPUT_LOCATION="${TEST_RESULT_LOCATION//.txt/-output.txt}"
        printf " * [%s](%s) ([output](%s))\n" "$FAILURE" "$TEST_RESULT_LOCATION" "$TEST_OUTPUT_LOCATION" >> "$SUMMARY_FILE"
    done
done

if [ -s "$SUMMARY_FILE" ]; then
   printf "# Failing tests: \n\n" | cat - "$SUMMARY_FILE" > temp && mv temp "$SUMMARY_FILE"
fi

## generate counter
wc -l "$REPORT_DIR/summary.txt" | awk '{print $1}'> "$REPORT_DIR/failures"
