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

REPORT_FILE="$1"

: ${ITERATIONS:="1"}

declare -i ITERATIONS

rc=0

if [[ ! -e "${REPORT_FILE}" ]]; then
  echo "Report file missing, check logs for details"
  rc=255

elif [[ ${ITERATIONS} -gt 1 ]]; then
  cat "${REPORT_FILE}"

  if grep -q 'exit code: [^0]' "${REPORT_FILE}"; then
    rc=1
  fi

elif [[ -s "${REPORT_FILE}" ]]; then
  cat "${REPORT_FILE}"
  rc=1
fi

exit ${rc}
