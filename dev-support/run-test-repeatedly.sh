#!/bin/bash
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

if [ $# -ne 1 ] ; then
  echo "Usage: $0 <TEST_PATTERN>"
  echo
  echo "TEST_PATTERN is something like TestRaftStream or TestRaftStream#testSimpleWrite"
  exit 1
fi

TEST_PATTERN=$1
TEST_NAME=`echo ${TEST_PATTERN} | cut -d# -f 1`

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "${DIR}/find_maven.sh"

set -ex

${MVN} clean

for i in `seq 1 99`;
do
  OUTDIR=${TEST_NAME}.${i}
  OUTF=${OUTDIR}/${OUTDIR}.txt
  mkdir ${OUTDIR}
  echo
  echo Running ${OUTDIR}
  echo
  time ${MVN} test -Dtest=${TEST_PATTERN} 2>&1 | tee ${OUTF}

  find */target/surefire-reports/ -name \*${TEST_NAME}\* | xargs -I{} cp {} ${OUTDIR}

  grep -e "BUILD SUCCESS" ${OUTF}
done
