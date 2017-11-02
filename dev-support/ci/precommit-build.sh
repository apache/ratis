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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/common.sh

YETUS_ARGS+=("--archive-list=checkstyle-errors.xml,findbugsXml.xml")
YETUS_ARGS+=("--basedir=${BASEDIR}")
YETUS_ARGS+=("--brief-report-file=${ARTIFACTS}/email-report.txt")
YETUS_ARGS+=("--build-url-artifacts=artifact/out")
YETUS_ARGS+=("--console-report-file=${ARTIFACTS}/console-report.txt")
YETUS_ARGS+=("--console-urls")
YETUS_ARGS+=("--docker")
YETUS_ARGS+=("--dockerfile=${WORKSPACE}/Dockerfile")
YETUS_ARGS+=("--dockermemlimit=20g")
YETUS_ARGS+=("--findbugs-strict-precheck")
YETUS_ARGS+=("--html-report-file=${ARTIFACTS}/console-report.html")
YETUS_ARGS+=("--jenkins")
YETUS_ARGS+=("--jira-password=${JIRA_PASSWORD}")
YETUS_ARGS+=("--jira-user=hadoopqa")
YETUS_ARGS+=("--multijdkdirs=/usr/lib/jvm/java-8-oracle")
YETUS_ARGS+=("--mvn-custom-repos")
YETUS_ARGS+=("--patch-dir=${ARTIFACTS}")
YETUS_ARGS+=("--project=ratis")
YETUS_ARGS+=("--personality=${WORKSPACE}/yetus-personality.sh")
YETUS_ARGS+=("--proclimit=5000")
YETUS_ARGS+=("--resetrepo")
YETUS_ARGS+=("--sentinel")
YETUS_ARGS+=("--shelldocs=/testptch/hadoop/dev-support/bin/shelldocs")
YETUS_ARGS+=("--skip-dir=dev-support")
YETUS_ARGS+=("--tests-filter=checkstyle,pylint,shelldocs")

YETUS_ARGS+=("RATIS-${ISSUE_NUM}")


/bin/bash ${TESTPATCHBIN} "${YETUS_ARGS[@]}"
