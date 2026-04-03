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

# This script installs SpotBugs.
# Requires _install_tool from _lib.sh.  Use `source` for both scripts, because it modifies $PATH.

_get_spotbugs_version() {
  MAVEN_ARGS='' ${MVN} -q -DforceStdout -Dscan=false help:evaluate -Dexpression=spotbugs.version 2>/dev/null || echo '4.8.6'
}

if [[ -z "${SPOTBUGS_VERSION:-}" ]]; then
  SPOTBUGS_VERSION="$(_get_spotbugs_version)"
fi

_install_spotbugs() {
  echo "https://repo.maven.apache.org/maven2/com/github/spotbugs/spotbugs/${SPOTBUGS_VERSION}/spotbugs-${SPOTBUGS_VERSION}.tgz"
  curl -LSs "https://repo.maven.apache.org/maven2/com/github/spotbugs/spotbugs/${SPOTBUGS_VERSION}/spotbugs-${SPOTBUGS_VERSION}.tgz" | tar -xz -f - || exit 1
  find "spotbugs-${SPOTBUGS_VERSION}"/bin -type f -print0 | xargs -0 --no-run-if-empty chmod +x
}

_install_tool spotbugs "spotbugs-${SPOTBUGS_VERSION}/bin"
