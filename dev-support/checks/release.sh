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

set -e -u -o pipefail

# This script tests the local part of the release process.  It does not publish anything.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../.." || exit 1

: "${RATISVERSION:="0.0.1"}"
: "${RC:="-ci-test"}"
: "${STAGING_REPO_DIR:="/tmp/ratis.staging-repo"}"
: "${SVNDISTDIR:="/tmp/ratis.svn"}"
: "${USERID:="ratis-ci-not-for-release"}"

MVN_REPO_DIR="${HOME}/.m2/repository"

mkdir -p "${SVNDISTDIR}"

if [[ -z "${CODESIGNINGKEY:-}" ]]; then
  gpg --batch --passphrase '' --pinentry-mode loopback --quick-generate-key "${USERID}" rsa4096 default 1d
  CODESIGNINGKEY=$(gpg --list-keys --with-colons "${USERID}" | grep '^pub:' | cut -f5 -d:)
fi

git config user.email || git config user.email 'test@example.com'
git config user.name || git config user.name 'Test User'

export CODESIGNINGKEY MVN_REPO_DIR RATISVERSION RC SVNDISTDIR

export MAVEN_ARGS="--batch-mode"

dev-support/make_rc.sh 1-prepare-src
dev-support/make_rc.sh 2-verify-bin
dev-support/make_rc.sh 3-publish-mvn -DaltDeploymentRepository="local::default::file://${STAGING_REPO_DIR}"
dev-support/make_rc.sh 4-assembly
