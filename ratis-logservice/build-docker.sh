#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

LOGSERVICE="$(dirname "$0")"
LOGSERVICE="$(cd "$LOGSERVICE">/dev/null; pwd)"

# Get the version of the project
VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"

# Validate that the tarball is there
if [[ ! -f "target/ratis-logservice-${VERSION}-bin.tar.gz" ]]; then
  echo "LogService assembly tarball missing, run 'mvn package assembly:single' first!"
  exit 1
fi

docker build -t ratis-logservice --build-arg BINARY=target/ratis-logservice-$VERSION-bin.tar.gz --build-arg VERSION=$VERSION .
