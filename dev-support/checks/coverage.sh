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

# This script merges combined jacoco files (output of unit.sh)
# and generates a report in HTML and XML formats

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR/../.." || exit 1

source "${DIR}/../find_maven.sh"

REPORT_DIR="$DIR/../../target/coverage"

mkdir -p "$REPORT_DIR"

JACOCO_VERSION=$(${MVN} help:evaluate -Dexpression=jacoco.version -q -DforceStdout)

#Install jacoco cli
${MVN} --non-recursive --no-transfer-progress \
  org.apache.maven.plugins:maven-dependency-plugin:3.6.1:copy \
  -Dartifact=org.jacoco:org.jacoco.cli:${JACOCO_VERSION}:jar:nodeps

jacoco() {
  java -jar target/dependency/org.jacoco.cli-${JACOCO_VERSION}-nodeps.jar "$@"
}

# merge all jacoco-combined.exec files
jacoco merge $(find target -name jacoco-combined.exec) --destfile "$REPORT_DIR/jacoco-all.exec"

rm -fr target/coverage-classes target/coverage-sources
mkdir -p target/coverage-classes target/coverage-sources

# unzip all classes from the build
find ratis-assembly/target/apache-ratis* -name 'ratis-*.jar' \
  | grep -v -E 'examples|proto|test|thirdparty' \
  | xargs -n1 unzip -o -q -d target/coverage-classes

# copy all sources
for d in $(find ratis* -type d -name java); do
  cp -r "$d"/org target/coverage-sources/
done

# generate the reports
jacoco report "$REPORT_DIR/jacoco-all.exec" \
  --sourcefiles target/coverage-sources \
  --classfiles target/coverage-classes \
  --html "$REPORT_DIR/all" \
  --xml "$REPORT_DIR/all.xml"
