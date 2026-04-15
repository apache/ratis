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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

LIB_DIR=${SCRIPT_DIR}/../lib

if [[ -d "$LIB_DIR" ]]; then
   #release directory layout
   LIB_DIR=`cd ${LIB_DIR} > /dev/null; pwd`
   ARTIFACT=`ls -1 ${LIB_DIR}/*.jar`
else
   #development directory layout
   EXAMPLES_DIR=${SCRIPT_DIR}/../../../
   if [[ -d "$EXAMPLES_DIR" ]]; then
      EXAMPLES_DIR=`cd ${EXAMPLES_DIR} > /dev/null; pwd`
   fi
   JAR_PREFIX=`basename ${EXAMPLES_DIR}`
   ARTIFACT=`ls -1 ${EXAMPLES_DIR}/target/${JAR_PREFIX}-*.jar | grep -v test | grep -v javadoc | grep -v sources | grep -v shaded`
   if [[ ! -f "$ARTIFACT" ]]; then
      echo "Jar file is missing. Please do a full build (mvn clean package -DskipTests) first."
      exit -1
   fi
fi

echo "Found ${ARTIFACT}"

QUORUM_OPTS="--peers n0:localhost:6000,n1:localhost:6001,n2:localhost:6002"

CONF_DIR="$DIR/../conf"
if [[ -d "${CONF_DIR}" ]]; then
  LOGGER_OPTS="-Dlog4j.configuration=file:${CONF_DIR}/log4j.properties"
else
  LOGGER_OPTS="-Dlog4j.configuration=file:${DIR}/../resources/log4j.properties"
fi


# for opentelemetry: release tarball uses lib/trace next to examples/;
# dev tree uses ratis-assembly/target/apache-ratis-*-bin/lib/trace after package.
if [[ -n "${OTEL_JAR:-}" && -f "${OTEL_JAR}" ]]; then
  : # use OTEL_JAR from environment
else
  otel_jar_candidates=()
  for _jar in "${SCRIPT_DIR}"/../../lib/trace/opentelemetry-javaagent*.jar; do
    [[ -f "${_jar}" ]] && otel_jar_candidates+=("${_jar}")
  done
  for _otel_trace_dir in "${SCRIPT_DIR}"/../../../../ratis-assembly/target/apache-ratis-*-bin/lib/trace; do
    [[ -d "${_otel_trace_dir}" ]] || continue
    for _jar in "${_otel_trace_dir}"/opentelemetry-javaagent*.jar; do
      [[ -f "${_jar}" ]] && otel_jar_candidates+=("${_jar}")
    done
  done
  if ((${#otel_jar_candidates[@]} > 0)); then
    OTEL_JAR="$(printf '%s\n' "${otel_jar_candidates[@]}" | sort -V | tail -n 1)"
  else
    echo "Warning: OpenTelemetry agent jar not found; OTEL disabled"
    OTEL_JAR=""
  fi
fi

OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-ratis.server}"
OTEL_DEFAULT_EXPORTER_OPTS="-Dotel.traces.exporter=logging \
-Dotel.metrics.exporter=none \
-Dotel.logs.exporter=logging"
# Unset or empty → default exporter flags (empty is not a supported override).
OTEL_EXPORTER_OPTS="${OTEL_EXPORTER_OPTS:-${OTEL_DEFAULT_EXPORTER_OPTS}}"

# Enable javaagent when OTEL_JAR resolves to a file (env or discovery above).
if [[ -n "${OTEL_JAR}" && -f "${OTEL_JAR}" ]]; then
  OTEL_OPTS="-javaagent:${OTEL_JAR} -Dotel.resource.attributes=service.name=${OTEL_SERVICE_NAME} ${OTEL_EXPORTER_OPTS} -Draft.otel.tracing.enabled=true"
else
  OTEL_OPTS=""
fi

echo "Using OTEL_OPTS: ${OTEL_OPTS}"