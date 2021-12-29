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

# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "${this}")" && pwd -P)
script="$(basename -- "${this}")"
this="${common_bin}/${script}"

# convert relative path to absolute path
config_bin=$(dirname "${this}")
script=$(basename "${this}")
config_bin=$(cd "${config_bin}"; pwd)
this="${config_bin}/${script}"

# This will set the default installation for a tarball installation while os distributors can
# set system installation locations.
RATIS_SHELL_HOME=$(dirname $(dirname "${this}"))
RATIS_SHELL_ASSEMBLY_CLIENT_JAR="${RATIS_SHELL_HOME}/lib/shell/*"
RATIS_SHELL_CONF_DIR="${RATIS_SHELL_CONF_DIR:-${RATIS_SHELL_HOME}/conf}"
RATIS_SHELL_LOGS_DIR="${RATIS_SHELL_LOGS_DIR:-${RATIS_SHELL_HOME}/logs}"

if [[ -e "${RATIS_SHELL_CONF_DIR}/ratis-shell-env.sh" ]]; then
  . "${RATIS_SHELL_CONF_DIR}/ratis-shell-env.sh"
fi

# Check if java is found
if [[ -z "${JAVA}" ]]; then
  if [[ -n "${JAVA_HOME}" ]] && [[ -x "${JAVA_HOME}/bin/java" ]];  then
    JAVA="${JAVA_HOME}/bin/java"
  elif [[ -n "$(which java 2>/dev/null)" ]]; then
    JAVA=$(which java)
  else
    echo "Error: Cannot find 'java' on path or under \$JAVA_HOME/bin/. Please set JAVA_HOME in ratis-shell-env.sh or user bash profile."
    exit 1
  fi
fi

# Check Java version == 1.8 or == 11
JAVA_VERSION=$(${JAVA} -version 2>&1 | awk -F '"' '/version/ {print $2}')
JAVA_MAJORMINOR=$(echo "${JAVA_VERSION}" | awk -F. '{printf("%03d%03d",$1,$2);}')
JAVA_MAJOR=$(echo "${JAVA_VERSION}" | awk -F. '{printf("%03d",$1);}')
if [[ ${JAVA_MAJORMINOR} != 001008 && ${JAVA_MAJOR} != 011 ]]; then
  echo "Error: ratis-shell requires Java 8 or Java 11, currently Java $JAVA_VERSION found."
  exit 1
fi

RATIS_SHELL_CLIENT_CLASSPATH="${RATIS_SHELL_CONF_DIR}/:${RATIS_SHELL_CLASSPATH}:${RATIS_SHELL_ASSEMBLY_CLIENT_JAR}"

if [[ -n "${RATIS_SHELL_HOME}" ]]; then
  RATIS_SHELL_JAVA_OPTS+=" -Dratis.shell.home=${RATIS_SHELL_HOME}"
fi

RATIS_SHELL_JAVA_OPTS+=" -Dratis.shell.conf.dir=${RATIS_SHELL_CONF_DIR} -Dratis.shell.logs.dir=${RATIS_SHELL_LOGS_DIR} -Dratis.shell.user.logs.dir=${RATIS_SHELL_USER_LOGS_DIR}"

RATIS_SHELL_JAVA_OPTS+=" -Dlog4j.configuration=file:${RATIS_SHELL_CONF_DIR}/log4j.properties"
RATIS_SHELL_JAVA_OPTS+=" -Dorg.apache.jasper.compiler.disablejsr199=true"
RATIS_SHELL_JAVA_OPTS+=" -Djava.net.preferIPv4Stack=true"
RATIS_SHELL_JAVA_OPTS+=" -Dorg.apache.ratis.thirdparty.io.netty.allocator.useCacheForAllThreads=false"
