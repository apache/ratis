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

USAGE="start-all.sh <example> <subcommand>"

if [ "$#" -ne 2 ]; then
  echo "$USAGE"
  exit 1
fi

source $DIR/common.sh

# One of the examples, e.g. "filestore" or "arithmetic"
example="$1"
shift

subcommand="$1"
shift

# Find a tmpdir, defaulting to what the environment tells us
tmp="${TMPDIR:-/tmp}"

echo "Starting 3 Ratis servers with '${example}' with directories in '${tmp}' as local storage"

# The ID needs to be kept in sync with QUORUM_OPTS
$DIR/server.sh "$example" "$subcommand" --id n0 --storage "${tmp}/n0" $QUORUM_OPTS &
$DIR/server.sh "$example" "$subcommand" --id n1 --storage "${tmp}/n1" $QUORUM_OPTS &
$DIR/server.sh "$example" "$subcommand" --id n2 --storage "${tmp}/n2" $QUORUM_OPTS &

echo "Waiting for the servers"
