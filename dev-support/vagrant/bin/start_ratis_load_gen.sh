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

# Trivial script to call the Ratis example load gen from GNU Screen

HOME=/home/vagrant
peers=$1

cd ${HOME}/ratis

export QUORUM_OPTS="--peers $peers"
# run the load generator
./ratis-examples/src/main/bin/client.sh filestore loadgen --size 1048576 --numFiles 100 2>&1 | \
  tee ${HOME}/loadgen.log

# verify all logs checksum the same
echo "Verification of all Ratis file server logs have the same checksum across all storage directories:"
find ${HOME}/test_data/data? -type f -a -name 'file-*' -exec md5sum \{\} \+ | sed 's/ .*//' | sort | uniq -c
