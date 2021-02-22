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

# Trivial script to call the Ratis example server from GNU Screen

HOME=/home/vagrant
storage=$1
id=$2
peers=$3

cd ${HOME}/ratis/
./ratis-examples/src/main/bin/server.sh filestore server --storage $storage --id $id --peers $peers 2>&1 | \
  tee ${HOME}/server_${id}.log
