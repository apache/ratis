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

set -e

testvms="ratis-servers ratis-hdd-slowdown"

box_path=$(dirname ${BASH_SOURCE[0]})/ratistest.box

if [[ $1 == "build" ]]; then
  # build everything
  echo "============================================"
  echo "Building the ratisbuild VM:"
  echo "============================================"
  vagrant up ratis-build --provision
  vagrant halt ratis-build
  [ '!' -e $box_path ] && vagrant package ratis-build --output $box_path

  echo "============================================"
  echo "Building the test-suite VMs:"
  echo "============================================"
  for vm in $testvms; do
    echo "============================================"
    echo "Building test-suite VM: $vm"
    echo "============================================"
    vagrant up $vm --provision
    vagrant suspend $vm
  done
  clear
  echo "============================================"
  echo "Build complete"
  echo "Run vagrant resume <vm name> to start a particular environment"
  echo "Run vagrant ssh <vm name> to enter a particular environment"
  echo "============================================"
  vagrant status
elif [[ $1 == "clean" ]]; then
  echo "============================================"
  echo "Cleaning-up all test artifacts"
  echo "============================================"
  vagrant destroy -f ratis-build || true
  for vm in $testvms; do
    vagrant destroy -f $vm || true
  done
  vagrant box remove ratis-test || true
  rm -f $box_path
  echo "============================================"
  echo "Clean-up complete"
  echo "============================================"
else
  echo "$(basename $0): Usage: $(basename $0) build"
  echo "                       $(basename $0) clean"
fi
