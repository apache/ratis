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

# Requires _install_tool from _lib.sh. Source both scripts to update PATH.
: "${INFER_VERSION:=1.3.0}"

_install_infer() {
  local platform
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) platform=linux-x86_64 ;;
    Darwin-arm64) platform=osx-arm64 ;;
    *)
      echo "Unsupported Infer platform: $(uname -s) $(uname -m)" >&2
      return 1
      ;;
  esac

  local archive="infer-${platform}-v${INFER_VERSION}.tar.xz"
  mkdir -p "infer-v${INFER_VERSION}" || return 1
  curl -fLSs "https://github.com/facebook/infer/releases/download/v${INFER_VERSION}/${archive}" \
    | tar -xJ -C "infer-v${INFER_VERSION}" --strip-components=1 -f -
}

_install_tool infer "infer-v${INFER_VERSION}/bin"
