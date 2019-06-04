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
set -e
mkdir -p build
rat_version="0.13"
filename="apache-rat-${rat_version}-bin.tar.gz"
artifact="creadur/apache-rat-${rat_version}/${filename}"
if [ ! -f "$DIR/build/${filename}" ]; then
  echo "RAT installation missing, download to build/"
  curl -L --fail -o "${DIR}/build/${filename}" "https://www.apache.org/dyn/closer.lua?filename=${artifact}&action=download"
  curl -L --fail -o "${DIR}/build/${filename}.sha512" "https://dist.apache.org/repos/dist/release/${artifact}.sha512"
fi

if [ ! -d "$DIR/build/apache-rat-${rat_version}" ]; then
  echo "Unpacked RAT installation missing, validating download RAT release using checksum"
  pushd ${DIR}/build >/dev/null
  gpg --print-md SHA512 ${filename} | diff ${filename}.sha512 -
  if [[ $? -ne 0 ]]; then
    echo "Failed to validate checksum of ${filename}"
    # Cleanup before exiting to avoid this stuff hanging around that is untrusted
    rm ${DIR}/build/${filename}
    rm ${DIR}/build/${filename}.sha512
    exit 2
  fi
  popd >/dev/null
  # Only now is it safe to extract this
  tar zxf build/${filename} -C build/
fi

echo "Running RAT license check"
output=$(java -jar $DIR/build/apache-rat-${rat_version}/apache-rat-${rat_version}.jar -d $DIR -E rat-excludes.txt)
if [[ ! $(echo "$output" | grep '0 Unknown Licenses') ]]; then
  echo 'RAT check appears to have failed, inspect its output:'
  echo "$output"
  exit 1
else
  echo "RAT check appears to have passed"
fi

HUGO_EXEC=$(which hugo)
if [ "$?" -ne 0 ]; then
      echo "Please install hugo and put it to the path"
		exit 1
fi
echo -e "\nBuilding website to public/"
$HUGO_EXEC -d public
