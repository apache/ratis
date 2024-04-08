#!/usr/bin/env bash

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


# Script that assembles all you need to make an RC. Does build of the tar.gzs
# which it stashes into a dir above $(pwd) named for the script with a
# timestamp suffix. Deploys builds to maven.
#
# To finish, check what was build.  If good copy to people.apache.org and
# close the maven repos.  Call a vote.
#
# Presumes your settings.xml all set up so can sign artifacts published to mvn, etc.
set -e
# Set mvn and mvnopts
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "${DIR}/find_maven.sh"
mvnopts="-Xmx1g"
if [ "$MAVEN_OPTS" != "" ]; then
  mvnopts="${MAVEN_OPTS}"
fi

mvnGet() {
  ${MVN} -q -Dexec.executable="echo" -Dexec.args="\${${1}}" --non-recursive \
    org.codehaus.mojo:exec-maven-plugin:1.6.0:exec 2>/dev/null
}


# Check project name
projectname=$(mvnGet project.name)
projectversion=$(mvnGet project.version)
if [ "${projectname}" = "Apache Ratis" ]; then
  echo
  echo "Prepare release artifacts for $projectname ${projectversion}"
  echo
else
  echo "Unexpected project name \"${projectname}\"."
  echo
  echo "Please run this script ($0) under the root directory of Apache Ratis."
  exit 1;
fi

if [ ! "$RATISVERSION" ]; then
  echo "Please set the RATISVERSION environment variable (eg. export RATISVERSION=0.3.0)"
  exit 1
fi

if [ ! "$RC" ]; then
   echo "Please set the RC number. (eg. export RC=\"-rc2\")"
   exit 1
fi


# Set projectdir and archivedir
projectdir=$(pwd)
echo "Project dir: ${projectdir}"
WORKINGDIR="${projectdir}/../$(basename "${projectdir}").${RATISVERSION}${RC}"
echo "Archive dir: ${WORKINGDIR}"

# Set repodir
repodir=${MVN_REPO_DIR:-${projectdir}/../$(basename "${projectdir}").repository}
echo "Repo dir: ${repodir}"

SVNDISTDIR=${SVNDISTDIR:-$projectdir/../svndistratis}
if [ ! -d "$SVNDISTDIR" ]; then
  svn co https://dist.apache.org/repos/dist/dev/ratis "$SVNDISTDIR"
fi


if [ ! "$CODESIGNINGKEY" ]; then
  echo "Please specify your signing key ID in the CODESIGNINGKEY environment variable"
  exit 1
fi


mvnFun() {
  MAVEN_OPTS="${mvnopts}" ${MVN} -Dmaven.repo.local="${repodir}" "$@"
}

prepare-src() {
  cd "$projectdir"
  git reset --hard
  git clean -fdx
  mvnFun versions:set -DnewVersion="$RATISVERSION"
  git commit -a -m "Change version for the version $RATISVERSION $RC"

  git config user.signingkey "${CODESIGNINGKEY}"
  git tag -s -m "Release $RATISVERSION $RC" ratis-"${RATISVERSION}${RC}"
  git reset --hard ratis-"${RATISVERSION}${RC}"

  git clean -fdx

  #grep -r SNAPSHOT --include=pom.xml

  mvnFun clean install assembly:single -DskipTests=true  -Prelease -Papache-release -Dgpg.keyname="${CODESIGNINGKEY}"
}

prepare-bin() {
  echo "Cleaning up workingdir $WORKINGDIR"
  rm -rf "$WORKINGDIR"
  mkdir -p "$WORKINGDIR"
  cd "$WORKINGDIR"
  tar zvxf "$projectdir/ratis-assembly/target/apache-ratis-${RATISVERSION}-src.tar.gz"
  mv "apache-ratis-${RATISVERSION}-src" "apache-ratis-${RATISVERSION}"
  cd "apache-ratis-${RATISVERSION}"

  mvnFun clean install assembly:single -DskipTests=true  -Prelease -Papache-release -Dgpg.keyname="${CODESIGNINGKEY}"
}

assembly() {
  cd "$SVNDISTDIR"
  RCDIR="$SVNDISTDIR/${RATISVERSION}/${RC#-}"
  mkdir -p "$RCDIR"
  cd "$RCDIR"
  cp "$WORKINGDIR/apache-ratis-${RATISVERSION}/ratis-assembly/target/apache-ratis-${RATISVERSION}-bin.tar.gz" "apache-ratis-${RATISVERSION}-bin.tar.gz"
  cp "$projectdir/ratis-assembly/target/apache-ratis-${RATISVERSION}-src.tar.gz" "apache-ratis-${RATISVERSION}-src.tar.gz"
  for i in *.tar.gz; do gpg  -u "${CODESIGNINGKEY}" --armor --output "${i}.asc" --detach-sig "${i}"; done
  for i in *.tar.gz; do gpg --print-md SHA512 "${i}" > "${i}.sha512"; done
  for i in *.tar.gz; do gpg --print-mds "${i}" > "${i}.mds"; done
  cd "$SVNDISTDIR"
  svn add "${RATISVERSION}" || svn add "${RATISVERSION}/${RC#-}"
}

publish-git(){
  cd "$projectdir"
  git push apache "ratis-${RATISVERSION}${RC}"
}

publish-svn() {
   cd "${SVNDISTDIR}"
  svn commit -m "Publish proposed version of the next Ratis release ${RATISVERSION}${RC}"
}

publish-mvn(){
  cd "$projectdir"
  mvnFun -X clean deploy assembly:single -DskipTests=true  -Prelease -Papache-release -Dgpg.keyname="${CODESIGNINGKEY}"
}

if [ "$#" -ne 1 ]; then
  cat << EOF

Please choose from available phases (eg. make_rc.sh prepare-src):

   1. prepare-src: This is the first step. It modifies the mvn version, creates the git tag and
                   builds the project to create the source artifacts.
                   IT INCLUDES A GIT RESET + CLEAN. ALL THE LOCAL CHANGES WILL BE LOST!

   2. prepare-bin: The source artifact is copied to the $WORKINGDIR and the binary artifact is created from the source.
                   This is an additional check as the the released source artifact should be enough to build the whole project.

   3. assembly :   This step copies all the required artifacts to the svn directory and ($SVNDISTDIR) creates the signatures/checksum files.

   4. publish-git: The first remote step, only do it if everything is fine. It pushes the rc tag to the repository.

   5. publish-svn: Uploads the artifacts to the apache dev staging area to start the vote.

   6. publish-mvn: Uploads the artifacts to the maven staging repository

The next steps of the release process are not scripted:

   7. Close the staging maven repository at https://repository.apache.org/

   8. Send out the vote mail to the ratis-dev list

   9. Summarize the vote after the given period.

   10. (If the vote passed): Move the staged artifacts in svn from the dev area to the dist area.

   11. Publish maven repository at https://repository.apache.org/





EOF
else
  set -x
  eval "$1"
fi
