#!/bin/bash

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
mvn=mvn
if [ "$MAVEN" != "" ]; then
  mvn="${MAVEN}"
fi
mvnopts="-Xmx1g"
if [ "$MAVEN_OPTS" != "" ]; then
  mvnopts="${MAVEN_OPTS}"
fi

mvnGet() {
  ${mvn} -q -Dexec.executable="echo" -Dexec.args="\${${1}}" --non-recursive \
    org.codehaus.mojo:exec-maven-plugin:1.6.0:exec 2>/dev/null
}

# Check project name
projectname=$(mvnGet project.name)
if [ "${projectname}" = "Apache Ratis" ]; then
  echo
  echo "Prepare release artifacts for $projectname"
  echo
else
  echo "Unexpected project name \"${projectname}\"."
  echo
  echo "Please run this script ($0) under the root directory of Apache Ratis."
  exit 1;
fi

# Set projectdir and archivedir
projectdir=$(pwd)
echo "Project dir: ${projectdir}"
archivedir="${projectdir}/../`basename ${projectdir}`.`date -u +"%Y%m%d-%H%M%S"`"
echo "Archive dir: ${archivedir}"
if [ -d "${archivedir}" ]; then
  echo "${archivedir} already exists"
  exit 1;
fi

# Set repodir
repodir=${projectdir}/../`basename ${projectdir}`.repository
echo "Repo dir: ${repodir}"

mvnFun() {
  set -x
  MAVEN_OPTS="${mvnopts}" ${mvn} -Dmaven.repo.local=${repodir} $@
  set +x
}

# clean shaded source
mvnFun clean -Pclean-shade
repodir=`cd ${repodir} > /dev/null; pwd`

# generate source tar.gz
mvnFun install -DskipTests assembly:single -Prelease -Dmaven.javadoc.skip=true

mkdir "${archivedir}"
archivedir=`cd ${archivedir} > /dev/null; pwd`

artifactid=$(mvnGet project.artifactId)
assemblydir="$(pwd)/${artifactid}-assembly"
mv ${assemblydir}/target/${artifactid}-*.tar.gz "${archivedir}"

echo
echo "Generated artifacts successfully."
ls -l ${archivedir}
echo
echo "Check the content of ${archivedir}."
echo "If good, sign and push to dist.apache.org"
echo "  cd ${archivedir}"
echo '  for i in *.tar.gz; do echo $i; gpg --print-mds $i > $i.mds ; done'
echo '  for i in *.tar.gz; do echo $i; gpg --print-md SHA512 $i > $i.sha512 ; done'
echo '  for i in *.tar.gz; do echo $i; gpg --armor --output $i.asc --detach-sig $i ; done'
echo "  rsync -av ${archivedir}/*.gz ${archivedir}/*.mds ${archivedir}/*.asc ~/repos/dist-dev/${artifactid}-VERSION/"
echo
echo "Check the content deployed to maven."
echo "If good, close the repo and record links of temporary staging repo"
echo "  MAVEN_OPTS=\"${mvnopts}\" ${mvn} deploy -DskipTests -Papache-release -Prelease -Dmaven.repo.local=${repodir}"
echo
echo "If all good tag the RC"
echo
echo "Finally, you may want to remove archive dir and repo dir"
echo "  rm -rf ${archivedir}"
echo "  rm -rf ${repodir}"
