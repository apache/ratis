<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

RATIS CI scripts
----------------

These scripts are called from jenkins for ratis pre-commit and nightly build jobs.

On jenkins, the repository is checked out to a subdirectory (sourcedir) and the script is called from the parent directory which is also used to store temporary files (yetus/out/...).

IT'S NOT RECOMMENDED to run it locally unless you know what could be expected. The script runs in sentinel/robot mode, so:

 * Old docker images are removed by the script
 * Local commits are removed by the script


## Running locally

To test the jenkins build locally:

 1. create a new directory
 2. Clone the ratis repository to a subdirectory: `git clone git://github.com/apache/incubator-ratis.git sourcedir`
 3. Run the script fro the parent directory ./sourcedir/dev-support/ci/nightly-build.sh

For nightly-build.sh you can set the BRANCH environment variable to define which branch should be tested.

For precommit-build.sh you should set ISSUE_NUM (eg. 122, RATIS prefix should not be added).

The variables are set by the jenkins.
