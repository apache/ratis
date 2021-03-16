---
title: Vagrant Testing
---
<!---
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

Please refer to the [documentation](https://github.com/apache/ratis/blob/master/dev-support/vagrant/README.md) for instructions to use the Vagrant automation.

Starting from the directory `dev-support/vagrant/`:

* To build all Vagrant boxes, invoke `./run_all_tests.sh build`
* To remove any generated data, invoke `./run_all_tests.sh clean`
* To run the tests, invoke `vagrant resume ratis-servers && vagrant ssh ratis-servers`
