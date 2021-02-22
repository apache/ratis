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
# What is this?

This is a series of scripts for [Vagrant](https://vagrantup.com) to stand-up and test [Apache Ratis](https://ratis.apache.org/) servers and clients. One needs to have Vagrant and [VirtualBox](https://virtualbox.org) installed to stand-up these tests environments.

# What is provided?

This provides a multi-host `Vagrantfile` which provides the following described VM's:

## `ratis-build` VM
This provides a built version of Ratis with the [Namazu](https://github.com/osrg/namazu) test framework as well

## `ratis-servers` VM
This leverages the built Ratis servers to run a three server cluster and load generator. The Ratis servers are listening on ports 6000, 6001, 6002. The four processes are started and daemonized using [GNU Screen](https://www.gnu.org/software/screen/). The daemons further log to files in the home directory of the test user.

## `ratis-hdd-slowdown` VM
This VM starts the three Ratis servers and load genearator as the `ratis-servers` VM. However, the three servers contend with their storage directories made pathological (slow and error-prone) by Namazu. The configuration of pathology in namazu can be tuned in [hdd_config.toml](./namazu_configs/hdd_config.toml).

The test VM's can be stoped and all daemons restarted via: `vagrant up --provision <VM name>`
One can login to the VM and read the message-of-the-day for instructions on how to read the daemon logs; or connect to the Screen session.

# How to get started:
There is a shell script `run_all_tests.sh` which provides a single entrypoint for building or cleaning up all tests.
To visualize the flow of building all tests, a BPMN diagram of the intended process flow is: ![Vagrantfile BPMN Flow][Vagrantfile_BPMN]

Execute `run_all_tests.sh` with option `build`:
* Builds the `ratis-build` VM
* Packages a [Vagrant box](https://www.vagrantup.com/docs/boxes.html) to build test VMs off of
* Builds all test VMs and suspends them on success

Execute `run_all_tests.sh` with option `clean`:
* Destroys all test VMs
* Destroys the `ratis-build` VM
* Removes the `ratis-test` from Vagrant
* Removes the `ratistest.box` from the local file-system

Run the tests for a machine with `vagrant resume`:
* e.g. `vagrant resume ratis-servers && vagrant ssh ratis-servers`

[Vagrantfile_BPMN]: ./docs/vagrantfile_bpmn.svg "Vagrantfile Steps in BPMN (created with https://demo.bpmn.io)"
