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

# Apache Ratis Examples

#### Building and Running The Examples

Compile the repository using `mvn clean package -DskipTests` under the project root directory;
see also [BUILDING.md](../BUILDING.md).

All the scripts for running the examples are located in the [ratis-examples/src/main/bin](src/main/bin) directory,

## Example 1: FileStore

**FileStore** is file service supporting *read*, *write* and *delete* operations.
The **FileStoreStateMachine** is implemented using the asynchronous event-driven model.
The source code is located in
* [ratis-examples/src/main/java/org/apache/ratis/examples/filestore/](src/main/java/org/apache/ratis/examples/filestore).


#### Server
To spawn a *FileStore* server, run
* `server.sh filestore server --id <SELF_ID> --storage <STORAGE_DIR> --peers <ID:IP_ADDRESS,...>`

where `<SELF_ID>`, which must be in the peer list, is the id of the instance being spawned.

For example `ratis-examples/src/main/bin/server.sh filestore server --id n0 --storage /tmp/data --peers n0:xx.xx.xx.xx:6000,n1:yy.yy.yy.yy:6001,n2:zz.zz.zz.zz:6002`

#### Client

To spawn a *FileStore* load generation client, run
* `client.sh filestore loadgen --size <FILE_SIZE> --numFiles <NUM_FILES> --peers <ID:IP_ADDRESS,...>`

where `<FILE_SIZE>` is the size of the files to be generated in bytes,
`<NUM_FILES>` is the number of files to be generated.

For example `ratis-examples/src/main/bin/client.sh filestore loadgen --size 1048576 --numFiles 1000 --peers n0:xx.xx.xx.xx:6000,n1:yy.yy.yy.yy:6001,n2:zz.zz.zz.zz:6002`

## Example 2: Arithmetic

**Arithmetic** is an implementation of a replicated state machine.
A *variable map* is stored in the *ArithmeticStateMachine* which supports *assign* or *get* operations.
Clients may assign a variable to a value by specifying either the value or a formula to compute the value.

In [TestArithemetic](src/test/java/org/apache/ratis/examples/arithmetic/TestArithmetic.java),
it uses Arithmetic to solve *Pythagorean* equation and compute &pi; using Gauss–Legendre algorithm.

The source code is located in
* [ratis-examples/src/main/java/org/apache/ratis/examples/arithmetic/](src/main/java/org/apache/ratis/examples/arithmetic).

#### Server
To spawn an *Arithmetic* server, run
* `server.sh arithmetic server --id <SELF_ID> --storage <STORAGE_DIR> --peers <ID:IP_ADDRESS,...>`

where `<SELF_ID>`, which must be in the peer list, is the id of the instance being spawned.

For example `ratis-examples/src/main/bin/server.sh arithmetic server --id n0 --storage /tmp/data --peers n0:xx.xx.xx.xx:6000,n1:yy.yy.yy.yy:6001,n2:zz.zz.zz.zz:6002`

#### Client

To run an *Arithmetic* client command, run
* `client.sh arithmetic get --name <VAR> --peers <ID:IP_ADDRESS,...>`

or
* `client.sh arithmetic assign --name <VAR> --value <VALUE> --peers <ID:IP_ADDRESS,...>`

where `<VAR>` is the name of the variable and `<VALUE>` is the value to be assigned.

For example `ratis-examples/src/main/bin/client.sh arithmetic get --name b --peers n0:xx.xx.xx.xx:6000,n1:yy.yy.yy.yy:6001,n2:zz.zz.zz.zz:6002`

## Pre-Setup Vagrant Pseudo Cluster
One can see the interactions of a three server Ratis cluster with a load-generator running against it
by using the `run_all_tests.sh` script found in [dev-support/vagrant/](../dev-support/vagrant).
See the [dev-support/vagrant/README.md](../dev-support/vagrant/README.md) for more on dependencies and what is setup.
This will allow one to try a fully setup three server Ratis cluster on a single VM image,
preventing resource contention with your development host and allowing failure injection too.
