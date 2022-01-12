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

The repository can be complied using `mvn clean package -DskipTests` under the project root directory;
see also [BUILDING.md](../BUILDING.md).

For the Example 1 and 2, All the scripts for running the examples are located in the [ratis-examples/src/main/bin](src/main/bin) directory;
see below for the usage.

Example 3 does not contain any script to run it refer to [Example 3 run section](#run-counter-server-and-client).

## Example 1: FileStore

**FileStore** is a high performance file service supporting *read*, *write* and *delete* operations.
The **FileStoreStateMachine** is implemented using the asynchronous event-driven model.
The source code is located in
* [ratis-examples/src/main/java/org/apache/ratis/examples/filestore/](src/main/java/org/apache/ratis/examples/filestore).


#### FileStore Server
To spawn a FileStore server, run
* `server.sh filestore server --id <SELF_ID> --storage <STORAGE_DIR> --peers <ID:IP_ADDRESS,...>`

where
* `<SELF_ID>`, which must be in the peer list, is the ID of the instance being spawned,
* `<STORAGE_DIR>` is a local directory for storing Raft log and other data, and
* `<ID:IP_ADDRESS,...>`, which is a comma separated list of ID and IP address pairs, specifies the list of server peers.

Note that when multiple servers running at the same host, they must use different `<STORAGE_DIR>`.

For example,

    BIN=ratis-examples/src/main/bin
    PEERS=n0:127.0.0.1:6000,n1:127.0.0.1:6001,n2:127.0.0.1:6002

    ID=n0; ${BIN}/server.sh filestore server --id ${ID} --storage /tmp/ratis/${ID} --peers ${PEERS}
    ID=n1; ${BIN}/server.sh filestore server --id ${ID} --storage /tmp/ratis/${ID} --peers ${PEERS}
    ID=n2; ${BIN}/server.sh filestore server --id ${ID} --storage /tmp/ratis/${ID} --peers ${PEERS}

#### FileStore Client

To spawn a FileStore load generation client, run
* `client.sh filestore loadgen --size <FILE_SIZE> --numFiles <NUM_FILES> --numClients <NUM_CLIENTS> --peers <ID:IP_ADDRESS,...>`

where
* `<FILE_SIZE>` is the size of the files to be generated in bytes, and
* `<NUM_FILES>` is the number of files to be generated, and
* `<NUM_CLIENTS>` is the number of clients.

Continue the server command example,

    ${BIN}/client.sh filestore loadgen --size 1048576 --numFiles 1000 --storage /tmp/ratis/loadgen --numClients 1 --peers ${PEERS}

## Example 2: Arithmetic

**Arithmetic** is an implementation of a replicated state machine.
A *variable map* is stored in the **ArithmeticStateMachine** which supports *assign* and *get* operations.
Clients may assign a variable to a value by specifying either the value or a formula to compute the value.

In [TestArithemetic](src/test/java/org/apache/ratis/examples/arithmetic/TestArithmetic.java),
it uses Arithmetic to solve Pythagorean equation and compute &pi; using Gauss–Legendre algorithm.

The source code is located in
* [ratis-examples/src/main/java/org/apache/ratis/examples/arithmetic/](src/main/java/org/apache/ratis/examples/arithmetic).

#### Arithmetic Server
To spawn an Arithmetic server, run
* `server.sh arithmetic server --id <SELF_ID> --storage <STORAGE_DIR> --peers <ID:IP_ADDRESS,...>`

where
* `<SELF_ID>`, which must be in the peer list, is the ID of the instance being spawned,
* `<STORAGE_DIR>` is a local directory for storing Raft log and other data, and
* `<ID:IP_ADDRESS,...>`, which is a comma separated list of ID and IP address pairs, specifies the list of server peers.

Note that when multiple servers running at the same host, they must use different `<STORAGE_DIR>`.

For example,

    BIN=ratis-examples/src/main/bin
    PEERS=n0:127.0.0.1:6000,n1:127.0.0.1:6001,n2:127.0.0.1:6002

    ID=n0; ${BIN}/server.sh arithmetic server --id ${ID} --storage /tmp/ratis/${ID} --peers ${PEERS}
    ID=n1; ${BIN}/server.sh arithmetic server --id ${ID} --storage /tmp/ratis/${ID} --peers ${PEERS}
    ID=n2; ${BIN}/server.sh arithmetic server --id ${ID} --storage /tmp/ratis/${ID} --peers ${PEERS}

#### Arithmetic Client

To run an Arithmetic client command, run
* `client.sh arithmetic get --name <VAR> --peers <ID:IP_ADDRESS,...>`

or
* `client.sh arithmetic assign --name <VAR> --value <VALUE> --peers <ID:IP_ADDRESS,...>`

where
* `<VAR>` is the name of a variable, and
* `<VALUE>` is the value (or a formula to compute the value) to be assigned.

Continue the server command example,

    ${BIN}/client.sh arithmetic assign --name a --value 3 --peers ${PEERS}
    ${BIN}/client.sh arithmetic assign --name b --value 4 --peers ${PEERS}
    ${BIN}/client.sh arithmetic assign --name c --value a+b --peers ${PEERS}
    ${BIN}/client.sh arithmetic get --name c --peers ${PEERS}

## Example 3: Counter
This example designed to be the simplest possible example and because of that 
this example does not follow the scripts and command line parameters of previous
examples.
The Goal of this example is to maintain and replicate a counter value across 
a cluster.
`CounterServer` class contains the main method to run the server and you can run it 
three times with three different parameters(1,2 and 3).
all address and ports of the peers hardcoded in `CounterCommon`, so you don't 
need any extra configuration to run this example on your localhost.
`CounterClient` class contains the main method to run the client,the client sends 
several INCREMENT command and after that, it sends a GET command and prints the 
result which should be the value of the counter.
'Counter State Machine' implemented in `CounterStateMachine` class.
You can find more detail by reading these classes javaDocs.

### Run Counter Server and Client
run the client and servers by these commands from ratis-examples directory:
for server: `java -cp target/*.jar org.apache.ratis.examples.counter.server.CounterServer {serverIndex}`
replace {serverIndex} with 1, 2, or 3
for client: `java -cp target/*.jar org.apache.ratis.examples.counter.client.CounterClient`

## Pre-Setup Vagrant Pseudo Cluster
Note: This option is only available to Example 1 and 2
One can see the interactions of a three server Ratis cluster with a load-generator running against it
by using the `run_all_tests.sh` script found in [dev-support/vagrant/](../dev-support/vagrant).
See the [dev-support/vagrant/README.md](../dev-support/vagrant/README.md) for more on dependencies and what is setup.
This will allow one to try a fully setup three server Ratis cluster on a single VM image,
preventing resource contention with your development host and allowing failure injection too.
