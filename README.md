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

# Apache Ratis
Apache Ratis is a java library that implements the RAFT protocol [1].
The Raft paper can be accessed at (https://raft.github.io/raft.pdf).
The paper introduces Raft and states its motivations in following words:
  > Raft is a consensus algorithm for managing a replicated log. It produces a result equivalent to (multi-)Paxos, and it is as efficient as Paxos, but its structure is different from Paxos; this makes Raft more understandable than Paxos and also provides a better foundation for building practical systems.

  Ratis aims to make raft available as a java library that can be used by any system that needs to use a replicated log. It provides pluggability for state machine implementations to manage replicated states. It also provides pluggability for Raft log, and rpc implementations to make it easy for integration with other projects. Another important goal is to support high throughput data ingest so that it can be used for more general data replication use cases.

# Usage

Compile the repository using `mvn clean package -DskipTests`

## FileStore

### Server
To spawn the FileStoreStateMachineServer, use `bin/server.sh filestore server --id <selfid> --storage <storage_dir> --peers <id:ip_address,...>`

selfid is the id of the instance being spwaned, this should be one of the ids in the peer list.

For example `ratis-examples/src/main/bin/server.sh filestore server --id n0 --storage /tmp/data --peers n0:172.26.32.224:6000,n1:172.26.32.225:6001,n2:172.26.32.226:6002`

### Client

To spawn the FileStoreStateMachine client, use `bin/client.sh filestore loadgen --value <file_size> --files <num_files> --peers <id:ip_address,...>`

Where, file_size is the size of the file to be generated in bytes, num_files is the number of files to be generated.

For example `ratis-examples/src/main/bin/client.sh filestore loadgen --value 1048576 --files 1000 --peers n0:172.26.32.224:6000,n1:172.26.32.225:6001,n2:172.26.32.226:6002`

## Arithmetic

### Server
To spawn the ArithmeticStateMachineServer, use `bin/server.sh arithmetic server --id <selfid> --storage <storage_dir> --peers <id:ip_address,...>`

selfid is the id of the instance being spwaned, this should be one of the ids in the peer list.

For example `ratis-examples/src/main/bin/server.sh arithmetic server --id n0 --storage /tmp/data --peers n0:172.26.32.224:6000,n1:172.26.32.225:6001,n2:172.26.32.226:6002`

### Client

To spawn the ArithmeticStateMachine client, use `bin/client.sh arithmetic get --name b --peers <id:ip_address,...>`

Where, b is the name of the variable.

For example `ratis-examples/src/main/bin/client.sh arithmetic get --name b --peers n0:172.26.32.224:6000,n1:172.26.32.225:6001,n2:172.26.32.226:6002`

PS: the peer is a id, ipaddress pair seperated by ':', for eg. n0:172.26.32.224:6000


# Reference
[1] _Diego Ongaro and John Ousterhout. 2014. In search of an understandable consensus algorithm. In Proceedings of the 2014 USENIX conference on USENIX Annual Technical Conference (USENIX ATC'14), Garth Gibson and Nickolai Zeldovich (Eds.). USENIX Association, Berkeley, CA, USA, 305-320._

