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

* To build the artifacts, see [BUILDING.md](BUILDING.md).
* To run the examples, see [ratis-examples/README.md](ratis-examples/README.md).

# Reference
[1] _Diego Ongaro and John Ousterhout. 2014. In search of an understandable consensus algorithm. In Proceedings of the 2014 USENIX conference on USENIX Annual Technical Conference (USENIX ATC'14), Garth Gibson and Nickolai Zeldovich (Eds.). USENIX Association, Berkeley, CA, USA, 305-320._

