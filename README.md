# Ratis
Ratis is a java library that implements the RAFT protocol[1]. The Raft paper can be accessed at [this link] (https://raft.github.io/raft.pdf). The paper introduces Raft and states its motivations in following words:
  > Raft is a consensus algorithm for managing a replicated log. It produces a result equivalent to (multi-)Paxos, and it is as efficient as Paxos, but its structure is different from Paxos; this makes Raft more understandable than Paxos and also provides a better foundation for building practical systems.

  Ratis aims to make raft available as a java library that can be used by any system that needs to use a replicated log. It provides pluggability for state machine implementations to manage replicated states. It also provides pluggability for Raft log, and rpc implementations to make it easy for integration with other projects. Another important goal is to support high throughput data ingest so that it can be used for more general data replication use cases.


# Reference
[1] _Diego Ongaro and John Ousterhout. 2014. In search of an understandable consensus algorithm. In Proceedings of the 2014 USENIX conference on USENIX Annual Technical Conference (USENIX ATC'14), Garth Gibson and Nickolai Zeldovich (Eds.). USENIX Association, Berkeley, CA, USA, 305-320._

