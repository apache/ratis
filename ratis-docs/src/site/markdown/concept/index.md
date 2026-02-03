<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Introduction to Apache Ratis

## Sections
1. [Overview](index.md#section-1)
2. [Core Concepts](core-concepts.md)
3. [Integration Guide](integration.md)
4. [Operations and Management](operations.md)
5. [Advanced Topics](advanced.md)

<a id="section-1" />

## Section 1: Overview of Raft and Apache Ratis

* [Introduction to Raft and Apache Ratis](#raft-and-apache-ratis)
* [Raft Cluster Topology](#raft-cluster-topology)

### Raft and Apache Ratis

The Raft consensus algorithm solves a fundamental problem in distributed systems: how do you get
multiple computers to agree on a sequence of operations, even when some might fail or become
unreachable? This problem, known as distributed consensus, is at the heart of building reliable
distributed systems.

Raft ensures that a cluster of servers maintains an identical, ordered log of operations. Each
server applies these operations to its local state machine in the same order, guaranteeing that
all servers end up with identical state. This approach, called state machine replication,
provides both consistency and fault tolerance.

You should consider using Raft when your system needs strong consistency guarantees across
multiple servers. This typically applies to systems where correctness is more important than
absolute performance, such as distributed databases, configuration management systems, or any
application where split-brain scenarios would be unacceptable.

Apache Ratis is a Java library that implements the Raft consensus protocol. The key word here
is "library" - Ratis is not a standalone service that you communicate with over the network.
Instead, you embed Ratis directly into your Java application, and it becomes part of your
application's runtime.

This embedded approach creates tight integration between your application and the consensus
mechanism. Your application and Ratis run in the same JVM, sharing memory and computational
resources. Your application provides the business logic (the "state machine" in Raft terminology),
while Ratis handles the distributed consensus mechanics needed to keep multiple instances of your
application synchronized.

### Raft Cluster Topology

Understanding the basic building blocks of a Raft deployment affects both the correctness and
performance of your system.

#### Servers, Clusters, and Groups

A Raft server (also known as a "peer" or "member") is a single running instance of your application
with Ratis embedded. Each server runs your state machine and participates in the consensus
protocol.

A Raft cluster is a physical collection of servers that can participate in consensus. A Raft
group is a logical consensus domain that runs across a specific subset of peers in the cluster.
One of the peers in a group acts as the "leader" while the others are "followers" or "listeners".
The leader handles all write requests and replicates operations to other peers in the group. Both
leaders and followers can service read requests, with different consistency guarantees. A single
cluster can host multiple independent Raft groups, each with its own leader election, consistency
and state replication.

#### Majority-Based Decision-Making

Raft's safety guarantees depend on majority agreement within each group. The leader replicates
each operation to the followers in its group, and operations are committed when at least
$\lfloor N/2 + 1 \rfloor$ peers in that group acknowledge them. This means a group of 3 peers can
tolerate 1 failure, a group of five peers can tolerate 2 failures, and so on. Since a group of
$N$ peers for an even $N$ can tolerate the same number of failures as a group of $(N-1)$ peers,
groups typically consist of an odd number of peers (3, 5, or 7 are common) to ensure clear
majority decisions.

This majority requirement affects both availability and performance. A group remains available as
long as a majority of its peers are reachable and functioning. However, every transaction must
wait for majority acknowledgment, so the slowest server in the majority determines your write
latency.

#### Server Placement and Network Considerations

The physical and network placement of your servers impacts both availability and performance.
Placing all servers in the same rack or data center provides the lowest latency but risks
creating a single point of failure. Distributing servers across multiple availability zones or
data centers improves fault tolerance but can increase latency.

A common approach is to place servers across multiple availability zones within a single region
for a balance of fault tolerance and performance. For applications requiring geographic
distribution, you might place servers in different regions, accepting higher latency in exchange
for better disaster recovery capabilities.

---
Next: [Core Concepts](core-concepts.md)