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
# Apache Ratis Concepts

## Table of Contents

1. [Overview of Raft and Apache Ratis](#overview-of-raft-and-apache-ratis)
2. [Raft Cluster Topology](#raft-cluster-topology)
3. [The Raft Log - Foundation of Consensus](#the-raft-log---foundation-of-consensus)
4. [The State Machine - Your Application's Heart](#the-state-machine---your-applications-heart)
5. [Consistency Models and Read Patterns](#consistency-models-and-read-patterns)
6. [Snapshots - Managing Growth and Recovery](#snapshots---managing-growth-and-recovery)
7. [Logical Organization of Ratis](#logical-organization-of-ratis)
8. [Leadership and Fault Tolerance](#leadership-and-fault-tolerance)
9. [Scaling with Multi-Raft Groups](#scaling-with-multi-raft-groups)

## Overview of Raft and Apache Ratis

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

## Raft Cluster Topology

Understanding the basic building blocks of a Raft deployment affects both the correctness and
performance of your system.

### Servers, Clusters, and Groups

A Raft server (also known as a "peer") is a single running instance of your application with
Ratis embedded. Each server runs your state machine and participates in the consensus protocol.

A Raft cluster is a physical collection of servers that can participate in consensus. A Raft
group is a logical consensus domain that runs across a specific subset of peers in the cluster.
At any given time, one peer in a group acts as the "leader" while the others are "followers" or
"listeners". The leader handles all write requests and replicates operations to other peers in
the group. Both leaders and followers can service read requests, with different consistency
guarantees.

A single cluster can host multiple independent Raft groups, each with its own leader election,
consistency and state replication. Groups typically consist of an odd number of peers (3, 5, or
7 are common) to ensure clear majority decisions.

### Majority-Based Decision-Making

Raft's safety guarantees depend on majority agreement within each group. The leader replicates
each operation to the followers in its group, and operations are committed when at least
(N/2 + 1) peers in that group acknowledge them. This means a group of 3 peers can tolerate 1
failure, a group of five peers can tolerate 2 failures, and so on.

This majority requirement affects both availability and performance. A group remains available as
long as a majority of its peers are reachable and functioning. However, every transaction must
wait for majority acknowledgment, so the slowest server in the majority determines your write
latency.

### Server Placement and Network Considerations

The physical and network placement of your servers impacts both availability and performance.
Placing all servers in the same rack or data center provides the lowest latency but risks
creating a single point of failure. Distributing servers across multiple availability zones or
data centers improves fault tolerance but can increase latency.

A common approach is to place servers across multiple availability zones within a single region
for a balance of fault tolerance and performance. For applications requiring geographic
distribution, you might place servers in different regions, accepting higher latency in exchange
for better disaster recovery capabilities.

## The Raft Log - Foundation of Consensus

The Raft log is the central data structure that makes distributed consensus possible. Each server
in a Raft group maintains its own copy of this append-only ledger that records every operation
in the exact order they should be applied to the state machine.

Each entry in the log contains three key pieces of information: the operation itself (what should
be done), a log index (a sequential number indicating the entry's position), and a term number
(the period during which a leader created this entry). Terms represent periods of leadership and
increase each time a new leader is elected, preventing old leaders from overwriting newer entries.
The combination of the term and log index is referred to as a term-index (`TermIndex`) and 
establishes the ordering of entries in the log.

The log serves as both the mechanism for replication (leaders send log entries to followers) and
the source of truth for recovery (servers can rebuild their state by replaying the log). When we
talk about "committing" an operation, we mean that a majority of servers have acknowledged
storing that log entry, making it safe to apply to the state machine.

## The State Machine - Your Application's Heart

In Ratis, the state machine is your application's primary integration point. Your business logic
or data storage operations are implemented by the state machine.

The state machine is not a finite state machine with states and transitions. Instead, it's a
deterministic computation engine that processes a sequence of operations and maintains some
internal state. The state machine must be deterministic: given the same sequence of operations,
it must always produce the same results and end up in the same final state. Operations are
processed sequentially, one at a time, in the order they appear in the Raft log.

### State Machine Responsibilities

Your state machine has three primary responsibilities. First, it processes Raft transactions by
validating incoming requests before they're replicated and applying committed operations to your
application state. Second, it maintains your application's actual data, which might be an
in-memory data structure, a local database, files on disk, or any combination of these. Third,
it creates point-in-time representations of its state (snapshots) and can restore its state from
snapshots during recovery.

### The State Machine Lifecycle

The state machine operates at two different lifecycle levels: an overall peer lifecycle and a
per-transaction processing lifecycle.

#### Peer Lifecycle

During initialization, when a peer starts up, the state machine loads any existing snapshots and
prepares its internal data structures. The Raft layer then replays any log entries that occurred
after the snapshot, bringing the peer up to the current state of the group.

During normal operation, the state machine continuously processes transactions as they're
committed by the Raft group, responds to leadership changes, and handles read-only queries. For
read-only operations, the state machine can answer queries directly without going through the
Raft log, providing better performance for reads but with consistency trade-offs.

Periodically, the state machine creates snapshots of its current state. This happens either
automatically based on configuration (like log size thresholds) or manually through
administrative commands.

#### Transaction Processing Lifecycle

For each individual transaction, the state machine follows a multistep processing sequence. In
the validation phase, the leader's state machine examines incoming requests through the
`startTransaction` method. This is where you validate that the operation is properly structured
and valid in the current context.

In the pre-append phase, just before the operation is written to the log, the state machine can
perform any final preparations through the `preAppendTransaction` method. After the operation is
committed by the Raft group, the state machine is notified via `applyTransactionSerial` and can
handle any order-sensitive logic that must happen before the main application logic is invoked.

Finally, in the application phase, the operation is applied to the actual application state
through the `applyTransaction` method. This is where your business logic executes and where the
operation's effects become visible to future queries.

### Designing Your State Machine

When designing your state machine, ensure your operations are deterministic and can be
efficiently serialized for replication. Operations must be idempotent, as Raft may occasionally
replay operations during recovery scenarios.

Plan how you'll represent your application's state for both runtime efficiency and snapshot
serialization. If your state machine maintains state in external systems (databases, files),
ensure your snapshot process captures this external state consistently.

Robust error handling is crucial. Server-side errors require distinguishing between recoverable
errors (like validation failures) and fatal errors (like storage failures). Errors in
`startTransaction` prevent operations from being committed and replicated. Errors in
`applyTransaction` are considered fatal since they indicate the state machine cannot process
already-committed operations.

## Consistency Models and Read Patterns

In a distributed system, consistency refers to the guarantees you have about seeing the effects
of write operations when you read data. For write operations, Raft and Ratis provide strong
consistency: once a write operation is acknowledged as committed, all subsequent reads will see
the effects of that write. Read operations are more complex because Ratis offers several
different approaches with different consistency and performance characteristics.

### Write Consistency

Write operations in Ratis follow a straightforward path that provides strong consistency. Clients
send write requests to the leader, which validates the operation through the state machine's
`startTransaction` method, then replicates it to a majority of followers. Once a majority
acknowledges, the operation is committed. The leader applies the operation to its state machine
and returns the result to the client, while followers eventually apply the same operation in the
same order.

### Read Consistency Options

**Linearizable reads** provide the strongest consistency by going through the Raft protocol to
ensure you're reading the most up-to-date committed data. Use the client's `sendReadOnly` method,
which forces the leader to confirm it's still the leader before serving the read.

**Leader reads** offer strong consistency but with caveats: these are reads served directly by
the leader without going through the Raft protocol. Use `sendReadOnlyNonLinearizable` to query
the leader's state machine directly. This is faster than linearizable reads but may return stale
data if the leader has been partitioned from the majority.

**Follower reads** provide eventual consistency by serving reads directly from followers using
their local state machine. Call `sendReadOnly(message, serverId)` with a specific follower's
server ID. These are the fastest reads but may return stale data if the follower is behind in
applying log entries.

**Stale reads with minimum index** offer a hybrid approach where you specify a minimum log index
that the peer must have applied before serving the read. Call `sendStaleRead`: if the peer
hasn't caught up to your minimum index, it throws a `StaleReadException`.

### The Query Method and Read-Only Operations

The state machine's `query` method enables you to handle read-only operations without going
through the Raft protocol. This provides significant performance benefits but requires careful
consideration of consistency requirements. Your state machine's `query` method will be called
for explicit read-only requests from clients, queries that need to read state without modifying
it, and health checks or monitoring queries.

### Choosing the Right Read Pattern

Use **linearizable reads** when correctness is more important than performance, you need to read
your own writes immediately, or the application cannot tolerate any stale data. Use **leader
reads** when you need strong consistency but can tolerate very brief staleness during network
partitions, or when building interactive applications where users expect to see their recent
changes.

Use **follower reads** when you can tolerate stale data in exchange for better performance and
availability, you're implementing read replicas for scaling read-heavy workloads, or the data
being read doesn't change frequently. Use **stale reads** when you need fine-grained control
over the consistency/performance trade-off.

## Snapshots - Managing Growth and Recovery

Snapshots are a point-in-time representation of your state machine's complete state, along with
metadata about which log entries are included in that state. They prevent the log from growing
without bound and enable efficient recovery and catch-up for peers that have fallen behind.

The snapshot includes the actual application state, the term-index of the last log entry that
contributed to this state, and the Raft group configuration at the time the snapshot was taken.

Without snapshots, the Raft log would grow indefinitely, eventually consuming all available
storage. Crashed peers would need to replay potentially millions of log entries to catch up,
dramatically slowing recovery. New peers joining an established group would need to process the
entire history of the group, which could take hours or days for active systems.

### Creating Snapshots

Snapshots can be created automatically when the log grows beyond a certain size, manually
triggered through the admin API, or sent by the leader to peers that are far behind instead of
replaying thousands of log entries.

When your state machine's `takeSnapshot` method is called, it needs to create a consistent view
of your application state. This might involve pausing writes, creating a database transaction,
or using copy-on-write data structures. The method must serialize state by writing it to durable
storage in a format that can be read back later, record which term-index the snapshot represents,
and return the log index so Ratis can safely discard older log entries.

Different applications will have different strategies for snapshot creation. A stop-the-world
approach pauses all operations while creating the snapshot: simple but impacts availability.
Copy-on-write uses data structures that support efficient point-in-time copies. Database
transactions can create consistent snapshots if your state is in a database. Some storage
engines support checkpointing to leverage native snapshot capabilities.

### Snapshot Installation and Recovery

When a peer needs to catch up using a snapshot, it receives the snapshot data from the leader or
loads it from local storage. The state machine is paused to prevent conflicts during restoration,
the snapshot data is loaded replacing any existing state, and the state machine resumes normal
operation by replaying any log entries that occurred after the snapshot.

Your state machine's `initialize` method is responsible for loading snapshots during startup by
loading the latest snapshot if available, with the Raft layer replaying any log entries after
the snapshot.

### Designing Snapshot-Friendly State Machines

When designing your state machine, ensure your state can be efficiently serialized and
deserialized, avoiding complex object graphs that are difficult to serialize. For very large
state machines, consider whether you can implement incremental snapshots that only capture
changes since the last snapshot.

If your state machine maintains state in external systems, ensure your snapshot process captures
this external state consistently. Regularly test your snapshot and recovery process to ensure it
works correctly under various failure scenarios.

## Logical Organization of Ratis

Rather than focusing on package structure, let's examine the logical components and their
relationships, understanding how they work together to provide the Raft consensus functionality.

### Primary Integration Points

When integrating with Ratis, you'll work with a small set of key classes and interfaces.

`StateMachine` Interface - This is where you'll spend most of your development time. Your
application implements this interface to define what operations mean and how they affect your
data. Key methods include `startTransaction()` to validate requests, `applyTransaction()` to
process committed operations, `query()` to handle reads, and `takeSnapshot()` to checkpoint your
application state.

`RaftClient` - Your application uses this to send requests to the Raft cluster. It handles
leader discovery, retries, and connection management automatically. You'll primarily use `send()`
for writes, `sendReadOnly()` for consistent reads, and `sendStaleRead()` for performance-
optimized reads.

`RaftServer` - This hosts your `StateMachine` and handles the Raft protocol. You'll configure
and start it, but most interaction happens through your `StateMachine` implementation. One
server can participate in multiple Raft groups simultaneously.

`RaftGroup` and `RaftPeer` - These define your cluster topology. `RaftGroup` represents a
consensus domain (which peers participate in a group), while `RaftPeer` represents individual
servers (their IDs and network addresses).

Configuration Classes - `RaftProperties` and related classes control behavior like timeouts,
storage locations, and transport settings.

Message and Request Types - Your operations flow through the system as `Message` objects. These
are serializable containers that carry your application's operations from clients to the
`StateMachine`. The `Message` interface is simple but designing your message types thoughtfully
affects both performance and maintainability.

### Client API Patterns

`RaftClient` provides several API styles to match different application patterns. The
`BlockingApi`, accessed through `RaftClient.io()`, offers traditional synchronous operations:
simple to use and understand, ideal when simplicity matters more than maximum throughput. The
`AsyncApi`, accessed through `RaftClient.async()` provides non-blocking operations that return
`CompletableFuture` objects, allowing your application to send multiple requests concurrently.

For applications that need to transfer large amounts of data, the `DataStreamApi` provides
efficient streaming that bypasses the normal Raft log for the data payload itself. Instead of
sending large payloads through the consensus mechanism, you stream data directly to peers while
still maintaining ordering and consistency guarantees through the Raft protocol.

The `AdminApi` handles cluster management operations like adding or removing peers, triggering
snapshots, and querying cluster status.

### Server Configuration and Lifecycle

`RaftServer` is the main server-side entry point, but it requires several configuration decisions
before startup. You'll need to choose a transport implementation (gRPC works well for most
deployments, while Netty provides more control), storage configuration including directories for
logs and snapshots, and key configuration like timeout and retry policies, snapshot policies,
and security settings.

A single `RaftServer` instance can participate in multiple Raft groups simultaneously through
Ratis's `Division` concept. Each group gets its own state machine instance and storage within
the server.

### Request Flow Through the System

When your application calls `RaftClient.send(message)`, the `RaftClient` first determines which
server to contact, handling leader discovery automatically. If the contacted server isn't the
current leader, it returns a `NotLeaderException` with information about the actual leader.

Once the message reaches the leader, your `StateMachine`'s `startTransaction(message)` method
validates the request. If validation succeeds, the leader replicates the operation through the
Raft protocol to a majority of followers. After the operation is committed, the leader calls
your `StateMachine`'s `applyTransaction(message)` method to execute the business logic.

The result flows back to the client, while followers eventually receive and apply the same
operation through their own `applyTransaction` calls. Read-only operations can bypass this flow
by going directly to the `query` method, trading consistency guarantees for better performance.

## Leadership and Fault Tolerance

Leadership in Ratis is both simpler and more complex than it might initially appear. Ratis
handles all the mechanics of leader election and failover automatically, but your application
needs to handle leadership changes robustly.

### Leadership and Automatic Election

In Raft, the leader is the only server that can accept write requests and decide the order of
operations in the log. This centralized decision-making enables Raft to provide strong
consistency guarantees. Leadership is temporary and can change at any time due to failures,
network partitions, or normal operational events.

When a Raft group starts up, or when the current leader fails, the remaining servers
automatically elect a new leader through a voting process. This process uses randomized timeouts
to prevent split votes and ensures that only servers with up-to-date logs can become leaders.
This happens entirely within Ratis without any intervention from your application code.

### Leadership and Client Behavior

From a client perspective, leadership changes are largely transparent. Clients can send requests
to any server in the group, and if that server is not the leader, it returns a
`NotLeaderException` with information about the current leader. If the leader fails while
processing a request, the client's retry logic will eventually find the new leader and retry.

Leadership changes can cause temporary performance degradation as the new leader establishes
itself and catches up any lagging followers. Applications should be designed to handle these
temporary slowdowns gracefully.

### State Machine Leadership Events

Your `StateMachine` can observe and react to leadership changes through several event
notification methods. The `notifyLeaderChanged` method is called whenever leadership changes.
The `notifyLeaderReady` method is called when this server becomes leader and is ready to serve
requests - the appropriate place to start any leader-specific background tasks. The
`notifyNotLeader` method is called when this server is no longer the leader - where you should
clean up any leader-specific resources.

### Handling Network Partitions

When a network partition occurs, the Raft group may split into multiple subgroups that cannot
communicate with each other. Raft's majority-based approach ensures that only one subgroup (the
one containing a majority of servers) can continue processing writes. The minority subgroup will
be unable to elect a leader and will reject write requests.

This behavior prevents split-brain scenarios where different parts of the system make conflicting
decisions. However, it also means that your application may become unavailable for writes if a
majority of servers are unreachable.

Consider the implications of different partition scenarios when designing your Raft deployment.
If you're deploying across multiple data centers, consider how network partitions between data
centers might affect availability. You may need to choose between consistency and availability
based on your application's requirements.

## Scaling with Multi-Raft Groups

As your application grows, you may find that a single Raft group becomes a bottleneck. This is
where Ratis's multi-group capability becomes valuable.

### Understanding Multi-Raft

Multi-Raft is an implementation pattern that Ratis supports for scaling beyond the limits of a
single Raft group. In a multi-Raft setup, you run multiple independent Raft groups, each
handling a subset of your application's operations. Each group operates independently with its
own leader election, consensus, log, and state machine.

### What is a Raft Group in Ratis?

In Ratis terminology, a "Raft Group" is a collection of servers that participate in a single
Raft cluster. Each group has a unique RaftGroupId (typically a UUID) that distinguishes it from
other groups. Each group consists of a set of RaftPeer objects representing the servers that
participate in that group's consensus.

### When to Use Multiple Groups

Consider using multiple Raft groups when a single group cannot handle the required throughput,
when you can logically partition your data or operations (such as having one group per geographic
region, per customer tenant, or per data type), when you need better fault isolation (if one
group fails, other groups can continue operating), or when you need different operational
characteristics for different parts of your system.

### Implementation Considerations

A single RaftServer instance can participate in multiple groups simultaneously. Each group gets
its own "Division" within the server, with its own state machine and storage. Since groups don't
coordinate at the Raft level, your application must handle any cross-group consistency
requirements through distributed transactions, saga patterns, or eventual consistency approaches.

To use multi-Raft effectively, you need to partition your application state. Horizontal
partitioning involves partitioning data across groups based on some key (e.g., user ID hash,
geographic region). Functional partitioning assigns different groups to handle different types
of operations or services. Hierarchical partitioning uses a tree-like structure where
higher-level groups coordinate lower-level groups.

Clients need to know which group to send requests to through client-side routing logic, a proxy
layer that routes requests, or consistent hashing schemes.

### Trade-offs and Limitations

Multi-group setups are significantly more complex than single-group setups. Maintaining
consistency across groups requires application-level coordination, which can be complex and
error-prone. More groups mean more leaders to monitor, more logs to manage, and more complex
failure scenarios. Each group consumes resources, so there's a practical limit to the number of
groups per server.

### Best Practices

Begin with a single group and only move to multiple groups when you have a clear scalability
need. Design your data model and operations to be partition-friendly from the start if you
anticipate needing multiple groups. Implement comprehensive monitoring for all groups, including
leader stability, replication lag, and resource usage.

Multi-Raft groups are a powerful scaling tool, but they should be used judiciously. The added
complexity is only worthwhile when you have clear scalability requirements that cannot be met
with a single Raft cluster.
