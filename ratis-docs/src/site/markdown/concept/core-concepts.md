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

Previous: [Overview of Raft and Ratis](index.md) | Top:[Overview of Raft and Ratis](index.md)

## Section 2: Core Concepts

* [The Raft Log](#the-raft-log---foundation-of-consensus)
* [The State Machine](#the-state-machine---your-applications-heart)
* [Consistency Models and Read Patterns](#consistency-models-and-read-patterns)

### The Raft Log - Foundation of Consensus

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

### The State Machine - Your Application's Heart

In Ratis, the state machine is your application's primary integration point. Your business logic
or data storage operations are implemented by the state machine.

The state machine is a deterministic computation engine that processes a sequence of operations
and maintains some internal state. The state machine must be deterministic: given the same
sequence of operations, it must always produce the same results and end up in the same final state.
Operations are processed sequentially, one at a time, in the order they appear in the Raft log.

#### State Machine Responsibilities

Your state machine has three primary responsibilities. First, it processes Raft transactions by
validating incoming requests before they're replicated and applying committed operations to your
application state. Second, it maintains your application's actual data, which might be an
in-memory data structure, a local database, files on disk, or any combination of these. Third,
it creates point-in-time representations of its state (snapshots) and can restore its state from
snapshots during recovery.

#### The State Machine Lifecycle

The state machine operates at two different lifecycle levels: an overall peer lifecycle and a
per-transaction processing lifecycle.

##### Peer Lifecycle

During initialization, when a peer starts up, the state machine loads any existing snapshots and
prepares its internal data structures. The Raft layer then replays any log entries that occurred
after the snapshot, bringing the peer up to the current state of the group.

During normal operation, the state machine continuously processes transactions as they're
committed by the Raft group, handles read-only queries, and may respond to changes in the node's
status as a leader or follower. For read-only operations, the state machine can answer queries
directly without going through the Raft log, providing better performance for reads but with
[consistency trade-offs](#consistency-models-and-read-patterns).

Periodically, the state machine creates snapshots of its current state. This happens either
automatically based on configuration (like log size thresholds) or manually through
administrative commands.

##### Transaction Processing Lifecycle

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

#### Designing Your State Machine

When designing your state machine, ensure your operations are deterministic and can be efficiently
serialized for replication. Operations are not required to be idempotent because the Raft protocol
ensures that each operation is applied exactly once on each peer, however idempotent operations may
make it easier to reason about your application.

Plan how you'll represent your application's state for both runtime efficiency and snapshot
serialization. If your state machine maintains state in external systems (databases, files),
ensure your snapshot process captures this external state consistently.

Robust error handling is crucial. Server-side errors require distinguishing between recoverable
errors (like validation failures) and fatal errors (like storage failures). Errors in
`startTransaction` prevent operations from being committed and replicated. Errors in
`applyTransaction` are considered fatal since they indicate the state machine cannot process
already-committed operations.

### Consistency Models and Read Patterns

In a distributed system, consistency refers to the guarantees you have about seeing the effects
of write operations when you read data. For write operations, Raft and Ratis provide strong
consistency: once a write operation is acknowledged as committed, all subsequent reads will see
the effects of that write. Read operations are more complex because Ratis offers several
different approaches with different consistency and performance characteristics.

#### Write Consistency

Write operations in Ratis follow a straightforward path that provides strong consistency. Clients
send write requests to the leader, which validates the operation through the state machine's
`startTransaction` method, then replicates it to a majority of followers. Once a majority
acknowledges, the operation is committed. The leader applies the operation to its state machine
and returns the result to the client, while followers eventually apply the same operation in the
same order.

#### Read Consistency Options

Ratis provides several read patterns with different consistency and performance characteristics.

Read requests query the state machine of a server directly without going through the Raft consensus
protocol. The `sendReadOnly()` API sends a read request to the leader. If a non-leader server
receives such request, it throws a `NotLeaderException` and then the client will retry other
servers. In contrast, the `sendReadOnly(message, serverId)` API sends the request to a particular
server, which may be a leader or a follower.

The server's `raft.server.read.option` configuration affects read consistency behavior:

* **DEFAULT (default setting)**: `sendReadOnly()` performs leader reads for efficiency. It provides
strong consistency under normal conditions. However, In case that an old leader has been
partitioned from the majority and a new leader has been elected, reading from the old leader can
return stale data since the old leader does not have the new transactions committed by the new
leader (referred to as the "split-brain problem").
* **LINEARIZABLE**: both `sendReadOnly()` and `sendReadOnly(message, serverId)` use the ReadIndex
protocol to provide linearizable consistency, ensuring you always read the most up-to-date committed
data and won't read stale data as described in the "Split-brain Problem" above.
    * Non-linearizable API: Clients may use `sendReadOnlyNonLinearizable()` to read from leader's
      state machine directly without a linearizable guarantee.

Server-side configuration allows operators to choose between performance (leader reads) and strong
consistency guarantees (linearizable reads) for their entire cluster.

Stale reads with minimum index let you specify a minimum log index that the peer must have
applied before serving the read. Call `sendStaleRead()`: if the peer hasn't caught up to your
minimum index, it will throw a `StaleReadException`.

In summary:
* **Leader reads** query the current leader's state machine directly without going through the Raft
consensus protocol. Call `sendReadOnly()` for the strongest consistency supported by the server.
* Use`sendReadOnlyNonLinearizable()` for leader reads without a linearizable guarantee.
* Use `sendReadOnly(message, serverId)` with a specific follower's server ID for **follower reads**,
which offer better performance but may return stale data.
* Use `sendStaleRead()` to specify the minimum log index that the server must have applied.
* Use `sendReadAfterWrite()` to ensure the read reflects the latest successful write by the
same client, for **read-after-write consistency**.

Note that all of these operations may be performing as blocking or async operations. See
[Client API Patterns](integration.md#client-api-patterns) for more information.

#### The Query Method and Read-Only Operations

The state machine's `query` method enables you to handle read-only operations without going
through the Raft protocol. This provides significant performance benefits but requires careful
consideration of consistency requirements. Your state machine's `query` method will be called
for explicit read-only requests from clients, queries that need to read state without modifying
it, and health checks or monitoring queries.

#### Choosing the Right Read Pattern

Use **linearizable reads** when correctness is more important than performance, you need to read
your own writes immediately, or the application cannot tolerate any stale data. Use **leader
reads** when you need strong consistency but can tolerate very brief staleness during network
partitions, or when building interactive applications where users expect to see their recent
changes.

Use **follower reads** when you can tolerate stale data in exchange for better performance and
availability, you're implementing read replicas for scaling read-heavy workloads, or the data
being read doesn't change frequently. Use **stale reads** when you need fine-grained control
over the consistency/performance trade-off.

---
Next: [Integration](integration.md)