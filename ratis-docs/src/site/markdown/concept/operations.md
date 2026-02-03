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

Previous: [Integration](integration.md) | Top:[Overview of Raft and Ratis](index.md)

## Section 4: Operations and Management

* [Snapshots](#snapshots---managing-growth-and-recovery)
* [Leadership and Fault Tolerance](#leadership-and-fault-tolerance)

### Snapshots - Managing Growth and Recovery

Snapshots are a point-in-time representation of your state machine's complete state, along with
metadata about which log entries are included in that state. They prevent the log from growing
without bound and enable efficient recovery and catch-up for peers that have fallen behind.

The snapshot includes the actual application state, the term-index of the last log entry that
contributed to this state, and the Raft group configuration at the time the snapshot was taken.

Without snapshots, the Raft log would grow indefinitely, eventually consuming all available
storage. Crashed peers would need to replay potentially millions of log entries to catch up,
dramatically slowing recovery. New peers joining an established group would need to process the
entire history of the group, which could take hours or days for active systems.

#### Creating Snapshots

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

#### Snapshot Installation and Recovery

When a peer needs to catch up using a snapshot, it receives the snapshot data from the leader or
loads it from local storage. The state machine is paused to prevent conflicts during restoration,
the snapshot data is loaded replacing any existing state, and the state machine resumes normal
operation by replaying any log entries that occurred after the snapshot.

Your state machine's `reinitialize` method is responsible for loading snapshots during startup by
loading the latest snapshot if available, with the Raft layer replaying any log entries after
the snapshot.

#### Designing Snapshot-Friendly State Machines

When designing your state machine, ensure your state can be efficiently serialized and
deserialized, avoiding complex object graphs that are difficult to serialize. For very large
state machines, consider whether you can implement incremental snapshots that only capture
changes since the last snapshot.

If your state machine maintains state in external systems, ensure your snapshot process captures
this external state consistently. Regularly test your snapshot and recovery process to ensure it
works correctly under various failure scenarios.

### Leadership and Fault Tolerance

Ratis handles the mechanics of leader election and failover automatically. If your application does
not care about whether a specific server is a leader or follower, then it does not need to do
anything when leadership changes. Otherwise, your application can optionally observe leadership
changes and react accordingly: see [State Machine Leadership Events](#State-Machine-Leadership-Events).

#### Leadership and Automatic Election

In Raft, the leader is the only server that can accept write requests and decide the order of
operations in the log. This centralized decision-making enables Raft to provide strong
consistency guarantees. Leadership is temporary and can change at any time due to failures,
network partitions, or normal operational events.

When a Raft group starts up, or when the current leader fails, the remaining servers
automatically elect a new leader through a voting process. This process uses randomized timeouts
to prevent split votes and ensures that only servers with up-to-date logs can become leaders.
This happens entirely within Ratis without any intervention from your application code.

#### Leadership and Client Behavior

From a client perspective, leadership changes are largely transparent. Clients can send requests
to any server in the group, and if that server is not the leader, it returns a
`NotLeaderException` with information about the current leader. If the leader fails while
processing a request, the client's retry logic will eventually find the new leader and retry.

Leadership changes can cause temporary performance degradation as the new leader establishes
itself and catches up any lagging followers. Applications should be designed to handle these
temporary slowdowns gracefully.

#### State Machine Leadership Events

Your `StateMachine` can observe and react to leadership changes through several event
notification methods exposed through the `StateMachine.EventApi` interface. The
`notifyLeaderChanged` method is called whenever leadership changes. The `notifyLeaderReady`
method is called when this server becomes leader and is ready to serve requests: the
appropriate place to start any leader-specific background tasks. The `notifyNotLeader` method
is called when this server is nolonger the leader: where you should clean up any leader-specific
resources.

#### Handling Network Partitions

When a network partition occurs, the Raft group may split into multiple subgroups that cannot
communicate with each other. Raft's majority-based approach ensures that at most one subgroup (that
contains a majority of servers) can continue processing writes. Any minority subgroup will be
unable to elect a leader and will reject write requests.

This behavior prevents split-brain scenarios where different parts of the system make conflicting
decisions. However, it also means that your application may become unavailable for writes if no
subgroups have a majority of servers.

Consider the implications of different partition scenarios when designing your Raft deployment.
If you're deploying across multiple data centers, consider how network partitions between data
centers might affect availability. You may need to choose between consistency and availability
based on your application's requirements.

---
Next: [Advanced Topics](advanced.md)