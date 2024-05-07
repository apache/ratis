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
# Concepts

## RaftServer

The `RaftServer` is a core component of Apache Ratis,
responsible for handling all operations related to the RAFT protocol.
A `RaftServer` manages all the `RaftGroup`(s) within the current process.
For each group, a corresponding `RaftServer.Division` handles the core functions
such as replication of log entries, voting, and leader election within that group.
Each division can independently act as a Leader, Candidate, Follower or Listener,
with the specific role depending on the outcome of elections and the state of the protocol.

## RaftPeer and RaftPeerId

`RaftPeer` represents a participant node in the cluster,
including the node's unique identifier, IP address, and port number.
The unique identifier is represented by the `RaftPeerId` class,
which is crucial for distinguishing different nodes within a cluster.

## RaftGroup

A `RaftGroup` represents a collection of `RaftPeer`(s) in a Raft protocol cluster.
Each group has a unique identifier represented by the `RaftGroupId` class.
Multiple groups can operate independently within a physical network,
while each group managing its own consistency and state replication.

## Transport (gRPC, Netty, etc.)

Ratis supports various network transport protocols for node communication,
including gRPC (default) and Netty.
These transport layers in Ratis are used for data serialization and deserialization,
as well as ensuring safe and efficient data transmission between different nodes.

## RaftLog

The `RaftLog` is a core component of the Raft algorithm,
used to record all state change transactions.
Once a log entry has been acknowledged by a majority of peers,
the entry becomes committed.
The Raft log is key to achieving distributed data consistency.

## Snapshot

A `Snapshot` is a point-in-time copy of the current state of the `StateMachine`.
It can be used for quick recovery of the state after system restarts,
and for transferring the state to newly joined nodes.
When a snapshot has been taken,
the log entries earlier than the snapshot can be purged
in order to free up the storage space.

## TermIndex

`TermIndex` is an order pair of `long` integers (term, index) as defined in the Raft protocol.
Term is the logical clock in Raft.
A newly elected leader starts a new term and remains the leader for the rest of the term.
Index is the position of log entries in the Raft log.

## StateMachine

In Ratis, `StateMachine` is the abstraction point for user-defined code.
Developers implement specific business logic or data storage operations at this layer.
The transactions committed through the Raft protocol will be applied to it.

### The `applyTransaction` method

In Ratis, transaction processing is implemented by the `StateMachine`
through the `applyTransaction` method.
A transaction usually changes the state of the `StateMachine`.

### StateMachineStorage

`StateMachineStorage` is a component for storing data related to the `StateMachine`.
It is for persisting the Raft log and the snapshots
such that the state can be fully recovered even after system failures.
