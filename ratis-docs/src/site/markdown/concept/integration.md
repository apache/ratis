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

Previous: [Core Concepts](core-concepts.md) | Top:[Overview of Raft and Ratis](index.md)

## Section 3: Integration

* [Logical Organization of Ratis](#logical-organization-of-ratis)
* [Server Configuration and Lifecycle](#server-configuration-and-lifecycle)

### Logical Organization of Ratis

Rather than focusing on package structure, let's examine the logical components and their
relationships, understanding how they work together to provide the Raft consensus functionality.

#### Primary Integration Points

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

#### Client API Patterns

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

#### Request Flow Through the System

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


### Server Configuration and Lifecycle

`RaftServer` is the main server-side entry point, but it requires several configuration decisions
before startup. You'll need to choose a transport implementation (gRPC works well for most
deployments, while Netty provides more control), storage configuration including directories for
logs and snapshots, and key configuration like timeout and retry policies, snapshot policies,
and security settings.

A single `RaftServer` instance can participate in [multiple Raft groups](advanced.md)
simultaneously through Ratis's `Division` concept. Each group gets its own state machine instance
and storage within the server.

---
Next: [Operations and Management](operations.md)