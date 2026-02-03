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

Previous: [Operations and Management](operations.md) | Top:[Overview of Raft and Ratis](index.md)

## Section 5: Advanced Topics

* [Scaling with Multi-Raft Groups](#scaling-with-multi-raft-groups)

### Scaling with Multi-Raft Groups

As your application grows, you may find that a single Raft group becomes a bottleneck. This is
where Ratis's multi-group capability becomes valuable.

#### Understanding Multi-Raft

Multi-Raft is an implementation pattern that Ratis supports for scaling beyond the limits of a
single Raft group. In a multi-Raft setup, you run multiple independent Raft groups, each
handling a subset of your application's operations. Each group operates independently with its
own leader election, consensus, log, and state machine.

#### What is a Raft Group in Ratis?

In Ratis terminology, a "Raft Group" is a collection of servers that participate in a single
Raft cluster. Each group has a unique RaftGroupId (a UUID) that distinguishes it from other groups.
Each group consists of a set of RaftPeer objects representing the servers that participate in that
group's consensus.

#### When to Use Multiple Groups

Consider using multiple Raft groups when a single group cannot handle the required throughput,
when you can logically partition your data or operations (such as having one group per geographic
region, per customer tenant, or per data type), when you need better fault isolation (if one
group fails, other groups can continue operating), or when you need different operational
characteristics for different parts of your system.

#### Implementation Considerations

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

#### Trade-offs and Limitations

Multi-group setups are significantly more complex than single-group setups. Maintaining
consistency across groups requires application-level coordination, which can be complex and
error-prone. More groups mean more leaders to monitor, more logs to manage, and more complex
failure scenarios. Each group consumes resources, so there's a practical limit to the number of
groups per server.

#### Best Practices

Begin with a single group and only move to multiple groups when you have a clear scalability
need. Design your data model and operations to be partition-friendly from the start if you
anticipate needing multiple groups. Implement comprehensive monitoring for all groups, including
leader stability, replication lag, and resource usage.

Multi-Raft groups are a powerful scaling tool, but they should be used judiciously. The added
complexity is only worthwhile when you have clear scalability requirements that cannot be met
with a single Raft cluster.
