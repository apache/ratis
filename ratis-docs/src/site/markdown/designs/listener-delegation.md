---
title: Listener Replication Delegation
summary: Delegate log replication to listeners from the leader to admin-selected followers
date: 2026-07-10
jira: RATIS-2599
status: proposed
author: Abhishek Pal
---

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

# Listener Replication Delegation (AppendEntries from Followers)

## Summary

This document describes the design for delegating log replication to listeners
from the leader to a follower.
Instead of the leader sending AppendEntries RPCs directly to every listener,
a follower can serve as the replication source for one or more listeners.
This reduces leader load in clusters with many listeners
and can reduce network hops for co-located listener-follower pairs.

## Motivation

In the current architecture,
the leader maintains a `LogAppender` for every peer in the cluster —
both followers (voting members) and listeners (non-voting members).
This means:

1. **Leader bandwidth bottleneck:**
   In clusters with many listeners,
   the leader must send every log entry N times — once per listener.
   This concentrates network I/O on a single node.

2. **Unnecessary network hops:**
   Listeners co-located with a follower (same rack/region)
   still receive data from a potentially remote leader.

3. **Leader CPU overhead:**
   Serializing and managing per-listener `LogAppender` state
   consumes leader resources that could be used for consensus-critical work.

The proposed solution offloads listener replication to a follower that already
has the data,
freeing the leader to focus on consensus with voting members.

## Design Overview

```
BEFORE (current):                       AFTER (proposed):
┌────────┐                               ┌────────┐
│ Leader │──appendEntries──► Follower1   │ Leader │──appendEntries──► Follower1
│        │──appendEntries──► Follower2   │        │──appendEntries──► Follower2
│        │──appendEntries──► Listener1   │        │──heartbeat-only──► Listener1
│        │──appendEntries──► Listener2   │        │──heartbeat-only──► Listener2
└────────┘                               └────────┘
                                              │ (delegation notification)
                                              ▼
                                         Follower1──appendEntries──► Listener1
                                         Follower2──appendEntries──► Listener2
```

### Key Invariants

1. **Leader/admin authority:**
   The delegation topology is decided by the administrator
   and enforced by the leader.
   Followers do not self-elect to serve listeners.
   *Future work:* a listener may select which follower to append from,
   but for now the admin/leader decides.

2. **Leader heartbeat continuity:**
   The leader continues sending full heartbeats
   (empty entries with `leaderCommit` and `commitInfos`)
   directly to delegated listeners.
   This maintains liveness detection
   and ensures listeners get accurate commit progress from the authoritative source.

3. **Term safety:**
   Listeners validate that `leaderTerm >= currentTerm` in delegated entries.
   A deposed leader's follower will have a stale term,
   so listeners reject stale entries automatically.

4. **Log consistency (same as before):**
   The standard Raft log consistency check (`previousLog` matching)
   still runs on the listener side, preventing any divergence regardless of the source.
   This is the exact same check the listener applies to entries from the leader today —
   only the sending peer differs.

5. **Full catch-up support (same as before):**
   The follower serves the listener using the same append/catch-up logic
   the leader uses to catch up any peer today.
   The follower can serve the listener from any `nextIndex` using its local log —
   there is no new catch-up mechanism, only a different source.

6. **Automatic fallback:**
   If the delegate follower becomes unhealthy,
   the leader resumes direct replication.
   The listener is unaware of the switch.

## Detailed Design

### 1. Protocol Changes

Extend the existing `AppendEntriesRequest` message with new fields.

```protobuf
// Leader → Follower: "here are the listeners you should serve"
message ListenerAssignmentProto {
  repeated RaftPeerProto assignedListeners = 1;
  uint64 leaderTerm = 2;
}

// Extended AppendEntriesRequestProto:
//   field 16: ListenerAssignmentProto listenerAssignment
//   field 17: bool fromFollower
```

No progress message is added on the reply path.
The leader already heartbeats listeners directly (invariant 2),
so each listener's own `AppendEntriesReply` to the leader
already carries its `matchIndex` / `commitIndex`.
The leader therefore learns listener progress directly from the listeners —
no follower → leader progress channel is needed.

### 2. Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `raft.server.listener.delegation.enabled` | `false` | Master switch for the feature |

The delegation topology itself is **not** auto-tuned by config knobs.
It is supplied by the administrator (see §3),
who knows the cluster topology (racks/regions) far better than an automatic selector.

### 3. Admin-configured Assignment

Rather than have the leader automatically rank and pick source followers,
the administrator declares the listener → source-follower mapping.
This mirrors how peers and listeners are already managed today
through `setConfiguration`
(`AdminApi.setConfiguration(servers, listeners)`,
carried in `SetConfigurationRequestProto.listeners`).

`RaftPeerProto` currently has no free-form metadata field
(it carries `id`, `address`, `priority`, `dataStreamAddress`,
`clientAddress`, `adminAddress`, `startupRole`).
To express the mapping,
add an optional source-follower field on the listener peer,
e.g. `optional bytes delegateSource` on `RaftPeerProto`,
set by the admin when submitting the listener list.
It flows through the existing pipeline:
`SetConfigurationRequest` → `PeerConfiguration` → Raft log → `LeaderStateImpl`.

**Leader behavior:**
- Reads the admin-supplied listener → follower mapping from the configuration.
- Sends `listenerAssignment` to the chosen follower
  (piggybacked on the follower's AppendEntries).
- Switches each delegated listener's `LogAppender` to **heartbeat-only mode**:
  periodic heartbeats with accurate `leaderCommit` and `commitInfos`, but no log entries.
- Immediately resumes full mode on fallback (see §6).

**ListenerDelegationState** (mutable, held by `LeaderStateImpl`):
- Tracks `listener → delegateFollower` mapping (from configuration).
- Tracks `follower → [assignedListeners]` mapping.
- Marks followers with pending notification changes.
- Provides `clear()` on term change.

### 4. Follower-side: Replication to Listeners

**FollowerListenerReplicator** (daemon thread per assigned listener):
- Reads entries from the follower's local `RaftLog`.
- Builds `AppendEntriesRequestProto` with `fromFollower=true` and the leader's term.
- Tracks `nextIndex` / `matchIndex` per listener.
- Handles `INCONSISTENCY` replies by adjusting `nextIndex` (full catch-up support).
- Uses the follower's `commitIndex` as `leaderCommit` (safe lower bound).
- Stops immediately on: term change, assignment revocation, `NOT_LEADER` reply.
- If the listener needs entries the follower no longer has (purged by snapshot):
  reports failure to the leader for snapshot-based recovery via RATIS-2428.

**FollowerListenerReplicatorManager** (lifecycle manager):
- Processes `listenerAssignment` from the leader's AppendEntries.
- Starts/stops/updates replicators based on assignment changes.

### 5. Listener-side: Accepting Delegated Entries

Modified `RaftServerImpl.appendEntriesAsync()`:

- **When `fromFollower=true` AND node is LISTENER:**
  - Validate `leaderTerm >= currentTerm` (reject stale terms).
  - Do NOT call `state.setLeader(senderId)` — the listener's known leader stays unchanged.
  - Do NOT call `changeToFollowerAndPersistMetadata` — the listener stays a listener.
  - DO update `lastRpcTime` (prevents timeout alerts).
  - DO proceed with standard log append and consistency checks.
- **When `fromFollower=true` AND node is NOT LISTENER:** reject with `NOT_LEADER`.
- **When `fromFollower=false`:** existing logic unchanged.

### 6. Leader-side: Progress Monitoring and Fallback

**Progress:**
- The leader reads each listener's `matchIndex` / `commitIndex`
  from the listener's own direct-heartbeat replies —
  the same replies it already receives today.
- No progress is extracted from the delegate follower.

**Health checks and fallback:**
- If the delegate follower becomes unresponsive
  (detected from its own AppendEntries replies),
  the leader resumes direct replication for the affected listeners
  (heartbeat-only → full mode).
- On recovery, the leader resumes the admin-configured delegation.
- The leader does not auto-reassign a listener to a different follower;
  the admin owns the topology.

**Revocation (admin change or term change):**
- Resume the leader's `LogAppender` for that listener (heartbeat-only → full).
- Notify the follower with an empty `assignedListeners` on the next heartbeat.
- The follower's `FollowerListenerReplicatorManager` stops the replicator.
