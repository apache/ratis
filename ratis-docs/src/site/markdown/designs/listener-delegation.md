---
title: Listener Replication Delegation
summary: Delegate log replication to listeners from the leader to sufficiently caught-up followers
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

This document describes the design for delegating log replication to listeners from the leader to sufficiently caught-up followers.
Instead of the leader sending AppendEntries RPCs directly to every listener, a caught-up follower can serve as the replication source for one or more listeners. This reduces leader load in clusters with many listeners and can reduce network hops for co-located listener-follower pairs.

## Motivation

In the current architecture, the leader maintains a `LogAppender` for every peer in the cluster — both followers (voting members) and listeners (non-voting members).
This means:

1. **Leader bandwidth bottleneck:** In clusters with many listeners, the leader must send every log entry N times — once per listener. This concentrates network I/O on a single node.

2. **Unnecessary network hops:** Listeners co-located with a follower (same rack/region) still receive data from a potentially remote leader.

3. **Leader CPU overhead:** Serializing and managing per-listener LogAppender state consumes leader resources that could be used for consensus-critical work.

The proposed solution offloads listener replication to followers that already have the data, freeing the leader to focus on consensus with voting members.

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

1. **Leader authority:** The leader makes all delegation decisions. Followers do not self-elect to serve listeners.

2. **Leader heartbeat continuity:** The leader continues sending full heartbeats (empty entries with `leaderCommit` and `commitInfos`) directly to delegated listeners. This maintains liveness detection and ensures listeners get accurate commit progress from the authoritative source.

3. **Term safety:** Listeners validate that `leaderTerm >= currentTerm` in delegated entries. A deposed leader's follower will have a stale term, so listeners reject stale entries automatically.

4. **Log consistency:** The standard Raft log consistency check (`previousLog` matching) still runs on the listener side preventing any divergence regardless of the source.

5. **Full catch-up support:** The follower replicator can serve listeners from any `nextIndex` using its local log — not limited to near-real-time forwarding.

6. **Automatic fallback:** If the delegate follower becomes unhealthy, the leader revokes delegation and resumes direct replication. The listener is unaware of the switch.

## Detailed Design

### 1. Protocol Changes

Extend the existing `AppendEntriesRequest/Reply` messages with new fields.

```protobuf
// Leader → Follower: "here are the listeners you should serve"
message ListenerAssignmentProto {
  repeated RaftPeerProto assignedListeners = 1;
  uint64 leaderTerm = 2;
}

// Follower → Leader: "here's how each listener is progressing"
message ListenerProgressProto {
  RaftPeerIdProto listenerId = 1;
  uint64 matchIndex = 2;
  uint64 nextIndex = 3;
  uint64 commitIndex = 4;
}

// Extended AppendEntriesRequestProto:
//   field 16: ListenerAssignmentProto listenerAssignment
//   field 17: bool fromFollower

// Extended AppendEntriesReplyProto:
//   field 8: repeated ListenerProgressProto listenerProgress
```

### 2. Configuration

All configuration under `raft.server.listener.delegation.*`:

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `false` | Master switch for the feature |
| `max-listeners-per-follower` | `5` | Maximum listeners a single follower can serve |
| `follower-lag-threshold` | `100` | Max entries a follower can lag behind leader and still be eligible |
| `reassignment-interval` | `30s` | How often the leader re-evaluates assignments |

### 3. Leader-side: Selection and Assignment

**ListenerDelegationSelector** (stateless utility, mirrors `SnapshotSourceSelector`):
- Filters followers: `leaderLastIndex - follower.matchIndex <= lagThreshold`
- Ranks by: highest matchIndex, then freshest RPC response
- Load-balances: round-robin assignment respecting `maxListenersPerFollower`
- Returns `Map<followerId, List<listener>>` — empty map triggers leader-direct fallback

**ListenerDelegationState** (mutable, held by `LeaderStateImpl`):
- Tracks `listener → delegateFollower` mapping
- Tracks `follower → [assignedListeners]` mapping
- Marks followers with pending notification changes
- Provides `revokeDelegation(listenerId)` and `clear()` (term change)

**Leader LogAppender behavior for delegated listeners:**
- Switches to **heartbeat-only mode**: sends periodic heartbeats with accurate
  `leaderCommit` and `commitInfos`, but no log entries
- Immediately resumes full mode on revocation

### 4. Follower-side: Replication to Listeners

**FollowerListenerReplicator** (daemon thread per assigned listener):
- Reads entries from the follower's local `RaftLog`
- Builds `AppendEntriesRequestProto` with `fromFollower=true` and the leader's term
- Tracks `nextIndex` / `matchIndex` per listener
- Handles `INCONSISTENCY` replies by adjusting `nextIndex` (full catch-up support)
- Uses the follower's `commitIndex` as `leaderCommit` (safe lower bound)
- Stops immediately on: term change, assignment revocation, `NOT_LEADER` reply
- If listener needs entries the follower no longer has (purged by snapshot):
  reports failure to leader for snapshot-based recovery via RATIS-2428

**FollowerListenerReplicatorManager** (lifecycle manager):
- Processes `listenerAssignment` from leader's AppendEntries
- Starts/stops/updates replicators based on assignment changes
- Collects `ListenerProgressProto` reports for piggybacking on follower's reply

### 5. Listener-side: Accepting Delegated Entries

Modified `RaftServerImpl.appendEntriesAsync()`:

- **When `fromFollower=true` AND node is LISTENER:**
  - Validate `leaderTerm >= currentTerm` (reject stale terms)
  - Do NOT call `state.setLeader(senderId)` — listener's known leader stays unchanged
  - Do NOT call `changeToFollowerAndPersistMetadata` — listener stays a listener
  - DO update `lastRpcTime` (prevents timeout alerts)
  - DO proceed with standard log append and consistency checks
- **When `fromFollower=true` AND node is NOT LISTENER:** reject with `NOT_LEADER`
- **When `fromFollower=false`:** existing logic unchanged

### 6. Leader-side: Progress Monitoring and Fallback

**Progress processing:**
- When handling `AppendEntriesReply` from a delegate follower, extract
  `listenerProgress` reports
- Update leader's own `FollowerInfo` for each reported listener (matchIndex,
  commitIndex)

**Health checks (every `reassignment-interval`):**
- Delegate follower's matchIndex fell behind `lagThreshold` → revoke
- Listener's matchIndex not advancing → revoke
- Delegate follower unresponsive → revoke
- Cooldown period prevents rapid reassignment oscillation

**Revocation:**
- Resume leader's LogAppender for that listener (heartbeat-only → full)
- Notify follower with empty `assignedListeners` on next heartbeat
- Follower's `FollowerListenerReplicatorManager` stops the replicator
