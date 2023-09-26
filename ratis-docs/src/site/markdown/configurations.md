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

# Ratis Configuration Reference

## Server Configurations

Most of the server configurations can be found at `RaftServerConfigKeys`.
To customize configurations, we may 
1. create a `RaftProperties` object,
2. set the desired values, and then
3. pass the customized `RaftProperties` object when building a `RaftServer`.

For example,
```java
final RaftProperties properties = new RaftProperties();
RaftServerConfigKeys.LeaderElection.setPreVote(properties, false);
final RaftServer raftServer = RaftServer.newBuilder()
    .setServerId(id)
    .setStateMachine(stateMachine)
    .setGroup(raftGroup)
    .setProperties(properties)
    .build();
```

### Server

| **Property**    | `raft.server.storage.dir`                      |
|:----------------|:-----------------------------------------------|
| **Description** | root storage directory to hold RaftServer data |
| **Type**        | List\<File\>                                   |
| **Default**     | /tmp/raft-server/                              |

--------------------------------------------------------------------------------

| **Property**    | `raft.server.storage.free-space.min`      |
|:----------------|:------------------------------------------|
| **Description** | minimal space requirement for storage dir |
| **Type**        | SizeInBytes                               |
| **Default**     | 0MB                                       |

---------------------------------------------------------------------------------

| **Property**    | `raft.server.removed.groups.dir`         |
|:----------------|:-----------------------------------------|
| **Description** | storage directory to hold removed groups |
| **Type**        | File                                     |
| **Default**     | /tmp/raft-server/removed-groups/         |

```java
// GroupManagementApi
RaftClientReply remove(RaftGroupId groupId,
    boolean deleteDirectory, boolean renameDirectory) throws IOException;
```
When removing an existing group,
if the `deleteDirectory` flag is set to false and `renameDirectory` is set to true,
the group data will be renamed to this dir instead of being deleted.

---------------------------------------------------------------------------------
| **Property**    | `raft.server.sleep.deviation.threshold` |
|:----------------|:----------------------------------------|
| **Description** | deviation threshold for election sleep  |
| **Type**        | TimeDuration                            |
| **Default**     | 300ms                                   |

When a server is a follower,
it sleeps and wakes up from time to time 
for checking the heartbeat condition.
If it cannot receive a heartbeat from the leader
within the election timeout,
it starts a leader election.

When a follower server wakes up from a sleep,
if the actual sleep time is longer
than the intended sleep time by this threshold,
it will immediately go back to sleep again,
instead of checking the heartbeat condition.
The extra sleep time indicates that
the server is too busy,
probably due to GC.

---------------------------------------------------------------------------------
| **Property**    | `raft.server.staging.catchup.gap`  |
|:----------------|:-----------------------------------|
| **Description** | catching up standard of a new peer |
| **Type**        | int                                |
| **Default**     | 1000                               |

When bootstrapping a new peer, If the gap between the match index of the
peer and the leader's latest committed index is less than this gap, we
treat the peer as caught-up. Increase this number when write throughput is high.

---------------------------------------------------------------------------------
### ThreadPool - Configurations related to server thread pools.

* Proxy thread pool: threads that recover and initialize RaftGroups when RaftServer starts.

| **Property**    | `raft.server.threadpool.proxy.cached`                   |
|:----------------|:--------------------------------------------------------|
| **Description** | use CachedThreadPool, otherwise, uee newFixedThreadPool |
| **Type**        | boolean                                                 |
| **Default**     | true                                                    |

| **Property**    | `raft.server.threadpool.proxy.size`                                           |
|:----------------|:------------------------------------------------------------------------------|
| **Description** | the maximum pool size                                                         |
| **Type**        | int                                                                           |
| **Default**     | 0 (means unlimited for CachedThreadPool. For FixedThreadPool, it must be >0.) |

--------------------------------------------------------------------------------
 
* Server thread pool: threads that handle internal RPCs,
  such as `appendEntries`.
 
| **Property**    | `raft.server.threadpool.server.cached`                  |
|:----------------|:--------------------------------------------------------|
| **Description** | use CachedThreadPool, otherwise, uee newFixedThreadPool |
| **Type**        | boolean                                                 |
| **Default**     | true                                                    |

| **Property**    | `raft.server.threadpool.proxy.size`                                           |
|:----------------|:------------------------------------------------------------------------------|
| **Description** | the maximum pool size                                                         |
| **Type**        | int                                                                           |
| **Default**     | 0 (means unlimited for CachedThreadPool. For FixedThreadPool, it must be >0.) |

--------------------------------------------------------------------------------

* Client thread pool: threads that handle client requests,
  such as `client.io().send()` and `client.async().send()`.
 
| **Property**    | `raft.server.threadpool.client.cached`                  |
|:----------------|:--------------------------------------------------------|
| **Description** | use CachedThreadPool, otherwise, uee newFixedThreadPool |
| **Type**        | boolean                                                 |
| **Default**     | true                                                    |

| **Property**    | `raft.server.threadpool.client.size`                                          |
|:----------------|:------------------------------------------------------------------------------|
| **Description** | the maximum pool size                                                         |
| **Type**        | int                                                                           |
| **Default**     | 0 (means unlimited for CachedThreadPool. For FixedThreadPool, it must be >0.) |

--------------------------------------------------------------------------------

### Read - Configurations related to read-only requests.


| **Property**    | `raft.server.read.option`                     |
|:----------------|:----------------------------------------------|
| **Description** | Option for processing read-only requests      |
| **Type**        | `Read.Option` enum[`DEFAULT`, `LINEARIZABLE`] |
| **Default**     | `Read.Option.DEFAULT`                         |

* `Read.Option.DEFAULT` - Directly query statemachine:
  * It is efficient but does not provide linearizability.
  * Only the leader can serve read requests.
    The followers only can serve stale-read requests.
  
* `Read.Option.LINEARIZABLE` - Use ReadIndex (see Raft Paper section 6.4):
  * It provides linearizability.
  * All the leader and the followers can serve read requests.

--------------------------------------------------------------------------------

| **Property**    | `raft.server.read.timeout`                          |
|:----------------|:----------------------------------------------------|
| **Description** | request timeout for linearizable read-only requests |
| **Type**        | TimeDuration                                        |
| **Default**     | 10s                                                 |

--------------------------------------------------------------------------------

| **Property**    | `raft.server.read.leader.lease.enabled`                    |
|:----------------|:-----------------------------------------------------------|
| **Description** | whether to enable lease in linearizable read-only requests |
| **Type**        | boolean                                                    |
| **Default**     | true                                                       |

--------------------------------------------------------------------------------

| **Property**    | `raft.server.read.leader.lease.timeout.ratio` |
|:----------------|:----------------------------------------------|
| **Description** | maximum timeout ratio of leader lease         |
| **Type**        | double, ranging from (0.0,1.0)                |
| **Default**     | 0.9                                           |


### Write - Configurations related to write requests.

* Limits on pending write requests

| **Property**    | `raft.server.write.element-limit`        |
|:----------------|:-----------------------------------------|
| **Description** | maximum number of pending write requests |
| **Type**        | int                                      |
| **Default**     | 4096                                     |

| **Property**    | `raft.server.write.byte-limit`                  |
|:----------------|:------------------------------------------------|
| **Description** | maximum byte size of all pending write requests |
| **Type**        | SizeInBytes                                     |
| **Default**     | 64MB                                            |

Ratis imposes limitations on pending write requests.
If the number of pending requests exceeds element-limit 
or the request size accumulated in pending requests exceeds byte-limit,
the server rejects new incoming write requests until the pending situation is relieved.

--------------------------------------------------------------------------------

| **Property**    | `raft.server.write.follower.gap.ratio.max`                                                                            |
|:----------------|:----------------------------------------------------------------------------------------------------------------------|
| **Description** | the threshold between the majority committed index and slow follower's committed index to guarantee the data in cache |
| **Type**        | int                                                                                                                   |
| **Default**     | -1, disable the feature                                                                                               |

--------------------------------------------------------------------------------

### Watch - Configurations related to watch requests.


| **Property**    | `raft.server.watch.element-limit`        |
|:----------------|:-----------------------------------------|
| **Description** | maximum number of pending watch requests |
| **Type**        | int                                      |
| **Default**     | 65536                                    |

| **Property**    | `raft.server.watch.timeout` |
|:----------------|:----------------------------|
| **Description** | watch request timeout       |
| **Type**        | TimeDuration                |
| **Default**     | 10s                         |

| **Property**    | `raft.server.watch.timeout.denomination`           |
|:----------------|:---------------------------------------------------|
| **Description** | watch request timeout denomination for rounding up |
| **Type**        | TimeDuration                                       |
| **Default**     | 1s                                                 |

Note that `watch.timeout` must be a multiple of `watch.timeout.denomination`.

--------------------------------------------------------------------------------

### Log - Configurations related to raft log.

| **Property**    | `raft.server.log.use.memory` |
|:----------------|:-----------------------------|
| **Description** | use memory RaftLog           |
| **Type**        | boolean                      |
| **Default**     | false                        |

Only use memory RaftLog for testing.

--------------------------------------------------------------------------------
| **Property**    | `raft.server.log.queue.element-limit` |
|:----------------|:--------------------------------------|
| **Description** | maximum number of pending log tasks   |
| **Type**        | int                                   |
| **Default**     | 4096                                  |

| **Property**    | `raft.server.log.queue.byte-limit`          |
|:----------------|:--------------------------------------------|
| **Description** | maximum bytes size of all pending log tasks |
| **Type**        | SizeInBytes                                 |
| **Default**     | 64MB                                        |

Note that `log.queue.element-limit` and `log.queue.byte-limit`
are similar to `write.element-limit` and `write.byte-limit`.
When the pending IO tasks reached the limit,
Ratis will temporarily stall the new IO Tasks.

--------------------------------------------------------------------------------

| **Property**    | `raft.server.log.purge.gap`             |
|:----------------|:----------------------------------------|
| **Description** | minimal log gap between two purge tasks |
| **Type**        | int                                     |
| **Default**     | 1024                                    |

| **Property**    | `raft.server.log.purge.upto.snapshot.index`                |
|:----------------|:-----------------------------------------------------------|
| **Description** | purge logs up to snapshot index when taking a new snapshot |
| **Type**        | boolean                                                    |
| **Default**     | false                                                      |

| **Property**    | `raft.server.log.purge.preservation.log.num`         |
|:----------------|:-----------------------------------------------------|
| **Description** | preserve logs when purging logs up to snapshot index |
| **Type**        | long                                                 |
| **Default**     | 0                                                    |

| **Property**    | `raft.server.log.segment.size.max`          |
|:----------------|:--------------------------------------------|
| **Description** | max file size for a single Raft Log Segment |
| **Type**        | SizeInBytes                                 |
| **Default**     | 8MB                                         |

| **Property**    | `raft.server.log.segment.cache.num.max`                                     |
|:----------------|:----------------------------------------------------------------------------|
| **Description** | the maximum number of segments caching log entries besides the open segment |
| **Type**        | int                                                                         |
| **Default**     | 6                                                                           |

| **Property**    | `raft.server.log.segment.cache.size.max`              |
|:----------------|:------------------------------------------------------|
| **Description** | the maximum byte size of segments caching log entries |
| **Type**        | SizeInBytes                                           |
| **Default**     | 200MB                                                 |

| **Property**    | `raft.server.log.preallocated.size` |
|:----------------|:------------------------------------|
| **Description** | preallocate size of log segment     |
| **Type**        | SizeInBytes                         |
| **Default**     | 4MB                                 |

| **Property**    | `raft.server.log.write.buffer.size`                         |
|:----------------|:------------------------------------------------------------|
| **Description** | size of direct byte buffer for SegmentedRaftLog FileChannel |
| **Type**        | SizeInBytes                                                 |
| **Default**     | 64KB                                                        |

| **Property**    | `raft.server.log.force.sync.num`                                                |
|:----------------|:--------------------------------------------------------------------------------|
| **Description** | perform RaftLog flush tasks when pending flush tasks num exceeds force.sync.num |
| **Type**        | int                                                                             |
| **Default**     | 128                                                                             |

| **Property**    | `raft.server.log.unsafe-flush.enabled`                                                        |
|:----------------|:----------------------------------------------------------------------------------------------|
| **Description** | unsafe-flush allows increasing flush index without waiting the actual async-flush to complete |
| **Type**        | boolean                                                                                       |
| **Default**     | false                                                                                         |

| **Property**    | `raft.server.log.async-flush.enabled`                   |
|:----------------|:--------------------------------------------------------|
| **Description** | async-flush enables to flush the RaftLog asynchronously |
| **Type**        | boolean                                                 |
| **Default**     | false                                                   |

| **Property**    | `raft.server.log.corruption.policy`                          |
|:----------------|:-------------------------------------------------------------|
| **Description** | the policy to handle corrupted raft log                      |
| **Type**        | `Log.CorruptionPolicy` enum [`EXCEPTION`, `WARN_AND_RETURN`] |
| **Default**     | CorruptionPolicy.EXCEPTION                                   |

1. `Log.CorruptionPolicy.EXCEPTION`:
   Rethrow the exception.
2. `Log.CorruptionPolicy.WARN_AND_RETURN`:
   Print a warning log message and return all uncorrupted log entries up to the corruption.

--------------------------------------------------------------------------------

#### StateMachineData - Configurations related to `StateMachine.DataApi`

| **Property**    | `raft.server.log.statemachine.data.sync`                |
|:----------------|:--------------------------------------------------------|
| **Description** | RaftLog flush should wait for statemachine data to sync |
| **Type**        | boolean                                                 |
| **Default**     | true                                                    |

| **Property**    | `raft.server.log.statemachine.data.sync.timeout` |
|:----------------|:-------------------------------------------------|
| **Description** | maximum timeout for statemachine data sync       |
| **Type**        | TimeDuration                                     |
| **Default**     | 10s                                              |

| **Property**    | `raft.server.log.statemachine.data.sync.timeout.retry` |
|:----------------|:-------------------------------------------------------|
| **Description** | retry policy when statemachine data sync timeouts      |
| **Type**        | int                                                    |
| **Default**     | -1                                                     |

* -1: retry indefinitely
* 0: no retry
* \>0: the number of retries
--------------------------------------------------------------------------------
| **Property**    | `raft.server.log.statemachine.data.read.timeout`         |
|:----------------|:---------------------------------------------------------|
| **Description** | statemachine data read timeout when get entire log entry |
| **Type**        | TimeDuration                                             |
| **Default**     | 1000ms                                                   |

--------------------------------------------------------------------------------

| **Property**    | `raft.server.log.statemachine.data.caching.enabled` |
|:----------------|:----------------------------------------------------|
| **Description** | enable RaftLogCache to cache statemachine data      |
| **Type**        | boolean                                             |
| **Default**     | false                                               |

If disabled, the state machine is responsible to cache the data.
RaftLogCache will remove the state machine data part when caching a LogEntry.
It is to avoid double caching.

--------------------------------------------------------------------------------

#### Appender - Configurations related to leader's LogAppender

| **Property**    | `raft.server.log.appender.buffer.element-limit`            |
|:----------------|:-----------------------------------------------------------|
| **Description** | limits on log entries num of in a single AppendEntries RPC |
| **Type**        | int                                                        |
| **Default**     | 0, means no limit                                          |

| **Property**    | `raft.server.log.appender.buffer.byte-limit`                         |
|:----------------|:---------------------------------------------------------------------|
| **Description** | limits on byte size of all RPC log entries in a single AppendEntries |
| **Type**        | SizeInBytes                                                          |
| **Default**     | 4MB                                                                  |

It is the limit of
* max serialized size of a single Log Entry. 
* max payload of a single AppendEntries RPC.
--------------------------------------------------------------------------------

| **Property**    | `raft.server.log.appender.snapshot.chunk.size.max`                       |
|:----------------|:-------------------------------------------------------------------------|
| **Description** | max chunk size of the snapshot contained in a single InstallSnapshot RPC |
| **Type**        | SizeInBytes                                                              |
| **Default**     | 16MB                                                                     |

| **Property**    | `raft.server.log.appender.install.snapshot.enabled` |
|:----------------|:----------------------------------------------------|
| **Description** | allow leader to send snapshot to followers          |
| **Type**        | boolean                                             |
| **Default**     | true                                                |

- When `install.snapshot.enabled` is true and the leader detects that
it does not contain the missing logs of a follower,
the leader sends a snapshot to follower as specified in the Raft Consensus Algorithm.
- When `install.snapshot.enabled` is false,
the leader won't send snapshots to follower.
It will just send a notification to that follower instead.
The follower's statemachine is responsible for fetching and installing snapshot by some other means.

| **Property**    | `raft.server.log.appender.wait-time.min`       |
|:----------------|:-----------------------------------------------|
| **Description** | wait time between two subsequent AppendEntries |
| **Type**        | TimeDuration                                   |
| **Default**     | 10ms                                           |

--------------------------------------------------------------------------------

### Snapshot - Configurations related to snapshot.

| **Property**    | `raft.server.snapshot.auto.trigger.enabled`                             |
|:----------------|:------------------------------------------------------------------------|
| **Description** | whether to trigger snapshot when log size exceeds limit                 |
| **Type**        | boolean                                                                 |
| **Default**     | false, by default let the state machine to decide when to do checkpoint |

| **Property**    | `raft.server.snapshot.creation.gap`                  |
|:----------------|:-----------------------------------------------------|
| **Description** | the log index gap between to two snapshot creations. |
| **Type**        | long                                                 |
| **Default**     | 1024                                                 |

| **Property**    | `raft.server.snapshot.auto.trigger.threshold`                                |
|:----------------|:-----------------------------------------------------------------------------|
| **Description** | log size limit (in number of applied log entries) that triggers the snapshot |
| **Type**        | long                                                                         |
| **Default**     | 400000                                                                       |

| **Property**    | `raft.server.snapshot.retention.file.num` |
|:----------------|:------------------------------------------|
| **Description** | how many old snapshot versions to retain  |
| **Type**        | int                                       |
| **Default**     | -1, means only keep latest snapshot       |

--------------------------------------------------------------------------------

### DataStream - ThreadPool configurations related to DataStream Api.

| **Property**    | `raft.server.data-stream.async.request.thread.pool.cached` |
|:----------------|:-----------------------------------------------------------|
| **Description** | use CachedThreadPool, otherwise, uee newFixedThreadPool    |
| **Type**        | boolean                                                    |
| **Default**     | false                                                      |

| **Property**    | `raft.server.data-stream.async.request.thread.pool.size` |
|:----------------|:---------------------------------------------------------|
| **Description** | maximumPoolSize for async request pool                   |
| **Type**        | int                                                      |
| **Default**     | 32                                                       |


| **Property**    | `raft.server.data-stream.async.write.thread.pool.cached` |
|:----------------|:---------------------------------------------------------|
| **Description** | use CachedThreadPool, otherwise, uee newFixedThreadPool  |
| **Type**        | boolean                                                  |
| **Default**     | false                                                    |

| **Property**    | `raft.server.data-stream.async.write.thread.pool.size` |
|:----------------|:-------------------------------------------------------|
| **Description** | maximumPoolSize for async write pool                   |
| **Type**        | int                                                    |
| **Default**     | 16                                                     |


| **Property**    | `raft.server.data-stream.client.pool.size`  |
|:----------------|:--------------------------------------------|
| **Description** | maximumPoolSize for data stream client pool |
| **Type**        | int                                         |
| **Default**     | 10                                          |

--------------------------------------------------------------------------------

### RPC - Configurations related to Server RPC timeout.

| **Property**    | `raft.server.rpc.request.timeout` |
|:----------------|:----------------------------------|
| **Description** | timeout for AppendEntries RPC     |
| **Type**        | TimeDuration                      |
| **Default**     | 3000ms                            |

| **Property**    | `raft.server.rpc.sleep.time`                   |
|:----------------|:-----------------------------------------------|
| **Description** | sleep time of two subsequent AppendEntries RPC |
| **Type**        | TimeDuration                                   |
| **Default**     | 25ms                                           |

| **Property**    | `raft.server.rpc.slowness.timeout` |
|:----------------|:-----------------------------------|
| **Description** | slowness timeout                   |
| **Type**        | TimeDuration                       |
| **Default**     | 60s                                |

Note that `slowness.timeout` is use in two places:
* Leader would consider a follower slow if `slowness.timeout` elapsed without hearing any responses from this follower.
* If server monitors a JVM Pause longer than `slowness.timeout`, it would shut down itself.

--------------------------------------------------------------------------------

#### RetryCache - Configuration related to server retry cache.

| **Property**    | `raft.server.retrycache.expire-time` |
|:----------------|:-------------------------------------|
| **Description** | expire time of retry cache entry     |
| **Type**        | TimeDuration                         |
| **Default**     | 60s                                  |
Note that we should set an expiration time longer than the total retry waiting duration of clients 
in order to ensure exactly-once semantic.

| **Property**    | `raft.server.retrycache.statistics.expire-time` |
|:----------------|:------------------------------------------------|
| **Description** | expire time of retry cache statistics           |
| **Type**        | TimeDuration                                    |
| **Default**     | 100us                                           |

--------------------------------------------------------------------------------

#### Notification - Configurations related to state machine notifications.

| **Property**    | `raft.server.notification.no-leader.timeout`                                   |
|:----------------|:-------------------------------------------------------------------------------|
| **Description** | timeout value to notify the state machine when there is no leader for a period |
| **Type**        | TimeDuration                                                                   |
| **Default**     | 60s                                                                            |

--------------------------------------------------------------------------------

#### LeaderElection - Configurations related to leader election.

| **Property**    | `raft.server.rpc.timeout.min`      |
|:----------------|:-----------------------------------|
| **Description** | Raft Protocol min election timeout |
| **Type**        | TimeDuration                       |
| **Default**     | 150ms                              |

| **Property**    | `raft.server.rpc.timeout.max`      |
|:----------------|:-----------------------------------|
| **Description** | Raft Protocol max election timeout |
| **Type**        | TimeDuration                       |
| **Default**     | 300ms                              |

--------------------------------------------------------------------------------

First election timeout is introduced to reduce unavailable time when a RaftGroup initially starts up.

| **Property**    | `raft.server.rpc.first-election.timeout.min` |
|:----------------|:---------------------------------------------|
| **Description** | Raft Protocol min election timeout           |
| **Type**        | TimeDuration                                 |
| **Default**     | 150ms                                        |

| **Property**    | `raft.server.rpc.first-election.timeout.max` |
|:----------------|:---------------------------------------------|
| **Description** | Raft Protocol max election timeout           |
| **Type**        | TimeDuration                                 |
| **Default**     | 300ms                                        |

--------------------------------------------------------------------------------

| **Property**    | `raft.server.leaderelection.leader.step-down.wait-time`                    |
|:----------------|:---------------------------------------------------------------------------|
| **Description** | when a leader steps down, it can't be re-elected until `wait-time` elapsed |
| **Type**        | TimeDuration                                                               |
| **Default**     | 10s                                                                        |

| **Property**    | `raft.server.leaderelection.pre-vote` |
|:----------------|:--------------------------------------|
| **Description** | enable pre-vote                       |
| **Type**        | bool                                  |
| **Default**     | true                                  |

In Pre-Vote, the candidate does not change its term and try to learn
if a majority of the cluster would be willing to grant the candidate their votes
(if the candidate’s log is sufficiently up-to-date, 
and the voters have not received heartbeats from a valid leader
for at least a baseline election timeout).
