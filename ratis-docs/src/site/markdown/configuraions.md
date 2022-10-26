# Ratis Configuration Reference

## RaftServer Configurations

All RaftServer configurations can be found at `RaftServerConfigKeys` . To customize configurations, you should first set
the expected value in the `RaftProperties` and then pass it to RaftServer builder when starting up RaftServer.

```java
final RaftProperties properties = new RaftProperties();
RaftServerConfigKeys.LeaderElection.setPreVote(properties,false);

// pass the customized properties when building the RaftServer
RaftServer server = RaftServer.newBuilder()
      .setProperties(properties)
      .build();
```

### Server

| **Param**       | **raft.server.storage.dir**                    |
|-----------------|------------------------------------------------|
| **Description** | root storage directory to hold RaftServer data |
| **Type**        | string                                         |
| **Default**     | /tmp/raft-server/                              |

If the storage.dir already contains data (from last run), RaftServer will try recovering from existing data when it
starts up.

| **Param**       | **raft.server.storage.free-space.min**    |
|-----------------|-------------------------------------------|
| **Description** | minimal space requirement for storage dir |
| **Type**        | SizeInBytes                               |
| **Default**     | 0MB                                       |

| **Param**       | **raft.server.removed.groups.dir**       |
|-----------------|------------------------------------------|
| **Description** | storage directory to hold removed groups |
| **Type**        | string                                   |
| **Default**     | /tmp/raft-server/removed-groups/         |

```java
RaftClientReply remove(RaftGroupId groupId,boolean deleteDirectory,boolean renameDirectory);
```

When removing an existing group, if the `deleteDirectory` flag is set to false and `renameDirectory` is set to true, the
group data will be renamed to this dir instead of being deleted.

| **Param**       | **raft.server.sleep.deviation.threshold** |
|-----------------|-------------------------------------------|
| **Description** | deviation threshold for election sleep    |
| **Type**        | TimeDuration                              |
| **Default**     | 300ms                                     |

When a server comes around from election sleep, if it discovers that the actual sleep time deviates from the election
timeout by this threshold, it will immediately come back to sleep and won't start any (potential) elections.

| **Param**       | **raft.server.staging.catchup.gap** |
|-----------------|-------------------------------------|
| **Description** | catching up standard of a new peer  |
| **Type**        | int                                 |
| **Default**     | 1000                                |

When bootstrapping a new peer, If the gap between the match index of the
peer and the leader's latest committed index is less than this gap, we
treat the peer as caught-up. Increase this number when write throughput is high.

### ThreadPool

Configurations related to RaftServer thread pools.

* proxy thread pool: threads that recover and initialize RaftGroups when RaftServer starts.
* server thread pool: threads that handle internal RPCs, like appendEntries.
* client thread pool: threads that handle client requests, like client.io().send().

| **Param**       | **raft.server.threadpool.proxy.cached** |
|-----------------|-----------------------------------------|
| **Description** | use CachedThreadPool for proxy-pool     |
| **Type**        | boolean                                 |
| **Default**     | true                                    |

| **Param**       | **raft.server.threadpool.proxy.size** |
|-----------------|---------------------------------------|
| **Description** | maximumPoolSize for proxy-pool        |
| **Type**        | int                                   |
| **Default**     | 0                                     |

| **Param**       | **raft.server.threadpool.server.cached** |
|-----------------|------------------------------------------|
| **Description** | use CachedThreadPool for server-pool     |
| **Type**        | boolean                                  |
| **Default**     | true                                     |

| **Param**       | **raft.server.threadpool.proxy.size** |
|-----------------|---------------------------------------|
| **Description** | maximumPoolSize for server-pool       |
| **Type**        | int                                   |
| **Default**     | 0                                     |

| **Param**       | **raft.server.threadpool.client.cached** |
|-----------------|------------------------------------------|
| **Description** | use CachedThreadPool for client-pool     |
| **Type**        | boolean                                  |
| **Default**     | true                                     |

| **Param**       | **raft.server.threadpool.client.size** |
|-----------------|----------------------------------------|
| **Description** | maximumPoolSize for client-pool        |
| **Type**        | int                                    |
| **Default**     | 0                                      |

### Read

Configurations related to read-only requests.

| **Param**       | **raft.server.read.option** |
|-----------------|-----------------------------|
| **Description** | read-only option            |
| **Type**        | enum                        |
| **Default**     | Option.DEFAULT              |

1. `Option.DEFAULT`: Directly query statemachine. Efficient but may undermine linearizability.
2. `Option.LINEARIZABLE`: Use ReadIndex (see Raft Paper section 6.4). Maintains linearizability.

| **Param**       | **raft.server.read.timeout**                        |
|-----------------|-----------------------------------------------------|
| **Description** | request timeout for linearizable read-only requests |
| **Type**        | TimeDuration                                        |
| **Default**     | 10s                                                 |

### Write

Configurations related to write requests.

| **Param**       | **raft.server.write.element-limit** |
|-----------------|-------------------------------------|
| **Description** | max pending write requests          |
| **Type**        | int                                 |
| **Default**     | 4096                                |

| **Param**       | **raft.server.write.byte-limit**    |
|-----------------|-------------------------------------|
| **Description** | max pending write requests sum size |
| **Type**        | SizeInBytes                         |
| **Default**     | 64MB                                |

Ratis imposes limitations on pending write requests. If the pending requests queue size exceeds element-limit or the
size of bytes accumulated in pending requests exceeds byte-limit, RaftServer will reject incoming write requests until
the pending situation is relieved.

| **Param**       | **raft.server.write.follower.gap.ratio.max**                                                                            |
|-----------------|-------------------------------------------------------------------------------------------------------------------------|
| **Description** | set a threshold between the majority committed index and slow follower's committed index to guarantee the data in cache |
| **Type**        | int                                                                                                                     |
| **Default**     | -1, disable the feature                                                                                                 |

### Watch

Configurations related to watch requests.

| **Param**       | **raft.server.watch.element-limit** |
|-----------------|-------------------------------------|
| **Description** | max pending watch requests          |
| **Type**        | int                                 |
| **Default**     | 65536                               |

| **Param**       | **raft.server.watch.timeout** |
|-----------------|-------------------------------|
| **Description** | watch request timeout         |
| **Type**        | TimeDuration                  |
| **Default**     | 10s                           |

| **Param**       | **raft.server.watch.timeout.denomination**         |
|-----------------|----------------------------------------------------|
| **Description** | watch request timeout denomination for rounding up |
| **Type**        | TimeDuration                                       |
| **Default**     | 1s                                                 |

watch.timeout should be a multiple of watch.timeout.denomination

### Log

Configurations related to raft log.

| **Param**       | **raft.server.log.use.memory** |
|-----------------|--------------------------------|
| **Description** | use memory RaftLog             |
| **Type**        | boolean                        |
| **Default**     | false                          |

| **Param**       | **raft.server.log.queue.element-limit** |
|-----------------|-----------------------------------------|
| **Description** | max pending log tasks                   |
| **Type**        | int                                     |
| **Default**     | 4096                                    |

| **Param**       | **raft.server.log.queue.byte-limit**           |
|-----------------|------------------------------------------------|
| **Description** | size of bytes accumulated in pending log tasks |
| **Type**        | SizeInBytes                                    |
| **Default**     | 64MB                                           |

log.queue.byte-limit and log.queue.element-limit are quite similar to write.element-limit and .write.byte-limit. When
pending IO tasks reached the limit, Ratis will temporarily stall new IO Tasks.

| **Param**       | **raft.server.log.purge.gap**           |
|-----------------|-----------------------------------------|
| **Description** | minimal log gap between two purge tasks |
| **Type**        | int                                     |
| **Default**     | 1024                                    |

| **Param**       | **raft.server.log.purge.upto.snapshot.index**               |
|-----------------|-------------------------------------------------------------|
| **Description** | purge logs up to snapshot index when a takes a new snapshot |
| **Type**        | boolean                                                     |
| **Default**     | false                                                       |

| **Param**       | **raft.server.log.purge.preservation.log.num**               |
|-----------------|--------------------------------------------------------------|
| **Description** | preserve certain logs when purging logs up to snapshot index |
| **Type**        | long                                                         |
| **Default**     | 0                                                            |

| **Param**       | **raft.server.log.segment.size.max**        |
|-----------------|---------------------------------------------|
| **Description** | max file size for a single Raft Log Segment |
| **Type**        | SizeInBytes                                 |
| **Default**     | 8MB                                         |

| **Param**       | **raft.server.log.segment.cache.num.max**                                |
|-----------------|--------------------------------------------------------------------------|
| **Description** | besides the open segment, the max number of segments caching log entries |
| **Type**        | int                                                                      |
| **Default**     | 6                                                                        |

| **Param**       | **raft.server.log.segment.cache.size.max**        |
|-----------------|---------------------------------------------------|
| **Description** | the max byte size of segments caching log entries |
| **Type**        | SizeInBytes                                       |
| **Default**     | 200MB                                             |

| **Param**       | **raft.server.log.preallocated.size** |
|-----------------|---------------------------------------|
| **Description** | preallocate size of log segments      |
| **Type**        | SizeInBytes                           |
| **Default**     | 4MB                                   |

| **Param**       | **raft.server.log.write.buffer.size**                 |
|-----------------|-------------------------------------------------------|
| **Description** | size of direct byte buffer for SegmentLog FileChannel |
| **Type**        | SizeInBytes                                           |
| **Default**     | 64KB                                                  |

| **Param**       | **raft.server.log.force.sync.num**                                              |
|-----------------|---------------------------------------------------------------------------------|
| **Description** | perform RaftLog flush tasks when pending flush tasks num exceeds force.sync.num |
| **Type**        | int                                                                             |
| **Default**     | 128                                                                             |

| **Param**       | **raft.server.log.unsafe-flush.enabled**                                               |
|-----------------|----------------------------------------------------------------------------------------|
| **Description** | unsafe-flush allow increasing flush index without waiting the actual flush to complete |
| **Type**        | boolean                                                                                |
| **Default**     | false                                                                                  |

| **Param**       | **raft.server.log.async-flush.enabled**                                     |
|-----------------|-----------------------------------------------------------------------------|
| **Description** | async-flush will increase flush index until the actual flush has completed. |
| **Type**        | boolean                                                                     |
| **Default**     | false                                                                       |

| **Param**       | **raft.server.log.corruption.policy**   |
|-----------------|-----------------------------------------|
| **Description** | the policy to handle corrupted raft log |
| **Type**        | enum                                    |
| **Default**     | CorruptionPolicy.EXCEPTION              |

1. `CorruptionPolicy.EXCEPTION`: Rethrow the exception.
2. `CorruptionPolicy.WARN_AND_RETURN`: Print a warning log message and return all uncorrupted log entries up to the
   corruption.

#### StateMachineData

configurations related to `StateMachine.DataApi`

| **Param**       | **raft.server.log.statemachine.data.sync**              |
|-----------------|---------------------------------------------------------|
| **Description** | RaftLog flush should wait for statemachine data to sync |
| **Type**        | boolean                                                 |
| **Default**     | true                                                    |

| **Param**       | **raft.server.log.statemachine.data.sync.timeout** |
|-----------------|----------------------------------------------------|
| **Description** | max timeout for statemachine data sync             |
| **Type**        | TimeDuration                                       |
| **Default**     | 10s                                                |

| **Param**       | **raft.server.log.statemachine.data.sync.timeout.retry** |
|-----------------|----------------------------------------------------------|
| **Description** | retry policy when statemachine data sync timeouts        |
| **Type**        | int                                                      |
| **Default**     | -1                                                       |

* -1: retry indefinitely
* 0: no retry
* \>0: the number of retries

| **Param**       | **raft.server.log.statemachine.data.read.timeout**       |
|-----------------|----------------------------------------------------------|
| **Description** | statemachine data read timeout when get entire log entry |
| **Type**        | TimeDuration                                             |
| **Default**     | 1000ms                                                   |

| **Param**       | **raft.server.log.statemachine.data.caching.enabled** |
|-----------------|-------------------------------------------------------|
| **Description** | enable RaftLogCache to cache statemachine data        |
| **Type**        | boolean                                               |
| **Default**     | false                                                 |

If disabled, The stateMachineData will be cached inside the StateMachine itself. RaftLogCache will remove the state
machine data part when caching a LogEntry.

#### Appender

Configurations related to leader's LogAppender

| **Param**       | **raft.server.log.appender.buffer.element-limit** |
|-----------------|---------------------------------------------------|
| **Description** | limits on AppendEntries RPC log entries           |
| **Type**        | int                                               |
| **Default**     | 0, means no limit                                 |

| **Param**       | **raft.server.log.appender.buffer.byte-limit**                |
|-----------------|---------------------------------------------------------------|
| **Description** | limits on byte size of a single AppendEntries RPC log entries |
| **Type**        | SizeInBytes                                                   |
| **Default**     | 4MB                                                           |

It is the limit of
* max serialized size of a single Log Entry. 
* max payload of a single AppendEntries RPC.

| **Param**       | **raft.server.log.appender.snapshot.chunk.size.max**                     |
|-----------------|--------------------------------------------------------------------------|
| **Description** | max chunk size of the snapshot contained in a single InstallSnapshot RPC |
| **Type**        | SizeInBytes                                                              |
| **Default**     | 16MB                                                                     |

| **Param**       | **raft.server.log.appender.install.snapshot.enabled** |
|-----------------|-------------------------------------------------------|
| **Description** | allow leader to send snapshot to followers            |
| **Type**        | boolean                                               |
| **Default**     | true                                                  |

If install.snapshot.enabled is disabled, leader won't send the snapshot to follower. Instead, if the leader detects that
it does not contain the missing logs of a follower, it will just send a notification to that follower, and the
follower's statemachine is responsible for fetching and installing snapshot from leader statemachine.

| **Param**       | **raft.server.log.appender.wait-time.min**   |
|-----------------|----------------------------------------------|
| **Description** | wait time between 2 subsequent AppendEntries |
| **Type**        | TimeDuration                                 |
| **Default**     | 10ms                                         |

### Snapshot

Configurations related to snapshot.

| **Param**       | **raft.server.snapshot.auto.trigger.enabled**                           |
|-----------------|-------------------------------------------------------------------------|
| **Description** | whether trigger snapshot when log size exceeds limit                    |
| **Type**        | boolean                                                                 |
| **Default**     | false, by default let the state machine to decide when to do checkpoint |

| **Param**       | **raft.server.snapshot.creation.gap**                |
|-----------------|------------------------------------------------------|
| **Description** | the log index gap between to two snapshot creations. |
| **Type**        | long                                                 |
| **Default**     | 1024                                                 |

| **Param**       | **raft.server.snapshot.auto.trigger.threshold**                      |
|-----------------|----------------------------------------------------------------------|
| **Description** | log size limit (in number of log entries) that triggers the snapshot |
| **Type**        | long                                                                 |
| **Default**     | 400000                                                               |

| **Param**       | **raft.server.snapshot.retention.file.num** |
|-----------------|---------------------------------------------|
| **Description** | retentive how many old snapshot versions    |
| **Type**        | int                                         |
| **Default**     | -1, means only keep latest snapshot         |

### DataStream

ThreadPool configurations related to DataStream Api.

| **Param**       | **raft.server.data-stream.async.request.thread.pool.cached** |
|-----------------|--------------------------------------------------------------|
| **Description** | use CachedThreadPool for async request pool                  |
| **Type**        | boolean                                                      |
| **Default**     | false                                                        |

| **Param**       | **raft.server.data-stream.async.request.thread.pool.size** |
|-----------------|------------------------------------------------------------|
| **Description** | maximumPoolSize for async request pool                     |
| **Type**        | int                                                        |
| **Default**     | 32                                                         |

| **Param**       | **raft.server.data-stream.async.write.thread.pool.cached** |
|-----------------|------------------------------------------------------------|
| **Description** | use CachedThreadPool for async write pool                  |
| **Type**        | boolean                                                    |
| **Default**     | false                                                      |

| **Param**       | **raft.server.data-stream.async.write.thread.pool.size** |
|-----------------|----------------------------------------------------------|
| **Description** | maximumPoolSize for async write pool                     |
| **Type**        | int                                                      |
| **Default**     | 16                                                       |

| **Param**       | **raft.server.data-stream.client.pool.size** |
|-----------------|----------------------------------------------|
| **Description** | maximumPoolSize for data stream client pool  |
| **Type**        | int                                          |
| **Default**     | 10                                           |


### RPC

Configurations related to Server RPC timeout.

| **Param**       | **raft.server.rpc.timeout.min**    |
|-----------------|------------------------------------|
| **Description** | Raft Protocol min election timeout |
| **Type**        | TimeDuration                       |
| **Default**     | 150ms                              |

| **Param**       | **raft.server.rpc.timeout.max**    |
|-----------------|------------------------------------|
| **Description** | Raft Protocol max election timeout |
| **Type**        | TimeDuration                       |
| **Default**     | 300ms                              |

First election timeout is introduced to reduce unavailable time when a RaftGroup initially starts up.

| **Param**       | **raft.server.rpc.first-election.timeout.min** |
|-----------------|------------------------------------------------|
| **Description** | Raft Protocol min election timeout             |
| **Type**        | TimeDuration                                   |
| **Default**     | 150ms                                          |

| **Param**       | **raft.server.rpc.first-election.timeout.max** |
|-----------------|------------------------------------------------|
| **Description** | Raft Protocol max election timeout             |
| **Type**        | TimeDuration                                   |
| **Default**     | 300ms                                          |

| **Param**       | **raft.server.rpc.request.timeout** |
|-----------------|-------------------------------------|
| **Description** | timeout for AppendEntries RPC       |
| **Type**        | TimeDuration                        |
| **Default**     | 3000ms                              |

| **Param**       | **raft.server.rpc.sleep.time**               |
|-----------------|----------------------------------------------|
| **Description** | sleep time of 2 subsequent AppendEntries RPC |
| **Type**        | TimeDuration                                 |
| **Default**     | 25ms                                         |

| **Param**       | **raft.server.rpc.slowness.timeout** |
|-----------------|--------------------------------------|
| **Description** | slowness timeout                     |
| **Type**        | TimeDuration                         |
| **Default**     | 60s                                  |

slowness.timeout is use in two places:

* Leader would consider a follower 'slow' if slowness.timeout elapsed without hearing any responses from this follower.
* If server monitors a JVM Pause longer than slowness.timeout, it would shut down self.

#### RetryCache

configuration related to server retry cache.

| **Param**       | **raft.server.retrycache.expire-time** |
|-----------------|----------------------------------------|
| **Description** | expire time of retry cache entry.      |
| **Type**        | TimeDuration                           |
| **Default**     | 60s                                    |

| **Param**       | **raft.server.retrycache.statistics.expire-time** |
|-----------------|---------------------------------------------------|
| **Description** | expire time of retry cache statistics.            |
| **Type**        | TimeDuration                                      |
| **Default**     | 100us                                             |

#### Notification

Configurations related to state machine notifications.

| **Param**       | **raft.server.notification.no-leader.timeout**                                  |
|-----------------|---------------------------------------------------------------------------------|
| **Description** | timeout value to notify the state machine when there is no leader for a period. |
| **Type**        | TimeDuration                                                                    |
| **Default**     | 60s                                                                             |

#### LeaderElection

Configurations related to leader election.

| **Param**       | **raft.server.leaderelection.leader.step-down.wait-time**                |
|-----------------|--------------------------------------------------------------------------|
| **Description** | when a leader steps down, it can't be re-elected until wait-time elapsed |
| **Type**        | TimeDuration                                                             |
| **Default**     | 10s                                                                      |

| **Param**       | **raft.server.leaderelection.pre-vote** |
|-----------------|-----------------------------------------|
| **Description** | enable pre-vote                         |
| **Type**        | bool                                    |
| **Default**     | true                                    |

In Pre-Vote, the candidate does not change its term and try to learn
if a majority of the cluster would be willing to grant the candidate their votes
(if the candidate’s log is sufficiently up-to-date, 
and the voters have not received heartbeats from a valid leader
for at least a baseline election timeout).

See Ongaro, D. Consensus: Bridging Theory and Practice. PhD thesis, Stanford University, 2014.
Available at https://github.com/ongardie/dissertation
