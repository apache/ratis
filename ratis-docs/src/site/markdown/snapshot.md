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

# Snapshot Guide

## Overview

Raft log grows during normal operation. As it grows larger, it occupies more space and takes more time to replay. 
Therefore, some form of log compaction is necessary for practical systems.
In Ratis, we introduce snapshot mechanism as the way to do log compaction. 

The basic idea of snapshot is to create and save a snapshot reflecting the latest state of the state machine, 
and then delete previous logs up to the checkpoint.

## Implement snapshot

To enable snapshot, we have to first implement the following two methods in `StateMachine`:

```java
/**
 * Dump the in-memory state into a snapshot file in the RaftStorage. The
 * StateMachine implementation can decide 1) its own snapshot format, 2) when
 * a snapshot is taken, and 3) how the snapshot is taken (e.g., whether the
 * snapshot blocks the state machine, and whether to purge log entries after
 * a snapshot is done).
 *
 * The snapshot should include the latest raft configuration.
 *
 * @return the largest index of the log entry that has been applied to the
 *         state machine and also included in the snapshot. Note the log purge
 *         should be handled separately.
 */
long takeSnapshot() throws IOException;
```

```java
/**
 * Returns the information for the latest durable snapshot.
 */
SnapshotInfo getLatestSnapshot();
```

Snapshotting for memory-based state machines is conceptually simple. In
snapshotting, the entire current system state is written to a snapshot on stable storage.

With disk-based state machines, a recent copy of the system state is maintained on disk as
part of normal operation. Thus, the Raft log can be discarded as soon as the state machine
reflects writes to disk, and snapshotting is used only when sending consistent disk images to
other servers.

Examples of snapshot implementation can be found at 
https://github.com/apache/ratis/blob/master/ratis-examples/src/main/java/org/apache/ratis/examples/arithmetic/ArithmeticStateMachine.java.

## Trigger a snapshot

To trigger a snapshot, we can either

* Trigger snapshot manually using `SnapshotManagementApi`. 
Note that Ratis imposes a minimal creation gap between two subsequent snapshot creation:

  ```java
  // SnapshotManagementApi
  RaftClientReply create(long timeoutMs) throws IOException;
  ```
  
  ```java
  // customize snapshot creation gap
  RaftServerConfigKeys.Snapshot.setCreationGap(properties, 1024L);
  ```
  
* Enable triggering snapshot automatically when log size exceeds limit (in number of applied log entries). 
To do so, we may turn on the auto-trigger-snapshot option in RaftProperties 
and set an appropriate triggering threshold.

  ```java
  RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
  RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, 400000);
  ```

## Purge obsolete logs after snapshot

When a snapshot is taken, Ratis will automatically discard obsolete logs.
By default, Ratis will purge logs up to min(snapshot index, safe index), 
where safe index equals to the minimal commit index of all group members.
In other words, if a log is committed by all group members and is included in the latest snapshot,
then this log can be safely deleted.

Sometimes we can choose to aggressively purge the logs up to the snapshot index 
even if some peers do not have commit index up to snapshot index. 
To do this, we can turn on the purge-up-to-snapshot-index option in RaftProperties:

```java
RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);
```

Purging the logs up to snapshot index sometimes leads to unnecessary burst of network bandwidth. 
That is, even if a follower lags behind only a few logs, the leader still needs to transfer the full snapshot. 
To avoid this situation, we can preserve some recent logs when purging logs up to snapshot index. 
To do this, we can set the number of logs to be preserved when purging in RaftProperties:

```java
RaftServerConfigKeys.Log.setPurgePreservationLogNum(properties, n);
```
Intuitively, cost of transferring n logs shall equal the cost of transferring the full snapshot.

## Retain multiple versions of snapshot

Ratis allows `StateMachine` to retain multiple versions of snapshot. 
To enable this feature, we have to:

1. Set how many number of versions to retain in RaftProperties when building the RaftServer:
   ```java
   RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties, 2);
   ```
2. Implement `StateMachineStorage.cleanupOldSnapshots` to clean up old versions of snapshot:
   ```java
   // StateMachineStorage.java
   void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) throws IOException;
   ```

## Load the latest snapshot

1. When a RaftServer restarts and tries to recover the data, it first has to read and load the latest snapshot,
   and then apply the logs not included in the snapshot. 
   We have to implement snapshot loading in `StateMachine.initialize` lifecycle hook.

    ```java
    /*
    * Initializes the State Machine with the given parameter.
    * The state machine must, if there is any, read the latest snapshot.
    */
    void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException;
    ```

2. When a RaftServer newly joins an existing cluster, 
   it has first to obtain the latest snapshot and install it locally. Before installing a snapshot,
   `StateMachine.pause` hook is called to ensure that a new snapshot can be installed.
   ```java
   /* 
   * Pauses the state machine. On return, the state machine should have closed all open files so
   * that a new snapshot can be installed.
   */
   void pause();
    ```
   After installing the snapshot, `StateMachine.reinitialize` is called. 
   We shall initialize the state machine with the latest installed snapshot in this lifecycle hook.
   ```java
   /**
   * Re-initializes the State Machine in PAUSED state. The
   * state machine is responsible reading the latest snapshot from the file system (if any) and
   * initialize itself with the latest term and index there including all the edits.
   */
   void reinitialize() throws IOException;
   ```
   

## Customize snapshot storage path

By default, Ratis assumes `StateMachine` snapshot files be placed under 
`RaftStorageDirectory.getStateMachineDir()`. When leader installs a snapshot to the follower,
Ratis will keep the snapshot layout in follower side unchanged relative to this directory. 
That is, the installed snapshot under follower's state machine directory
will have the same hierarchy as in the leader side.

`StateMachine` can also customize the snapshot storage and install directory. To do this, 
we may provide the snapshot storage root directory in `StateMachineStorage`, 
together with a temporary directory holding in-transmitting snapshot files.

```java
// StateMachineStorage
/** @return the state machine directory. */
default File getSnapshotDir() {
  return null;
}

/** @return the temporary directory. */
default File getTmpDir() {
  return null;
}
```
Examples of customizing snapshot storage path can be found at
https://github.com/apache/ratis/blob/master/ratis-server/src/test/java/org/apache/ratis/InstallSnapshotFromLeaderTests.java

## Customize snapshot installation

When a new follower joins the cluster, leader will install the latest snapshot to the follower.
`StateMachine` only needs to provide the `SnapshotInfo` of the latest snapshot, and it's Ratis'
responsibility to handle all the nuances of dividing snapshot into chunks / transferring chunks / 
validating checksum. This default implementation works well in most scenarios. 

However, there are scenarios where the default implementation cannot satisfy. To name a few:
1. `StateMachine` has a flexible snapshot layout with files scattering around different directories.
2. Follower wants to fully utilize underling topology. For example, follower may download the snapshot 
from the nearest follower instead of the leader.
3. Follower wants to download snapshot from different sources in parallel. 

Ratis provides `InstallSnapshotNotification` to let `StateMachine` take over the full control
of snapshot installation. To enable this feature, we may first turn off the leader-install-snapshot option
to disable leader explicitly installing snapshot to follower:
```java
RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
```
After this option is disabled, Whenever the leader detects that a follower needs snapshot, 
instead of installing snapshot to that follower, 
leader will only send a snapshot installation notification. 
It's now the follower's responsibility to install the latest snapshot asynchronously.
To do this, we have to implement `FollowerEventApi.notifyInstallSnapshotFromLeader`:

```java
interface FollowerEventApi {
  /**
   * Notify the {@link StateMachine} that the leader has purged entries from its log.
   * In order to catch up, the {@link StateMachine} has to install the latest snapshot asynchronously.
   *
   * @param roleInfoProto information about the current node role and rpc delay information.
   * @param firstTermIndexInLog The term-index of the first append entry available in the leader's log.
   * @return return the last term-index in the snapshot after the snapshot installation.
   */
  default CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(
      RoleInfoProto roleInfoProto, TermIndex firstTermIndexInLog) {
    return CompletableFuture.completedFuture(null);
  }
}
```
Examples of `notifyInstallSnapshotFromLeader` implementation can be found at 
https://github.com/apache/ratis/blob/master/ratis-server/src/test/java/org/apache/ratis/InstallSnapshotNotificationTests.java