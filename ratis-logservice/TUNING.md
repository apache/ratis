<!--
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
# Tuning for the Log Service

This is a list of Ratis configuration properties which have been
found to be relevant/important to control how Ratis operates for
the purposes of the LogService.

## RAFT Log

The default RAFT log implementation uses "segments" on disk to avoid
a single file growing to be very large. By default, each segment is
`8MB` in size and can be set by the API `RaftServerConfigKeys.Log.setSegmentSizeMax()`.
When a new segment is created, Ratis will "preallocate" that segment by writing
data into the file to reduce the risk of latency when we first try to append
entries to the RAFT log. By default, the segment is preallocated with `4MB`
and can be changed via `RaftServerConfigKeys.Log.setPreallocatedSize()`.

Up to 2 log segments are cached in memory (including the segment actively being
written to). This is controlled by `RaftServerConfigKeys.Log.setMaxCachedSegmentNum()`.
Increasing this configuration would use more memory but should reduce the latency
of reading entries from the RAFT log.

Writes to the RAFT log are buffered using a Java Direct ByteBuffer (offheap). By default,
this buffer is `64KB` in size and can be changed via `RaftServerConfigKeys.Log.setWriteBufferSize`.
Beware that when one LogServer is hosting multiple RAFT groups (multiple "LogService Logs"), each
will LogServer will have its own buffer. Thus, high concurrency will result in multiple buffers.

## RAFT Server

Every RAFT server maintains a queue of I/O actions that it needs to execute. As with
much of Ratis, these actions are executed asynchronously and the client can block on
completion of these tasks as necessary. To prevent saturating memory, this queue of
items can be limited in size by both number of entries and size of the elements in the queue.
The former defaults to 4096 elements and id controlled by `RaftServerConfigKeys.Log.setElementLimit()`,
while the latter defaults to `64MB` and is controlled by `RaftServerConfigKeys.Log.setByteLimit()`.

## Do Not Set

Running a snapshot indicates that we can truncate part of the RAFT log, as the expectation is that
a snapshot is an equivalent representation of all of the updates from the log. However, the LogService
is written to expect that we maintain these records. As such, we must not allow snapshots to automatically
happen as we may lose records from the RAFT log. `RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled()`
defaults to `false` and should not be set to `true`.
