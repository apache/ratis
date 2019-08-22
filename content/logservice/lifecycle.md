---
title: Lifecycle
---
<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

The LogService is a system which manages a collection of logs. Each
of these logs has a defined state which allows certain operations on that
log or corresponds to actions that the system is taking on that log.

### OPEN

This is the first state for a Log which is created in the LogService. A
Log which is OPEN can be read from or written to. This Log has a corresponding
Raft Group (a quorum of servers) who are participating in the hosting of this
Log.

The only transition out from this state is to the CLOSED state.

### CLOSED

The CLOSED state indicates that a Log is no longer accepting writes. The
Log is still available to be read from the Raft Group.

A log can be transitioned from OPEN to CLOSED via the client API, but it
can also be done automatically by the LogService. When a node which was
participating in the Raft Group for this Log becomes unreachable, we consider
this Group to be unhealthy and proactively close it to prevent any additional
writes which may block due to too few nodes to accept a write.

The transition from OPEN to CLOSED is one-way: a Log cannot transition back
to the OPEN state from the CLOSED state. A CLOSED log may be deleted from the
system.

From the CLOSED state, a log can be transitioned to the ARCHIVING state or the DELETED
state.

### DELETED

This is a simple state that is short lived. It tracks the clean up
of any state from the hosting this Log. There are no transitions out
of this state.

### ARCHIVING

The ARCHIVING state is reached by the archive API call from
the LogService client. An archival of a log is equivalent to an export
of that log from the beginning of the log file to a known location. See
below for a tangent on exporting versus archiving.

This state indicates that the LogService is in the process of copying all
records in the Log from the starting offset of the archival request to the
specified location (a user-provided location or a preconfigured location).
We expect the location to be in some remote storage system such as HDFS or S3.

The only transition out from this state is to ARCHIVED.

### ARCHIVED

A Log can only reach the ARCHIVED state from the ARCHIVING state. This state
is automatically transitioned into when the archival of a log is done in
its entirety.

The action of archiving a log is an asynchronous process, managed by the leader
of the Raft Group, thus watching for this state on a log indicates when the
asynchronous archival is complete and the log can be safely read from the
archived location.

The only transition out from this state is to DELETED.

## Archive and Export

The archive and export API calls are very similar in nature but have
important distinctions in their implementation. As mentioned above,
an archival of a log is an export of the entire log to a specific location.

An archival of a log is specification of export in that:

* An archival of a log requires it to be CLOSED.
* An archived log cannot receive new writes.

An export of a log is more generic in that:

* A log does not need to be CLOSED to be exported.
* A log can be repeatedly exported (e.g. to multiple locations).
* More data can be appended to a log that was exported (but new data would not be reflected in the exported version of the log).
