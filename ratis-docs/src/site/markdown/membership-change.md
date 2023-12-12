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

# Ratis Membership Change

## Overview

Adjusting the configuration, such as replacing failed servers or changing replication levels, 
is sometimes necessary in practice. 
Ratis facilitates these functionalities through joint consensus, 
allowing for simultaneous addition or replacement of multiple servers in a cluster.

> For details on how joint consensus algorithm works, please refer to section 4.3 in 
[Raft Paper](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)

During membership changes, the availability guarantee of Raft is more vulnerable than usual.
Agreement (for elections and entry commitment) requires separate majorities from both the
old and new configurations.
For example, when changing from a cluster of 3 servers to a different cluster of 9 servers, 
agreement requires both 2 of the 3 servers in the old configuration 
and 5 of the 9 servers in the new configuration.
Be carefully to keep both separate majorities online! 

## Adding a new server

To add a new node (e.g., N3) to an existing group (e.g., N0, N1, N2), follow these steps:

1. Start the new peer N3 with **EMPTY** group. 

```java
this.serverN3 = RaftServer.newBuilder()
    .setGroup(RaftGroup.emptygroup())
    .setProperties(properties)
    .setServerId(N3)
    .setStateMachine(userStateMachine)
    .build();
this.serverN3.start()
```

2. Issue a `setConfiguration` request to any member of the existing group. 
This command waits for the new peer to catch up before returning.
```java
RaftClientReply reply = client.admin()
    .setConfiguration(List.of(N0, N1, N2, N3))
```


> Avoid starting N3 with the group (N0, N1, N2, N3) as it may lead to shutdown scenarios, 
> particularly if N3 initiates a leader election before being recognized by original peers.


## Removing an existing peer
To remove an existing node (e.g., N3) from a group (e.g., N0, N1, N2, N3), follow these steps:

1. Issue a `setConfiguration` request to the existing group to inform about the configuration change.
```java
RaftClientReply reply = client.admin()
    .setConfiguration(List.of(N0, N1, N2))
```

2. After successful completion of Step 1, stop N3 and clean up its data.
```java
serverN3.stop()
// delete the data in serverN3
```

## Arbitrary configuration change

To perform multiple member changes at one time, 
like replacing two old nodes with new ones in a five-node cluster (changing from
(N0, N1, N2, N3, N4) to (N0, N1, N2, N5, N6)), follow this steps:
1. Start new peers with **EMPTY** group configuration.
2. Issue a setConfiguration request to the original group tp inform the change
3. Close the removed peers and clean up the data.


For full code examples, you can refer to [examples/membership/sever](
https://github.com/apache/ratis/blob/53831534c69309688ce379006363e645bf42b654/ratis-examples/src/main/java/org/apache/ratis/examples/membership/server/RaftCluster.java#L68)


## ADD Mode and COMPARE_AND_SET Mode

* Default `setConfiguration` mode is `SET`, the leader will consider the given peers in request
to be the new members of the group
* When `ADD` mode is used in `setConfiguration` request, the leader will only add the new peers
in request to the existing group and will not remove any existing members.
* When `COMPARE_AND_SET` mode is used in `setConfiguration` request, 
the leader will first compare between the current configuration and the provided configuration in request,
and it will only accept the new configuration 
if the current configuration equals to the provided configuration in request.