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
Be careful to keep both separate majorities online! 

## Adding a new server

To add a new node (e.g., `N3`) to an existing group (e.g., `N0`, `N1`, `N2`), follow these steps:

1. Start the new peer `N3` with **EMPTY** group. 

```java
        RaftServer N3 = RaftServer.newBuilder()
            .setGroup(RaftGroup.emptygroup())
            .setProperties(properties)
            .setServerId(n3id)
            .setStateMachine(userStateMachine)
            .build();
        N3.start()
```

2. Invoke a `setConfiguration` method in the [AdminApi](
../../../../ratis-client/src/main/java/org/apache/ratis/client/api/AdminApi.java#L44) 
with the new group as the parameter.
It will wait for the new peer to catch up before returning the reply.
```java
      reply = client.admin().setConfiguration(List.of(N0, N1, N2, N3))
``` 

&#x26a0;&#xfe0f; Note
> Avoid starting `N3` with the group (`N0`, `N1`, `N2`, `N3`) as it may lead to shutdown scenarios, 
> particularly if `N3` initiates a leader election before being recognized by original peers.


## Removing an existing peer
To remove an existing node (e.g., `N3`) from a group (e.g., `N0`, `N1`, `N2`, `N3`), follow these steps:

1. Issue a `setConfiguration` request to inform about the configuration change.
```java
      reply = client.admin().setConfiguration(List.of(N0, N1, N2))
```

2. Check if `reply.isSuccess()`

Note that `N3` will automatically shut down. 
The data in `N3` can be safely deleted.

## Arbitrary configuration change

To perform multiple member changes at one time, 
like replacing two old nodes with new ones in a five-node cluster (changing from
an existing group {`N0`, `N1`, `N2`, `N3`, `N4`} to a new group {`N0`, `N1`, `N2`, `N5`, `N6`}),
follow these steps:
1. Start new peers with an **EMPTY** group.
2. Issue a `setConfiguration(List.of(N0, N1, N2, N5, N6))` request to the original group.

The removed peers `N3` and `N4` will automatically shut down.

For full code examples, see [examples/membership/server](
../../../../ratis-examples/src/main/java/org/apache/ratis/examples/membership/server/RaftCluster.java#L68).

## `ADD` Mode and `COMPARE_AND_SET` Mode

There are different `setConfiguration` modes:`SET` (default), `ADD` and `COMPARE_AND_SET`.

* `SET`:
The leader will consider the given peers in request to be the new group.
* `ADD`:
The leader will only add the new peers in request to the existing group
and will not remove any existing peers.
* `COMPARE_AND_SET`:
The leader will first compare between the current configuration and the provided configuration in request,
and it will accept the new configuration
only if the current configuration equals to the provided configuration in request.

For the `ADD` mode and the `COMPARE_AND_SET` mode,
build a `SetConfigurationRequest.Arguments` object and then pass it to
[setConfiguration(SetConfigurationRequest.Arguments)](
../../../../ratis-client/src/main/java/org/apache/ratis/client/api/AdminApi.java#L35).

## Majority-add
**Majority-add** is defined as below:
- Adding a majority of members in a single `setConfiguration` request.  In other words,
adding `n` members to a group of `m` members in a `setConfiguration` request, where `n >= m`.
  
Note that, when a `setConfiguration` request removes and adds members at the same time,
the majority is counted after the removal.
For examples, `setConfiguration` to a 3-member group by adding 2 new members is NOT a majority-add.
However, `setConfiguration` to a 3-member group by removing 2 of members and adding 2 new members
at the same time is a majority-add.

Majority-add is unsafe since it can end up in a no-leader state which cannot be recovered automatically.
The following property is to enable/disable majority-add.
- `raft.server.leaderelection.member.majority-add` (`boolean`, default=`false`)

In a monitored environment, majority-add can be enabled in order to reduce the number of `setConfiguration` calls.
As an example, change from non-HA single server to a HA 3-server group can be done in a single `setConfiguration` call,
instead of 2 `setConfiguration` calls with majority-add disabled.
In case the no-leader state problem happens, the monitor (a human or a program) can fix it by restarting the servers.