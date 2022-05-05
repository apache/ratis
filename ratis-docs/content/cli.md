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

# Ratis-shell

Ratis-shell is the command line interface of Ratis.

> **Note**:
> Ratis-shell is currently only **experimental**.
> The compatibility story is not considered for the time being.

The following command can be invoked in order to get the basic usage:

```shell
$ ratis sh
Usage: ratis sh [generic options]
         [election [transfer] [stepDown] [pause] [resume]]
         [group [info] [list]]
         [peer [add] [remove] [setPriority]]
         [snapshot [create]]
```

## election
The `election` command manages leader election.
It has the following subcommands:
`transfer`, `stepDown`, `pause`, `resume`

### election transfer
Transfer a group leader to the specified server.
```
$ ratis sh election transfer -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> -address <HOSTNAME:PORT> [-groupid <RAFT_GROUP_ID>]
```

### election stepDown
Make a group leader of the given group step down its leadership.
```
$ ratis sh election stepDown -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>]
```

### election pause
Pause leader election at the specified server.
Then, the specified server would not start a leader election.
```
$ ratis sh election pause -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> -address <HOSTNAME:PORT> [-groupid <RAFT_GROUP_ID>]
```

### election resume
Resume leader election at the specified server.
```
$ ratis sh election resume -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> -address <HOSTNAME:PORT> [-groupid <RAFT_GROUP_ID>]
```