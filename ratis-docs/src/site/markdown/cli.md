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

## Availability
| Version  | Available in src tarball? | Available in bin tarball? |
| :------: | :-----------------------: | :-----------------------: |
| < 2.3.0  | No                        | No                        |
| 2.3.0    | Yes                       | No                        |
| \> 2.3.0 | Yes                       | Yes                       |

## Setting up the ratis-shell

### Setting up from a source tarball
Download the Ratis source tarball from https://ratis.apache.org/downloads.html .
Note that ratis-shell is available starting from version 2.3.0.
Extract the source tarball to a destination directory `<DST_DIR>`
and then build the ratis-shell tarball.

```
$ tar -C <DST_DIR> -zxvf apache-ratis-<VERSION>-src.tar.gz
$ cd <DST_DIR>/apache-ratis-<VERSION>-src
$ mvn -DskipTests -Prelease -Papache-release clean package assembly:single
[INFO] Scanning for projects...
 ...
[INFO] BUILD SUCCESS
```

Extract the ratis-shell tarball.
```
$ mkdir <DST_DIR>/ratis-shell
$ tar -C <DST_DIR>/ratis-shell -xzf ratis-assembly/target/apache-ratis-<VERSION>-shell.tar.gz --strip-component 1
```

### Setting up from a binary tarball
Download the Ratis bin tarball from https://ratis.apache.org/downloads.html .
Note that the bin tarball of Ratis version 2.3.0 or earlier does not contain ratis-shell.
The bin tarball of later versions will contain ratis-shell.
Extract the bin tarball to a destination directory `<DST_DIR>`
```
$ tar -C <DST_DIR> -zxvf apache-ratis-<VERSION>-bin.tar.gz apache-ratis-<VERSION>/ratis-shell
$ cd <DST_DIR>
$ mv apache-ratis-<VERSION>/ratis-shell .
$ rmdir apache-ratis-<VERSION>/
```

Export the `RATIS_SHELL_HOME` environment variable and add the bin directory to the `$PATH`.
```
$ export RATIS_SHELL_HOME=<DST_DIR>/ratis-shell
$ export PATH=${RATIS_SHELL_HOME}/bin:$PATH
```

The following command can be invoked in order to get the basic usage:

```shell
$ ratis sh
Usage: ratis sh [generic options]
         [election [transfer] [stepDown] [pause] [resume]]
         [group [info] [list]]
         [peer [add] [remove] [setPriority]]
         [snapshot [create]]
         [local [raftMetaConf]]
```

## generic options
The `generic options` supports the following content:
`-D*`, `-X*`, `-agentlib*`, `-javaagent*`

The `-D*` can pass values for a given ratis property to ratis-shell client RaftProperties.

```
$ ratis sh -D<property=value> ...
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

## group
The `group` command manages ratis groups.
It has the following subcommands:
`info`, `list`

### group info
Display the information of a specific raft group.
```
$ ratis sh group info -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>]
```

### group list
Display the group information of a specific raft server
```
$ ratis sh group list -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>]  <[-serverAddress <P0_HOST:P0_PORT>]|[-peerId <peerId0>]>
```

## peer
The `peer` command manages ratis cluster peers.
It has the following subcommands:
`add`, `remove`, `setPriority`

### peer add
Add peers to a ratis group.
```
$ ratis sh peer add -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>] -address <P4_HOST:P4_PORT,...,PN_HOST:PN_PORT>
```

### peer remove
Remove peers to from a ratis group.
```
$ ratis sh peer remove -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>] -address <P0_HOST:P0_PORT,...>
```

### peer setPriority
Set priority to ratis peers.
The priority of ratis peer can affect the leader election, the server with the highest priority will eventually become the leader of the cluster.
```
$ ratis sh peer setPriority -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> [-groupid <RAFT_GROUP_ID>] -addressPriority <P0_HOST:P0_PORT|PRIORITY>
```
## snapshot
The `snapshot` command manages ratis snapshot.
It has the following subcommands:
`create`

### snapshot create
Trigger the specified server take snapshot.
```
$ ratis sh snapshot create -peers <P0_HOST:P0_PORT,P1_HOST:P1_PORT,P2_HOST:P2_PORT> -peerId <peerId0> [-groupid <RAFT_GROUP_ID>]
```

## local
The `local` command is used to process local operation, which no need to connect to ratis server.
It has the following subcommands:
`raftMetaConf`

### local raftMetaConf
Generate a new raft-meta.conf file based on original raft-meta.conf and new peers, which is used to move a raft node to a new node.
```
$ ratis sh local raftMetaConf -peers <[P0_ID|]P0_HOST:P0_PORT,[P1_ID|]P1_HOST:P1_PORT,[P2_ID|]P2_HOST:P2_PORT> -path <PARENT_PATH_OF_RAFT_META_CONF>
```
