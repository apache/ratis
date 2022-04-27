---
title: Ratis-shell
menu: main
weight: -10
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

# Ratis-shell 
Ratis's commad line interface provides operations to manage the ratis server. You can invoke the following command line 
utility to get all the subcommands:

```shell
$ cd $PATH_TO_RATIS/ratis-assembly/target/apache-ratis-$version-SNAPSHOT
$ ./bin/ratis sh
Usage: ratis sh [generic options]
         [election [transfer] [stepDown] [pause] [resume]]         
         [group [info] [list]]                                     
         [peer [add] [remove] [setPriority]]                       
         [snapshot [create]]  
```

## Operations:

### election

The `election` command manage ratis leader election, it has four subcommands：`transfer`、`stepDown`、`pause`、`resume`

```
# Switch the ratis leader to the specified ratis server(its address is <HOSTNAME:PORT>).
$ ./bin/ratis sh election transfer -peers <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT> -address <HOSTNAME:PORT> [-groupid <RAFT_GROUP_ID>]

# Make the leader node of the current Ratis cluster lose their leadership
$ ./bin/ratis sh election stepDown -peers <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT> [-groupid <RAFT_GROUP_ID>]

# Forbid the specified ratis server(its address is <HOSTNAME:PORT>) Leader election
$ ./bin/ratis sh election pause -peers <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT> -address <HOSTNAME:PORT> [-groupid <RAFT_GROUP_ID>] 

# Resume the specified ratis server Leader election
$ ./bin/ratis sh election resume -peers <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT> -address <HOSTNAME:PORT> [-groupid <RAFT_GROUP_ID>] 
```

### group

The `group` command manage ratis group, it has two subcommands：`info`、`list`

```
# Display the information of a specific ratis group
$ ./bin/ratis sh group info transfer -peers <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT> [-groupid <RAFT_GROUP_ID>]

# Display the group information of a specific ratis server(its address is <PEER0_HOST:PEER0_PORT>)
$ ./bin/ratis sh group list -peers <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT> [-groupid <RAFT_GROUP_ID>] <[-serverAddress <PEER0_HOST:PEER0_PORT>]|[-peerId <peerId>]>
```

### peer

The `peer` command manage ratis peer, it has three subcommands：`add`、`remove`、`setPriority`

```
# Add peer to a ratis group
$ ./bin/ratis sh peer add -peers <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT> [-groupid <RAFT_GROUP_ID>] -address <PEER_HOST:PEER_PORT>

# Remove peers from a ratis group
$ ./bin/ratis sh group list -peers <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT> [-groupid <RAFT_GROUP_ID>] <[-serverAddress <PEER0_HOST:PEER0_PORT>]|[-peerId <peerId>]>

# Set the specific ratis server's, the priority will affect the strategy of ratis group leader election
$ ./bin/ratis sh setPriority -peers <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT> [-groupid <RAFT_GROUP_ID>] -addressPriority <PEER_HOST:PEER_PORT|PRIORITY>

```
### snapshot

The `snapshot` command manage ratis snapshot, it has one subcommands：`create`

```
# Make specific ratis server generate snpshot file
$ ./bin/ratis sh snapshit create -peers <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT> [-groupid <RAFT_GROUP_ID>] -peerId <peerId>
```