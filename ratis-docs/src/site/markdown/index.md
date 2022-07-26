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

# Apache Ratis
Apache Ratis is a highly customizable Raft protocol implementation in Java.
[Raft](https://raft.github.io/) is an easily understandable consensus algorithm to manage replicated state. 

The Ratis project was started at 2016,
entered Apache incubation in 2017,
and graduated as a top level Apache project on Feb 17, 2021.
Originally, Ratis was built for using Raft in [Apache Ozone](https://ozone.apache.org)
in order to replicate raw data and to provide high availability.
The correctness and the performance of Ratis has been heavily tested with Ozone.

Unlike many other raft implementations, 
Ratis is designed to be pluggable, 
it could be used in any Java application where state should be replicated between multiple instances.

## Ratis Features

Since Ratis is designed to be pluggable in any Java applications, 
Ratis provides abstractions over Raft protocol for users, 
which make Raft library fully decoupled from your application. 
The key features of Raft are described below.

### Pluggable transport
Ratis provides a pluggable transport layer. 
By default gRPC, Netty+Protobuf and Apache Hadoop RPC based transports are provided.

### Pluggable state machine
Ratis supports a log and state machine. 
State machine typically contains the data that you want to make highly available. 
Ratis makes it easy to use your own state machine.

### Pluggable raft log
Raft log is also pluggable, 
users can provide their own log implementation. 
The default implementation stores log in local files.

Application can define the policies of how to write data and where it should be written easily.

### Log service

Ratis provides a log service recipe provides StateMachines to implement a distributed log service with a focused client API. 
For more information, please read the [LogService documentation](https://ratis.apache.org/logservice).