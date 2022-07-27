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
The correctness and the performance of Ratis have been heavily tested with Ozone.

## Pluggability

Unlike many other raft implementations,
Ratis is designed to be pluggable,
it could be used in any Java applications
where state should be replicated between multiple instances.
Ratis provides abstractions over Raft protocol for users,
which make Raft library fully decoupled from the applications.

### Pluggable transport
Ratis provides a pluggable transport layer. 
Applications may use their own implementation.
By default, gRPC, Netty+Protobuf and Apache Hadoop RPC based transports are provided.

### Pluggable state machine
Ratis supports a log and state machine. 
State machine typically contains the data that you want to make highly available.
Applications usually define its own state machine for the application logic.
Ratis makes it easy to use your own state machine.

### Pluggable raft log
Raft log is also pluggable, 
users can provide their own log implementation. 
The default implementation stores log in local files.
