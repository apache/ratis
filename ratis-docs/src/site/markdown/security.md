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

# Security

## Trust Model

Raft is a consensus algorithm designed for Crash Fault Tolerance (CFT).
It assumes that

- the servers in a group can trust each other, and
- the servers and the clients can trust each other.

Raft is not designed for Byzantine Fault Tolerance (BFT),
which assumes that some of the servers may be malicious.
A malicious member of a group can corrupt the replicated log and the state machine.
Correctness holds only when all the servers in a group are honest.
Malicious clients are not supported either.
A malicious client can, for example, impersonate other clients
or submit requests that corrupt the state machine.

Apache Ratis is a Raft library with a pluggable transport.
Ratis itself does not authenticate or authorize requests:

- The `ClientId` and the `RaftPeerId` in a request are declared by the sender; Ratis does not verify them.
- Anyone who can reach a server port can send requests to it,
  including client requests, admin requests (such as group and membership changes)
  and server requests (such as log appends and votes).
- The identity established by TLS is not passed to the `StateMachine`.
  If an application needs per-user authorization,
  it must carry and verify the credentials in its own messages.

## Application Responsibilities

Applications using Ratis usually run a transport over a network.
In such case, the applications must do one of the following:

- provide secure communications between the servers and the clients, for example, by enabling TLS,
- provide a safe network environment, for example, by running the servers in a private network, or
- use a proprietary transport which provides security by other means.

Applications must not accept requests from any untrusted servers or clients.
When using TLS, it is recommended to

- enable mutual TLS, so that the servers authenticate the clients and the other servers, and
- use a trust store dedicated to the Ratis group,
  since every holder of a certificate trusted by the trust store is accepted.

## TLS Configuration

The TLS configurations are set in a `Parameters` object, not in `RaftProperties`.
Applications pass the `Parameters` to `RaftServer.Builder.setParameters(..)`
and `RaftClient.Builder.setParameters(..)` for establishing secure connections.
When a TLS configuration is not set, the corresponding connection is neither authenticated nor encrypted.

| **Parameter**                           | **Setter**                                     | **Description**                                          |
|:----------------------------------------|:-----------------------------------------------|:---------------------------------------------------------|
| `raft.grpc.tls.conf`                    | `GrpcConfigKeys.TLS.setConf`                   | gRPC default TLS conf, used when a conf below is not set |
| `raft.grpc.server.tls.conf`             | `GrpcConfigKeys.Server.setTlsConf`             | gRPC server TLS conf for server-to-server connections    |
| `raft.grpc.client.tls.conf`             | `GrpcConfigKeys.Client.setTlsConf`             | gRPC client TLS conf for client-to-server connections    |
| `raft.grpc.admin.tls.conf`              | `GrpcConfigKeys.Admin.setTlsConf`              | gRPC admin TLS conf for admin connections                |
| `raft.netty.dataStream.server.tls.conf` | `NettyConfigKeys.DataStream.Server.setTlsConf` | Netty data stream server TLS conf                        |
| `raft.netty.dataStream.client.tls.conf` | `NettyConfigKeys.DataStream.Client.setTlsConf` | Netty data stream client TLS conf                        |

Notes:

- The gRPC confs are `GrpcTlsConfig` objects.
  The data stream confs are `TlsConf` objects; a `GrpcTlsConfig` can also be used since it is a `TlsConf`.
- The data stream confs do not fall back to the gRPC confs.
  When data stream is enabled, its confs must be set separately.
- The Netty RPC transport (`raft.netty.server`) currently does not support TLS;
  see [RATIS-2801](https://issues.apache.org/jira/browse/RATIS-2801).

For example, a server using gRPC with mutual TLS can be built as below.

```java
final GrpcTlsConfig tlsConf = new GrpcTlsConfig(
    privateKeyFile, certChainFile, trustStoreFile, true); // true: enable mutual TLS
final Parameters parameters = new Parameters();
GrpcConfigKeys.TLS.setConf(parameters, tlsConf);

final RaftServer server = RaftServer.newBuilder()
    .setServerId(id)
    .setGroup(group)
    .setStateMachine(stateMachine)
    .setProperties(properties)
    .setParameters(parameters)
    .build();
```
