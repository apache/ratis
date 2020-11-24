/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.protocol;

import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;

import java.util.Objects;
import java.util.Optional;

/**
 * The id of a client invocation.
 * A client invocation may be an RPC or a stream.
 *
 * This is a value-based class.
 */
public final class ClientInvocationId {
  public static ClientInvocationId valueOf(ClientId clientId, long invocationId) {
    return new ClientInvocationId(clientId, invocationId);
  }

  public static ClientInvocationId valueOf(RaftClientMessage message) {
    return valueOf(message.getClientId(), message.getCallId());
  }

  public static ClientInvocationId valueOf(StateMachineLogEntryProto proto) {
    return valueOf(ClientId.valueOf(proto.getClientId()), proto.getCallId());
  }

  private final ClientId clientId;
  /** It may be a call id or a stream id. */
  private final long longId;

  private ClientInvocationId(ClientId clientId, long longId) {
    this.clientId = clientId;
    this.longId = longId;
  }

  public ClientId getClientId() {
    return clientId;
  }

  public long getLongId() {
    return longId;
  }

  public boolean match(StateMachineLogEntryProto proto) {
    return longId == proto.getCallId() && Optional.ofNullable(clientId)
        .map(RaftId::toByteString)
        .filter(b -> b.equals(proto.getClientId()))
        .isPresent();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final ClientInvocationId that = (ClientInvocationId) obj;
    return this.longId == that.longId && Objects.equals(this.clientId, that.clientId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clientId, longId);
  }

  @Override
  public String toString() {
    return longId + "@" + clientId;
  }
}
