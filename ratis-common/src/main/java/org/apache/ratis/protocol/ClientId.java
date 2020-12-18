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

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.Optional;
import java.util.UUID;

/**
 * Id of Raft client. Should be globally unique so that raft peers can use it
 * to correctly identify retry requests from the same client.
 */
public final class ClientId extends RaftId {
  private static final ClientId EMPTY_CLIENT_ID = new ClientId(ZERO_UUID_BYTESTRING);

  public static ClientId emptyClientId() {
    return EMPTY_CLIENT_ID;
  }

  public static ClientId randomId() {
    return new ClientId(UUID.randomUUID());
  }

  public static ClientId valueOf(ByteString data) {
    return Optional.ofNullable(data).filter(d -> !d.isEmpty()).map(ClientId::new).orElse(EMPTY_CLIENT_ID);
  }

  public static ClientId valueOf(UUID uuid) {
    return new ClientId(uuid);
  }

  private ClientId(ByteString data) {
    super(data);
  }

  private ClientId(UUID uuid) {
    super(uuid);
  }

  @Override
  String createUuidString(UUID uuid) {
    return "client-" + super.createUuidString(uuid);
  }
}
