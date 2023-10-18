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

import java.util.UUID;

/**
 * The id of RaftClient. Should be globally unique so that raft peers can use it
 * to correctly identify retry requests from the same client.
 */
public final class ClientId extends RaftId {
  private static final Factory<ClientId> FACTORY = new Factory<ClientId>() {
    @Override
    ClientId newInstance(UUID uuid, ByteString bytes) {
      return bytes == null? new ClientId(uuid): new ClientId(uuid, bytes);
    }
  };

  public static ClientId emptyClientId() {
    return FACTORY.emptyId();
  }

  public static ClientId randomId() {
    return FACTORY.randomId();
  }

  public static ClientId valueOf(ByteString bytes) {
    return FACTORY.valueOf(bytes);
  }

  public static ClientId valueOf(UUID uuid) {
    return FACTORY.valueOf(uuid);
  }

  private ClientId(UUID uuid, ByteString bytes) {
    super(uuid, bytes);
  }

  private ClientId(UUID uuid) {
    super(uuid);
  }

  @Override
  String createUuidString(UUID uuid) {
    return "client-" + super.createUuidString(uuid);
  }
}
