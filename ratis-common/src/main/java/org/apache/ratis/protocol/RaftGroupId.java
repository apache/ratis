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
 * The id of a raft group.
 * <p>
 * This is a value-based class.
 */
public final class RaftGroupId extends RaftId {
  private static final Factory<RaftGroupId> FACTORY = new Factory<RaftGroupId>() {
    @Override
    RaftGroupId newInstance(UUID uuid) {
      return new RaftGroupId(uuid);
    }
  };

  public static RaftGroupId emptyGroupId() {
    return FACTORY.emptyId();
  }

  public static RaftGroupId randomId() {
    return FACTORY.randomId();
  }

  public static RaftGroupId valueOf(UUID uuid) {
    return FACTORY.valueOf(uuid);
  }

  public static RaftGroupId valueOf(ByteString bytes) {
    return FACTORY.valueOf(bytes);
  }

  private RaftGroupId(UUID id) {
    super(id);
  }

  @Override
  String createUuidString(UUID uuid) {
    return "group-" + super.createUuidString(uuid);
  }
}
