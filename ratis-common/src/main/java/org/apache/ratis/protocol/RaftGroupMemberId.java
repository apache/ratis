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

import java.util.Objects;

/**
 * A {@link RaftGroupMemberId} consists of a {@link RaftPeerId} and a {@link RaftGroupId}.
 *
 * This is a value-based class.
 */
public final class RaftGroupMemberId {
  public static RaftGroupMemberId valueOf(RaftPeerId peerId, RaftGroupId groupId) {
    return new RaftGroupMemberId(peerId, groupId);
  }

  private final RaftPeerId peerId;
  private final RaftGroupId groupId;
  private final String name;

  private RaftGroupMemberId(RaftPeerId peerId, RaftGroupId groupId) {
    this.peerId = Objects.requireNonNull(peerId, "peerId == null");
    this.groupId = Objects.requireNonNull(groupId, "groupId == null");
    this.name = peerId + "@" + groupId;
  }

  public RaftPeerId getPeerId() {
    return peerId;
  }

  public RaftGroupId getGroupId() {
    return groupId;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof RaftGroupMemberId)) {
      return false;
    }

    final RaftGroupMemberId that = (RaftGroupMemberId)obj;
    return this.peerId.equals(that.peerId) && this.groupId.equals(that.groupId);
  }

  @Override
  public int hashCode() {
    return peerId.hashCode() ^ groupId.hashCode();
  }
}
