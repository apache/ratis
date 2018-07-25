/**
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

package org.apache.ratis.server.impl;

import org.apache.ratis.shaded.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.util.Timestamp;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Maintain the Role of a Raft Peer.
 */
public class RoleInfo {

  private volatile RaftPeerRole role;
  private final AtomicReference<Timestamp> transitionTime;

  RoleInfo() {
    this.transitionTime = new AtomicReference<>(new Timestamp());
  }

  public void transitionRole(RaftPeerRole newRole) {
    this.role = newRole;
    this.transitionTime.set(new Timestamp());
  }

  public long getRoleElapsedTimeMs() {
    return transitionTime.get().elapsedTimeMs();
  }

  public RaftPeerRole getCurrentRole() {
    return role;
  }

  public boolean isFollower() {
    return role == RaftPeerRole.FOLLOWER;
  }

  public boolean isCandidate() {
    return role == RaftPeerRole.CANDIDATE;
  }

  public boolean isLeader() {
    return role == RaftPeerRole.LEADER;
  }
}
