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

package org.apache.ratis.server;

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.util.LifeCycle;

/**
 * Information of a {@link RaftServer.Division}.
 */
public interface DivisionInfo {
  RaftPeerRole getCurrentRole();

  default boolean isFollower() {
    return getCurrentRole() == RaftPeerRole.FOLLOWER;
  }

  default boolean isCandidate() {
    return getCurrentRole() == RaftPeerRole.CANDIDATE;
  }

  default boolean isLeader() {
    return getCurrentRole() == RaftPeerRole.LEADER;
  }

  boolean isLeaderReady();

  LifeCycle.State getLifeCycleState();

  default boolean isAlive() {
    return !getLifeCycleState().isClosingOrClosed();
  }
}
