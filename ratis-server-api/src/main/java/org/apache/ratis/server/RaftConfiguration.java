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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.Collection;

/**
 * A configuration is a subset of the members in a {@link org.apache.ratis.protocol.RaftGroup}.
 * The configuration of a cluster may change from time to time.
 *
 * In a configuration,
 * - the peers are voting members such as LEADER, CANDIDATE and FOLLOWER;
 * - the listeners are non-voting members.
 *
 * This class captures the current configuration and the previous configuration of a cluster.
 *
 * The objects of this class are immutable.
 *
 * @see org.apache.ratis.proto.RaftProtos.RaftPeerRole
 */
public interface RaftConfiguration {
  /**
   * @return the peer corresponding to the given id;
   *         or return null if the peer is not in this configuration.
   */
  RaftPeer getPeer(RaftPeerId id, RaftPeerRole... roles);

  /** The same as getAllPeers(RaftPeerRole.FOLLOWER). */
  default Collection<RaftPeer> getAllPeers() {
    return getAllPeers(RaftPeerRole.FOLLOWER);
  }

  /** @return all the peers of the given role in the current configuration and the previous configuration. */
  Collection<RaftPeer> getAllPeers(RaftPeerRole role);

  /** The same as getCurrentPeers(RaftPeerRole.FOLLOWER). */
  default Collection<RaftPeer> getCurrentPeers() {
    return getCurrentPeers(RaftPeerRole.FOLLOWER);
  }

  /** @return all the peers of the given role in the current configuration. */
  Collection<RaftPeer> getCurrentPeers(RaftPeerRole roles);

  /** The same as getPreviousPeers(RaftPeerRole.FOLLOWER). */
  default Collection<RaftPeer> getPreviousPeers() {
    return getPreviousPeers(RaftPeerRole.FOLLOWER);
  }

  /** @return all the peers of the given role in the previous configuration. */
  Collection<RaftPeer> getPreviousPeers(RaftPeerRole roles);

  /** @return the index of the corresponding log entry for the current configuration. */
  long getLogEntryIndex();
}
