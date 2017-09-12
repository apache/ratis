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
package org.apache.ratis.protocol;

import org.apache.ratis.util.Preconditions;

import java.util.*;

/**
 * Description of a raft group. It has a globally unique ID and a group of raft
 * peers.
 */
public class RaftGroup {
  /** UTF-8 string as id */
  private final RaftGroupId groupId;
  /** The group of raft peers */
  private final List<RaftPeer> peers;

  public RaftGroup(RaftGroupId groupId) {
    this(groupId, Collections.emptyList());
  }

  public RaftGroup(RaftGroupId groupId, RaftPeer[] peers) {
    this(groupId, Arrays.asList(peers));
  }

  public RaftGroup(RaftGroupId groupId, Collection<RaftPeer> peers) {
    Preconditions.assertTrue(peers != null);
    this.groupId = groupId;
    this.peers = Collections.unmodifiableList(new ArrayList<>(peers));
  }

  public RaftGroupId getGroupId() {
    return groupId;
  }

  public List<RaftPeer> getPeers() {
    return peers;
  }

  @Override
  public String toString() {
    return groupId + ":" + peers;
  }
}
