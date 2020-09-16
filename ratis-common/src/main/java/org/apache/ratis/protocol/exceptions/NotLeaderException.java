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
package org.apache.ratis.protocol.exceptions;

import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.Preconditions;

import java.util.Collection;
import java.util.Collections;

public class NotLeaderException extends RaftException {
  private final RaftPeer suggestedLeader;
  /** the client may need to update its RaftPeer list */
  private final Collection<RaftPeer> peers;

  public NotLeaderException(RaftGroupMemberId memberId, RaftPeer suggestedLeader, Collection<RaftPeer> peers) {
    super("Server " + memberId + " is not the leader" + (suggestedLeader != null? " " + suggestedLeader: ""));
    this.suggestedLeader = suggestedLeader;
    this.peers = peers != null? Collections.unmodifiableCollection(peers): Collections.emptyList();
    Preconditions.assertUnique(this.peers);
  }

  public RaftPeer getSuggestedLeader() {
    return suggestedLeader;
  }

  public Collection<RaftPeer> getPeers() {
    return peers;
  }
}
