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
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.RaftPeer;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class PeerChanges {
  private final List<RaftPeer> peersInNewConf;
  private final List<RaftPeer> addedPeers;
  private final List<RaftPeer> removedPeers;

  PeerChanges(List<RaftPeer> all, List<RaftPeer> addedPeers, List<RaftPeer> removed) {
    this.peersInNewConf = Collections.unmodifiableList(all);
    this.addedPeers = Collections.unmodifiableList(addedPeers);
    this.removedPeers = Collections.unmodifiableList(removed);
  }

  public List<RaftPeer> getPeersInNewConf() {
    return peersInNewConf;
  }

  public List<RaftPeer> getAddedPeers() {
    return addedPeers;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (!(obj instanceof PeerChanges)) {
      return false;
    }
    final PeerChanges that = (PeerChanges) obj;
    return Objects.equals(this.peersInNewConf, that.peersInNewConf)
        && Objects.equals(this.addedPeers, that.addedPeers)
        && Objects.equals(this.removedPeers, that.removedPeers);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(peersInNewConf);
  }

  @Override
  public String toString() {
    return "PeerChanges{"
        + "\n  newConf=" + peersInNewConf
        + "\n    added=" + addedPeers
        + "\n  removed=" + removedPeers
        + "\n}";
  }
}
