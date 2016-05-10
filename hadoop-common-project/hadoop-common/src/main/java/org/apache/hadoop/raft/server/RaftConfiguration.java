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
package org.apache.hadoop.raft.server;

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.protocol.RaftPeer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RaftConfiguration {

  public enum State {
    STABLE,            // stable, no configuration change
    TRANSITIONAL,      // in the middle of a configuration change
    STAGING            // in the process of bootstrapping new peers
  }

  /**
   * The state of the raft ring.
   */
  private final State state;
  /**
   * non-null while in TRANSITIONAL state
   */
  private final SimpleConfiguration oldConf;
  /**
   * the new configuration while in TRANSITIONAL state,
   * or the current configuration in STABLE/STAGING state
   */
  private final SimpleConfiguration conf;

  // TODO private Map<String, RaftPeer> stagingNewPeers;

  public RaftConfiguration(RaftPeer[] peers) {
    Preconditions.checkArgument(peers != null && peers.length > 0);
    this.conf = new SimpleConfiguration(peers);
    this.oldConf = null;
    this.state = State.STABLE;
  }

  public RaftConfiguration(RaftPeer[] newPeers, RaftPeer[] oldPeers) {
    Preconditions.checkArgument(newPeers != null && newPeers.length > 0);
    Preconditions.checkArgument(oldPeers != null && oldPeers.length > 0);

    this.conf = new SimpleConfiguration(newPeers);
    this.oldConf = new SimpleConfiguration(oldPeers);
    this.state = State.TRANSITIONAL;
  }

  public State getState() {
    return this.state;
  }

  public boolean inTransitionState() {
    return this.state == State.TRANSITIONAL;
  }

  public boolean inStableState() {
    return this.state == State.STABLE;
  }

  public boolean containsInConf(String peerId) {
    return conf.contains(peerId);
  }

  public boolean containsInOldConf(String peerId) {
    return oldConf != null && oldConf.contains(peerId);
  }

  public RaftPeer getPeer(String id) {
    RaftPeer peer = conf.getPeer(id);
    if (peer != null) {
      return peer;
    } else if (oldConf != null) {
      return oldConf.getPeer(id);
    }
    return null;
  }

  public Collection<RaftPeer> getPeers() {
    Collection<RaftPeer> peers = conf.getPeers();
    if (oldConf != null) {
      for (RaftPeer p : oldConf.getPeers()) {
        if (!peers.contains(p)) {
          peers.add(p);
        }
      }
    }
    return peers;
  }

  public Collection<RaftPeer> getOtherPeers(String selfId) {
    Collection<RaftPeer> others = conf.getOtherPeers(selfId);
    if (oldConf != null) {
      Collection<RaftPeer> oldOthers = oldConf.getOtherPeers(selfId);
      for (RaftPeer p : oldOthers) {
        if (!others.contains(p)) {
          others.add(p);
        }
      }
    }
    return others;
  }

  public boolean hasMajorities(Collection<String> others,
      String selfId) {
    Preconditions.checkArgument(!others.contains(selfId));
    return conf.hasMajority(others, selfId) &&
        (oldConf == null || oldConf.hasMajority(others, selfId));
  }

  @Override
  public String toString() {
    return conf.toString() + ", old:"
        + (oldConf != null ? oldConf : "[]");
  }

  boolean hasNoChange(RaftPeer[] newMembers) {
    if (!inStableState() || conf.getSize() != newMembers.length) {
      return false;
    }
    for (RaftPeer peer : newMembers) {
      if (!conf.contains(peer.getId())) {
        return false;
      }
    }
    return true;
  }

  Collection<RaftPeer> getNewPeers(RaftPeer[] newMembers) {
    List<RaftPeer> peers = new ArrayList<>();
    for (RaftPeer p : newMembers) {
      if (!conf.contains(p.getId())) {
        peers.add(p);
      }
    }
    return peers;
  }

  // TODO check if leader is in the new/old configuration
}
