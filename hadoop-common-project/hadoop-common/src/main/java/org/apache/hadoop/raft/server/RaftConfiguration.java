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
import org.apache.hadoop.raft.server.protocol.RaftPeer;

import java.util.Collection;

public class RaftConfiguration {

  public enum State {
    STABLE,            // stable, no configuration change
    TRANSITIONAL,      // in the middle of a configuration change
    STAGING            // in the process of bootstrapping new peers
  }

  /**
   * The state of the raft ring.
   */
  private State state;
  /**
   * non-null while in TRANSITIONAL state
   */
  private SimpleConfiguration oldConf;
  /**
   * the new configuration while in TRANSITIONAL state,
   * or the current configuration in STABLE/STAGING state
   */
  private SimpleConfiguration conf;

  // TODO private Map<String, RaftPeer> stagingNewPeers;

  public RaftConfiguration(RaftPeer[] peers) {
    this.conf = new SimpleConfiguration(peers);
    this.state = State.STABLE;
  }

  public synchronized State getState() {
    return this.state;
  }

  public synchronized boolean containsInConf(String peerId) {
    return conf.contains(peerId);
  }

  public synchronized boolean containsInOldConf(String peerId) {
    return oldConf != null && oldConf.contains(peerId);
  }

  public synchronized boolean inStableState() {
    final boolean stable = state == State.STABLE;
    if (stable) {
      Preconditions.checkState(oldConf == null && conf != null);
    }
    return stable;
  }

  public synchronized void setNewConfiguration(RaftPeer[] newPeers) {
    Preconditions.checkArgument(newPeers != null && newPeers.length > 0);
    Preconditions.checkState(inStableState());
    oldConf = conf;
    conf = new SimpleConfiguration(newPeers);
    state = State.TRANSITIONAL;
  }

  public synchronized boolean inTransitionState() {
    final boolean transitional = state == State.TRANSITIONAL;
    if (transitional) {
      Preconditions.checkState(oldConf != null && conf != null);
    }
    return transitional;
  }

  public synchronized void commitNewConfiguration() {
    Preconditions.checkState(inTransitionState());
    oldConf = null;
    state = State.STABLE;
  }

  public synchronized RaftPeer getPeer(String id) {
    RaftPeer peer = conf.getPeer(id);
    if (peer != null) {
      return peer;
    } else if (oldConf != null) {
      return oldConf.getPeer(id);
    }
    return null;
  }

  public synchronized Collection<RaftPeer> getPeers() {
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

  public synchronized Collection<RaftPeer> getOtherPeers(String selfId) {
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

  public synchronized boolean hasMajorities(Collection<String> others,
      String selfId) {
    Preconditions.checkArgument(!others.contains(selfId));
    return conf.hasMajority(others, selfId) &&
        (oldConf == null || oldConf.hasMajority(others, selfId));
  }

  // TODO check if leader is in the new/old configuration
}
