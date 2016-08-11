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
package org.apache.raft.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.raft.protocol.RaftPeer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RaftConfiguration {

  public enum State {
    STABLE,            // stable, no configuration change
    TRANSITIONAL,      // in the middle of a configuration change
  }

  public static RaftConfiguration composeOldNewConf(RaftPeer[] peers,
      RaftPeer[] old, long index) {
    Preconditions.checkArgument(peers != null && peers.length > 0);
    Preconditions.checkArgument(old != null && old.length > 0);
    return new RaftConfiguration(new SimpleConfiguration(peers),
        new SimpleConfiguration(old), index);
  }

  public static RaftConfiguration composeConf(RaftPeer[] peers, long index) {
    Preconditions.checkArgument(peers != null);
    Preconditions.checkArgument(peers.length > 0);
    return new RaftConfiguration(peers, index);
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

  /** the index of the corresponding log entry */
  private final long logEntryIndex;

  private RaftConfiguration(RaftPeer[] peers, long index) {
    Preconditions.checkArgument(peers != null && peers.length > 0);
    this.conf = new SimpleConfiguration(peers);
    this.oldConf = null;
    this.state = State.STABLE;
    this.logEntryIndex = index;
  }

  private RaftConfiguration(SimpleConfiguration newConf, SimpleConfiguration old,
      long index) {
    this.conf = newConf;
    this.oldConf = old;
    this.state = State.TRANSITIONAL;
    this.logEntryIndex = index;
  }

  public RaftConfiguration generateOldNewConf(SimpleConfiguration newConf,
      long index) {
    Preconditions.checkState(inStableState());
    return new RaftConfiguration(newConf, this.conf, index);
  }

  public RaftConfiguration generateNewConf(long index) {
    Preconditions.checkState(inTransitionState());
    RaftPeer[] peersInNewConf = conf.getPeers()
        .toArray(new RaftPeer[conf.getPeers().size()]);
    return new RaftConfiguration(peersInNewConf, index);
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

  public boolean contains(String peerId) {
    return conf.contains(peerId) && (oldConf == null || containsInOldConf(peerId));
  }

  public RaftPeer getPeer(String id) {
    if (id == null) {
      return null;
    }
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
      oldConf.getPeers().stream().filter(p -> !peers.contains(p))
          .forEach(peers::add);
    }
    return peers;
  }

  public Collection<RaftPeer> getOtherPeers(String selfId) {
    Collection<RaftPeer> others = conf.getOtherPeers(selfId);
    if (oldConf != null) {
      Collection<RaftPeer> oldOthers = oldConf.getOtherPeers(selfId);
      oldOthers.stream().filter(p -> !others.contains(p)).forEach(others::add);
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
    return "{" + conf.toString() + ", old:"
        + (oldConf != null ? oldConf : "[]") + "}";
  }

  @VisibleForTesting
  public boolean hasNoChange(RaftPeer[] newMembers) {
    if (!inStableState() || conf.size() != newMembers.length) {
      return false;
    }
    for (RaftPeer peer : newMembers) {
      if (!conf.contains(peer.getId())) {
        return false;
      }
    }
    return true;
  }

  long getLogEntryIndex() {
    return logEntryIndex;
  }

  static Collection<RaftPeer> computeNewPeers(RaftPeer[] newMembers,
      RaftConfiguration old) {
    List<RaftPeer> peers = new ArrayList<>();
    for (RaftPeer p : newMembers) {
      if (!old.containsInConf(p.getId())) {
        peers.add(p);
      }
    }
    return peers;
  }

  RaftPeer getRandomPeer(String exclusiveId) {
    Collection<RaftPeer> peers = conf.getOtherPeers(exclusiveId);
    if (peers.isEmpty()) {
      return null;
    }
    int index = RaftConstants.RANDOM.nextInt(peers.size());
    return peers.toArray(new RaftPeer[peers.size()])[index];
  }

  public Collection<RaftPeer> getPeersInOldConf() {
    return oldConf != null ? oldConf.getPeers() : new ArrayList<>(0);
  }

  public Collection<RaftPeer> getPeersInConf() {
    return conf.getPeers();
  }
}
