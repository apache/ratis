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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The configuration of the raft cluster.
 *
 * The configuration is stable if there is no on-going peer change. Otherwise,
 * the configuration is transitional, i.e. in the middle of a peer change.
 */
public class RaftConfiguration {
  public static RaftConfiguration composeOldNewConf(RaftPeer[] peers,
      RaftPeer[] old, long index) {
    Preconditions.checkArgument(peers != null && peers.length > 0);
    Preconditions.checkArgument(old != null && old.length > 0);
    return new RaftConfiguration(
        new PeerConfiguration(Arrays.asList(peers)),
        new PeerConfiguration(Arrays.asList(old)), index);
  }

  /** Non-null only if this configuration is transitional. */
  private final PeerConfiguration oldConf;
  /**
   * The current peer configuration while this configuration is stable;
   * or the new peer configuration while this configuration is transitional.
   */
  private final PeerConfiguration conf;

  /** The index of the corresponding log entry for this configuration. */
  private final long logEntryIndex;

  public RaftConfiguration(Iterable<RaftPeer> peers) {
    this(peers, RaftServerConstants.INVALID_LOG_INDEX);
  }

  public RaftConfiguration(RaftPeer[] peers, long index) {
    this(Arrays.asList(peers), index);
  }

  public RaftConfiguration(Iterable<RaftPeer> peers, long index) {
    this.conf = new PeerConfiguration(peers);
    this.oldConf = null;
    this.logEntryIndex = index;
  }

  private RaftConfiguration(PeerConfiguration newConf, PeerConfiguration old,
                            long index) {
    this.conf = newConf;
    this.oldConf = old;
    this.logEntryIndex = index;
  }

  public RaftConfiguration generateOldNewConf(PeerConfiguration newConf,
      long index) {
    Preconditions.checkState(isStable());
    return new RaftConfiguration(newConf, this.conf, index);
  }

  public RaftConfiguration generateNewConf(long index) {
    Preconditions.checkState(isTransitional());
    return new RaftConfiguration(conf.getPeers(), index);
  }

  /** Is this configuration transitional, i.e. in the middle of a peer change? */
  public boolean isTransitional() {
    return oldConf != null;
  }

  /** Is this configuration stable, i.e. no on-going peer change? */
  public boolean isStable() {
    return oldConf == null;
  }

  boolean containsInConf(String peerId) {
    return conf.contains(peerId);
  }

  boolean containsInOldConf(String peerId) {
    return oldConf != null && oldConf.contains(peerId);
  }

  public boolean contains(String peerId) {
    return containsInConf(peerId) && (oldConf == null || containsInOldConf(peerId));
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
    final Collection<RaftPeer> peers = new ArrayList<>(conf.getPeers());
    if (oldConf != null) {
      oldConf.getPeers().stream().filter(p -> !peers.contains(p))
          .forEach(peers::add);
    }
    return peers;
  }

  public Collection<RaftPeer> getOtherPeers(String selfId) {
    Collection<RaftPeer> others = conf.getOtherPeers(selfId);
    if (oldConf != null) {
      oldConf.getOtherPeers(selfId).stream()
          .filter(p -> !others.contains(p))
          .forEach(others::add);
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
    return conf + (oldConf != null ? "old:" + oldConf : "");
  }

  @VisibleForTesting
  public boolean hasNoChange(RaftPeer[] newMembers) {
    if (!isStable() || conf.size() != newMembers.length) {
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
    final List<RaftPeer> peers = conf.getOtherPeers(exclusiveId);
    if (peers.isEmpty()) {
      return null;
    }
    final int index = ThreadLocalRandom.current().nextInt(peers.size());
    return peers.get(index);
  }

  public Collection<RaftPeer> getPeersInOldConf() {
    return oldConf != null ? oldConf.getPeers() : new ArrayList<>(0);
  }

  public Collection<RaftPeer> getPeersInConf() {
    return conf.getPeers();
  }
}
