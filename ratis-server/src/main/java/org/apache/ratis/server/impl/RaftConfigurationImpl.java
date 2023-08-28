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

import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * The configuration of the raft cluster.
 * <p>
 * The configuration is stable if there is no on-going peer change. Otherwise,
 * the configuration is transitional, i.e. in the middle of a peer change.
 * <p>
 * The objects of this class are immutable.
 */
final class RaftConfigurationImpl implements RaftConfiguration {
  /** Create a {@link Builder}. */
  static Builder newBuilder() {
    return new Builder();
  }

  static final class Builder {
    private PeerConfiguration oldConf;
    private PeerConfiguration conf;
    private long logEntryIndex = RaftLog.INVALID_LOG_INDEX;

    private boolean forceStable = false;
    private boolean forceTransitional = false;

    private Builder() {}

    Builder setConf(PeerConfiguration conf) {
      Objects.requireNonNull(conf);
      Preconditions.assertTrue(this.conf == null, "conf is already set.");
      this.conf = conf;
      return this;
    }

    Builder setConf(Iterable<RaftPeer> peers) {
      return setConf(new PeerConfiguration(peers));
    }

    Builder setConf(Iterable<RaftPeer> peers, Iterable<RaftPeer> listeners) {
      return setConf(new PeerConfiguration(peers, listeners));
    }

    Builder setConf(RaftConfigurationImpl transitionalConf) {
      Objects.requireNonNull(transitionalConf);
      Preconditions.assertTrue(transitionalConf.isTransitional());

      Preconditions.assertTrue(!forceTransitional);
      forceStable = true;
      return setConf(transitionalConf.conf);
    }


    Builder setOldConf(PeerConfiguration oldConf) {
      Objects.requireNonNull(oldConf);
      Preconditions.assertTrue(this.oldConf == null, "oldConf is already set.");
      this.oldConf = oldConf;
      return this;
    }

    Builder setOldConf(Iterable<RaftPeer> oldPeers, Iterable<RaftPeer> oldListeners) {
      return setOldConf(new PeerConfiguration(oldPeers, oldListeners));
    }

    Builder setOldConf(RaftConfigurationImpl stableConf) {
      Objects.requireNonNull(stableConf);
      Preconditions.assertTrue(stableConf.isStable());

      Preconditions.assertTrue(!forceStable);
      forceTransitional = true;
      return setOldConf(stableConf.conf);
    }

    Builder setLogEntryIndex(long logEntryIndex) {
      Preconditions.assertTrue(logEntryIndex != RaftLog.INVALID_LOG_INDEX);
      Preconditions.assertTrue(this.logEntryIndex == RaftLog.INVALID_LOG_INDEX, "logEntryIndex is already set.");
      this.logEntryIndex = logEntryIndex;
      return this;
    }

    RaftConfigurationImpl build() {
      if (forceTransitional) {
        Preconditions.assertTrue(oldConf != null);
      }
      if (forceStable) {
        Preconditions.assertTrue(oldConf == null);
      }
      return new RaftConfigurationImpl(conf, oldConf, logEntryIndex);
    }
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

  private RaftConfigurationImpl(PeerConfiguration conf, PeerConfiguration oldConf,
      long logEntryIndex) {
    this.conf = Objects.requireNonNull(conf);
    this.oldConf = oldConf;
    this.logEntryIndex = logEntryIndex;
  }

  /** Is this configuration transitional, i.e. in the middle of a peer change? */
  boolean isTransitional() {
    return oldConf != null;
  }

  /** Is this configuration stable, i.e. no on-going peer change? */
  boolean isStable() {
    return oldConf == null;
  }

  boolean containsInConf(RaftPeerId peerId, RaftPeerRole... roles) {
    if (roles == null || roles.length == 0) {
      return conf.contains(peerId);
    } else if (roles.length == 1) {
      return conf.contains(peerId, roles[0]);
    } else {
      return conf.contains(peerId, EnumSet.of(roles[0], roles)) != null;
    }
  }

  PeerConfiguration getConf() {
    return conf;
  }

  PeerConfiguration getOldConf() {
    return oldConf;
  }

  boolean isHighestPriority(RaftPeerId peerId) {
    RaftPeer target = getPeer(peerId);
    if (target == null) {
      return false;
    }
    Collection<RaftPeer> peers = getCurrentPeers();
    for (RaftPeer peer : peers) {
      if (peer.getPriority() > target.getPriority() && !peer.equals(target)) {
        return false;
      }
    }
    return true;
  }

  boolean containsInOldConf(RaftPeerId peerId) {
    return oldConf != null && oldConf.contains(peerId);
  }

  /**
   * @return true iff the given peer is contained in conf and,
   *         if old conf exists, is contained in old conf.
   */
  boolean containsInBothConfs(RaftPeerId peerId) {
    return containsInConf(peerId) &&
        (oldConf == null || containsInOldConf(peerId));
  }

  @Override
  public RaftPeer getPeer(RaftPeerId id, RaftPeerRole... roles) {
    if (id == null) {
      return null;
    }
    final RaftPeer peer = conf.getPeer(id, roles);
    if (peer != null) {
      return peer;
    } else if (oldConf != null) {
      return oldConf.getPeer(id, roles);
    }
    return null;
  }

  @Override
  public List<RaftPeer> getAllPeers(RaftPeerRole role) {
    final List<RaftPeer> peers = new ArrayList<>(conf.getPeers(role));
    if (oldConf != null) {
      oldConf.getPeers(role).stream()
          .filter(p -> !peers.contains(p))
          .forEach(peers::add);
    }
    return peers;
  }

  /**
   * @return all the peers other than the given self id from the conf,
   *         and the old conf if it exists.
   */
  Collection<RaftPeer> getOtherPeers(RaftPeerId selfId) {
    Collection<RaftPeer> others = conf.getOtherPeers(selfId);
    if (oldConf != null) {
      oldConf.getOtherPeers(selfId).stream()
          .filter(p -> !others.contains(p))
          .forEach(others::add);
    }
    return others;
  }

  /** @return true if the self id together with the others are in the majority. */
  boolean hasMajority(Collection<RaftPeerId> others, RaftPeerId selfId) {
    Preconditions.assertTrue(!others.contains(selfId));
    return conf.hasMajority(others, selfId) &&
        (oldConf == null || oldConf.hasMajority(others, selfId));
  }

  /** @return true if the self id together with the acknowledged followers reach majority. */
  boolean hasMajority(Predicate<RaftPeerId> followers, RaftPeerId selfId) {
    final boolean includeInCurrent = containsInConf(selfId);
    final boolean hasMajorityInNewConf = conf.hasMajority(followers, includeInCurrent);

    if (!isTransitional()) {
      return hasMajorityInNewConf;
    } else {
      final boolean includeInOldConf = containsInOldConf(selfId);
      final boolean hasMajorityInOldConf = oldConf.hasMajority(followers, includeInOldConf);
      return hasMajorityInOldConf && hasMajorityInNewConf;
    }
  }

  int getMajorityCount() {
    return conf.getMajorityCount();
  }

  /** @return true if the rejects are in the majority(maybe half is enough in some cases). */
  boolean majorityRejectVotes(Collection<RaftPeerId> rejects) {
    return conf.majorityRejectVotes(rejects) ||
            (oldConf != null && oldConf.majorityRejectVotes(rejects));
  }

  /** @return true if only one voting member (the leader) in the cluster */
  boolean isSingleton() {
    return getCurrentPeers().size() == 1 && getPreviousPeers().size() <= 1;
  }

  @Override
  public String toString() {
    return logEntryIndex + ": " + conf + ", old=" + oldConf;
  }

  boolean hasNoChange(Collection<RaftPeer> newMembers, Collection<RaftPeer> newListeners) {
    if (!isStable() || conf.size() != newMembers.size()
        || conf.getPeers(RaftPeerRole.LISTENER).size() != newListeners.size()) {
      return false;
    }
    for (RaftPeer peer : newMembers) {
      final RaftPeer inConf = conf.getPeer(peer.getId());
      if (inConf == null) {
        return false;
      }
      if (inConf.getPriority() != peer.getPriority()) {
        return false;
      }
    }
    for (RaftPeer peer : newListeners) {
      final RaftPeer inConf = conf.getPeer(peer.getId(), RaftPeerRole.LISTENER);
      if (inConf == null) {
        return false;
      }
      if (inConf.getPriority() != peer.getPriority()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long getLogEntryIndex() {
    return logEntryIndex;
  }

  /** @return the peers which are not contained in conf. */
  Collection<RaftPeer> filterNotContainedInConf(List<RaftPeer> peers) {
    return peers.stream()
        .filter(p -> !containsInConf(p.getId(), RaftPeerRole.FOLLOWER, RaftPeerRole.LISTENER))
        .collect(Collectors.toList());
  }

  @Override
  public Collection<RaftPeer> getPreviousPeers(RaftPeerRole role) {
    return oldConf != null ? oldConf.getPeers(role) : Collections.emptyList();
  }

  @Override
  public Collection<RaftPeer> getCurrentPeers(RaftPeerRole role) {
    return conf.getPeers(role);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    final RaftConfigurationImpl that = (RaftConfigurationImpl)obj;
    return this.logEntryIndex == that.logEntryIndex
        && Objects.equals(this.conf,  that.conf)
        && Objects.equals(this.oldConf,  that.oldConf);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(logEntryIndex);
  }
}
