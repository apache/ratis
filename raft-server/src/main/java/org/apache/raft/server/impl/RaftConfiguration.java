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
package org.apache.raft.server.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.raft.protocol.RaftPeer;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The configuration of the raft cluster.
 *
 * The configuration is stable if there is no on-going peer change. Otherwise,
 * the configuration is transitional, i.e. in the middle of a peer change.
 *
 * The objects of this class are immutable.
 */
public class RaftConfiguration {
  /** Create a {@link Builder}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link RaftConfiguration} objects. */
  public static class Builder {
    private PeerConfiguration oldConf;
    private PeerConfiguration conf;
    private long logEntryIndex = RaftServerConstants.INVALID_LOG_INDEX;

    private boolean forceStable = false;
    private boolean forceTransitional = false;

    private Builder() {}

    public Builder setConf(PeerConfiguration conf) {
      Preconditions.checkNotNull(conf);
      Preconditions.checkState(this.conf == null, "conf is already set.");
      this.conf = conf;
      return this;
    }

    public Builder setConf(Iterable<RaftPeer> peers) {
      return setConf(new PeerConfiguration(peers));
    }

    public Builder setConf(RaftPeer[] peers) {
      return setConf(Arrays.asList(peers));
    }

    Builder setConf(RaftConfiguration transitionalConf) {
      Preconditions.checkNotNull(transitionalConf);
      Preconditions.checkState(transitionalConf.isTransitional());

      Preconditions.checkState(!forceTransitional);
      forceStable = true;
      return setConf(transitionalConf.conf);
    }


    public Builder setOldConf(PeerConfiguration oldConf) {
      Preconditions.checkNotNull(oldConf);
      Preconditions.checkState(this.oldConf == null, "oldConf is already set.");
      this.oldConf = oldConf;
      return this;
    }

    public Builder setOldConf(Iterable<RaftPeer> oldPeers) {
      return setOldConf(new PeerConfiguration(oldPeers));
    }

    public Builder setOldConf(RaftPeer[] oldPeers) {
      return setOldConf(Arrays.asList(oldPeers));
    }

    Builder setOldConf(RaftConfiguration stableConf) {
      Preconditions.checkNotNull(stableConf);
      Preconditions.checkState(stableConf.isStable());

      Preconditions.checkState(!forceStable);
      forceTransitional = true;
      return setOldConf(stableConf.conf);
    }

    public Builder setLogEntryIndex(long logEntryIndex) {
      Preconditions.checkArgument(
          logEntryIndex != RaftServerConstants.INVALID_LOG_INDEX);
      Preconditions.checkState(
          this.logEntryIndex == RaftServerConstants.INVALID_LOG_INDEX,
          "logEntryIndex is already set.");
      this.logEntryIndex = logEntryIndex;
      return this;
    }

    /** Build a {@link RaftConfiguration}. */
    public RaftConfiguration build() {
      if (forceTransitional) {
        Preconditions.checkState(oldConf != null);
      }
      if (forceStable) {
        Preconditions.checkState(oldConf == null);
      }
      return new RaftConfiguration(conf, oldConf, logEntryIndex);
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

  private RaftConfiguration(PeerConfiguration conf, PeerConfiguration oldConf,
      long logEntryIndex) {
    Preconditions.checkNotNull(conf);
    this.conf = conf;
    this.oldConf = oldConf;
    this.logEntryIndex = logEntryIndex;
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

  /**
   * @return the peer corresponding to the given id;
   *         or return null if the peer is not in this configuration.
   */
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

  /** @return all the peers from the conf, and the old conf if it exists. */
  public Collection<RaftPeer> getPeers() {
    final Collection<RaftPeer> peers = new ArrayList<>(conf.getPeers());
    if (oldConf != null) {
      oldConf.getPeers().stream().filter(p -> !peers.contains(p))
          .forEach(peers::add);
    }
    return peers;
  }

  /**
   * @return all the peers other than the given self id from the conf,
   *         and the old conf if it exists.
   */
  public Collection<RaftPeer> getOtherPeers(String selfId) {
    Collection<RaftPeer> others = conf.getOtherPeers(selfId);
    if (oldConf != null) {
      oldConf.getOtherPeers(selfId).stream()
          .filter(p -> !others.contains(p))
          .forEach(others::add);
    }
    return others;
  }

  /** @return true if the self id together with the others are in the majority. */
  public boolean hasMajority(Collection<String> others, String selfId) {
    Preconditions.checkArgument(!others.contains(selfId));
    return conf.hasMajority(others, selfId) &&
        (oldConf == null || oldConf.hasMajority(others, selfId));
  }

  @Override
  public String toString() {
    return conf + (oldConf != null ? "old:" + oldConf : "");
  }

  @VisibleForTesting
  boolean hasNoChange(RaftPeer[] newMembers) {
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
    return oldConf != null ? oldConf.getPeers() : Collections.emptyList();
  }

  public Collection<RaftPeer> getPeersInConf() {
    return conf.getPeers();
  }
}
