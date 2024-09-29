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

import org.apache.ratis.proto.RaftProtos.CandidateInfoProto;
import org.apache.ratis.proto.RaftProtos.FollowerInfoProto;
import org.apache.ratis.proto.RaftProtos.LeaderInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ratis.server.impl.ServerProtoUtils.toServerRpcProto;

/**
 * Maintain the Role of a Raft Peer.
 */
class RoleInfo {
  public static final Logger LOG = LoggerFactory.getLogger(RoleInfo.class);

  private final RaftPeerId id;
  private final AtomicReference<RaftPeerRole> role = new AtomicReference<>();
  /** Used when the peer is leader */
  private final AtomicReference<LeaderStateImpl> leaderState = new AtomicReference<>();
  /** Used when the peer is follower, to monitor election timeout */
  private final AtomicReference<FollowerState> followerState = new AtomicReference<>();
  /** Used when the peer is candidate, to request votes from other peers */
  private final AtomicReference<LeaderElection> leaderElection = new AtomicReference<>();
  private final AtomicBoolean pauseLeaderElection = new AtomicBoolean(false);

  private final AtomicReference<Timestamp> transitionTime;

  RoleInfo(RaftPeerId id) {
    this.id = id;
    this.transitionTime = new AtomicReference<>(Timestamp.currentTime());
  }

  void transitionRole(RaftPeerRole newRole) {
    this.role.set(newRole);
    this.transitionTime.set(Timestamp.currentTime());
  }

  long getRoleElapsedTimeMs() {
    return transitionTime.get().elapsedTimeMs();
  }

  RaftPeerRole getCurrentRole() {
    return role.get();
  }

  boolean isLeaderReady() {
    return getLeaderState().map(LeaderStateImpl::isReady).orElse(false);
  }

  Optional<LeaderStateImpl> getLeaderState() {
    return Optional.ofNullable(leaderState.get());
  }

  LeaderStateImpl getLeaderStateNonNull() {
    return Objects.requireNonNull(leaderState.get(), "leaderState is null");
  }

  LeaderStateImpl updateLeaderState(RaftServerImpl server) {
    return updateAndGet(leaderState, new LeaderStateImpl(server));
  }

  CompletableFuture<Void> shutdownLeaderState(boolean allowNull) {
    final LeaderStateImpl leader = leaderState.getAndSet(null);
    if (leader == null) {
      if (!allowNull) {
        return JavaUtils.completeExceptionally(new NullPointerException("leaderState == null"));
      }
      return CompletableFuture.completedFuture(null);
    } else {
      LOG.info("{}: shutdown {}", id, leader);
      return leader.stop();
    }
  }

  Optional<FollowerState> getFollowerState() {
    return Optional.ofNullable(followerState.get());
  }

  void startFollowerState(RaftServerImpl server, Object reason) {
    updateAndGet(followerState, new FollowerState(server, reason)).start();
  }

  CompletableFuture<Void> shutdownFollowerState() {
    final FollowerState follower = followerState.getAndSet(null);
    if (follower == null) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info("{}: shutdown {}", id, follower);
    return follower.stopRunning();
  }

  void startLeaderElection(RaftServerImpl server, boolean force) {
    if (pauseLeaderElection.get()) {
      return;
    }
    updateAndGet(leaderElection, new LeaderElection(server, force)).start();
  }

  void setLeaderElectionPause(boolean pause) {
    pauseLeaderElection.set(pause);
  }

  CompletableFuture<Void> shutdownLeaderElection() {
    final LeaderElection election = leaderElection.getAndSet(null);
    if (election == null) {
      return CompletableFuture.completedFuture(null);
    }
    LOG.info("{}: shutdown {}", id, election);
    return election.shutdown();
  }

  private <T> T updateAndGet(AtomicReference<T> ref, T current) {
    final T updated = ref.updateAndGet(previous -> previous != null? previous: current);
    Preconditions.assertTrue(updated == current, "previous != null");
    LOG.info("{}: start {}", id, current);
    return updated;
  }

  RoleInfoProto buildRoleInfoProto(RaftServerImpl server) {
    final RaftPeerRole currentRole = getCurrentRole();
    final RoleInfoProto.Builder proto = RoleInfoProto.newBuilder()
        .setSelf(server.getPeer().getRaftPeerProto())
        .setRole(currentRole)
        .setRoleElapsedTimeMs(getRoleElapsedTimeMs());

    switch (currentRole) {
      case LEADER:
        getLeaderState().ifPresent(leader -> {
          final LeaderInfoProto.Builder b = LeaderInfoProto.newBuilder()
              .setTerm(leader.getCurrentTerm());
          leader.getLogAppenders()
              .map(LogAppender::getFollower)
              .map(f -> toServerRpcProto(f.getPeer(), f.getLastRpcResponseTime().elapsedTimeMs()))
              .forEach(b::addFollowerInfo);
          proto.setLeaderInfo(b);
        });
        return proto.build();

      case CANDIDATE:
        return proto.setCandidateInfo(CandidateInfoProto.newBuilder()
            .setLastLeaderElapsedTimeMs(server.getState().getLastLeaderElapsedTimeMs()))
            .build();

      case LISTENER:
      case FOLLOWER:
        // FollowerState can be null while adding a new peer as it is not a voting member yet
        final FollowerState follower = getFollowerState().orElse(null);
        final long rpcElapsed;
        final int outstandingOp;
        if (follower != null) {
          rpcElapsed = follower.getLastRpcTime().elapsedTimeMs();
          outstandingOp = follower.getOutstandingOp();
        } else {
          rpcElapsed = 0;
          outstandingOp = 0;
        }
        final RaftPeer leader = server.getRaftConf().getPeer(server.getState().getLeaderId());
        return proto.setFollowerInfo(FollowerInfoProto.newBuilder()
            .setLeaderInfo(toServerRpcProto(leader, rpcElapsed))
            .setOutstandingOp(outstandingOp))
            .build();

      default:
        throw new IllegalStateException("Unexpected role " + currentRole);
    }
  }

  @Override
  public String toString() {
    return String.format("%9s", role);
  }
}
