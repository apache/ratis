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

import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;

/**
 * Used when the peer is a follower. Used to track the election timeout.
 */
class FollowerState extends Daemon {
  enum UpdateType {
    APPEND_START(AtomicInteger::incrementAndGet),
    APPEND_COMPLETE(AtomicInteger::decrementAndGet),
    INSTALL_SNAPSHOT_START(AtomicInteger::incrementAndGet),
    INSTALL_SNAPSHOT_COMPLETE(AtomicInteger::decrementAndGet),
    INSTALL_SNAPSHOT_NOTIFICATION(AtomicInteger::get),
    REQUEST_VOTE(AtomicInteger::get);

    private final ToIntFunction<AtomicInteger> updateFunction;

    UpdateType(ToIntFunction<AtomicInteger> updateFunction) {
      this.updateFunction = updateFunction;
    }

    int update(AtomicInteger outstanding) {
      return updateFunction.applyAsInt(outstanding);
    }
  }

  static final Logger LOG = LoggerFactory.getLogger(FollowerState.class);

  private final String name;
  private final RaftServerImpl server;

  private volatile Timestamp lastRpcTime = Timestamp.currentTime();
  private volatile boolean isRunning = true;
  private final AtomicInteger outstandingOp = new AtomicInteger();
  private final RaftServerMetrics raftServerMetrics;

  FollowerState(RaftServerImpl server) {
    this.name = server.getMemberId() + "-" + getClass().getSimpleName();
    this.server = server;
    raftServerMetrics = RaftServerMetrics.getRaftServerMetrics(server);
    raftServerMetrics.addPeerCommitIndexGauge(server.getPeer());
  }

  void updateLastRpcTime(UpdateType type) {
    lastRpcTime = Timestamp.currentTime();

    final int n = type.update(outstandingOp);
    if (LOG.isTraceEnabled()) {
      LOG.trace("{}: update lastRpcTime to {} for {}, outstandingOp={}", this, lastRpcTime, type, n);
    }
  }

  Timestamp getLastRpcTime() {
    return lastRpcTime;
  }

  int getOutstandingOp() {
    return outstandingOp.get();
  }

  boolean shouldWithholdVotes() {
    return lastRpcTime.elapsedTimeMs() < server.getMinTimeoutMs();
  }

  void stopRunning() {
    this.isRunning = false;
  }

  @Override
  public  void run() {
    long sleepDeviationThresholdMs = server.getSleepDeviationThresholdMs();
    while (isRunning && server.isFollower()) {
      final long electionTimeout = server.getRandomTimeoutMs();
      try {
        if (!JavaUtils.sleep(electionTimeout, sleepDeviationThresholdMs)) {
          continue;
        }

        final boolean isFollower = server.isFollower();
        if (!isRunning || !isFollower) {
          LOG.info("{}: Stopping now (isRunning? {}, isFollower? {})", this, isRunning, isFollower);
          break;
        }
        synchronized (server) {
          if (outstandingOp.get() == 0 && lastRpcTime.elapsedTimeMs() >= electionTimeout) {
            LOG.info("{}: change to CANDIDATE, lastRpcTime:{}ms, electionTimeout:{}ms",
                this, lastRpcTime.elapsedTimeMs(), electionTimeout);
            server.getLeaderElectionMetricsRegistry().onLeaderElectionTimeout(); // Update timeout metric counters.
            // election timeout, should become a candidate
            server.changeToCandidate();
            break;
          }
        }
      } catch (InterruptedException e) {
        LOG.info(this + " was interrupted: " + e);
        LOG.trace("TRACE", e);
        return;
      } catch (Exception e) {
        LOG.warn(this + " caught an exception", e);
      }
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
