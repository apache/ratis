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

import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
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
  private final Object reason;
  private final RaftServerImpl server;

  private final Timestamp creationTime = Timestamp.currentTime();
  private volatile Timestamp lastRpcTime = creationTime;
  private volatile boolean isRunning = true;
  private final AtomicInteger outstandingOp = new AtomicInteger();

  FollowerState(RaftServerImpl server, Object reason) {
    this.name = server.getMemberId() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.setName(this.name);
    this.server = server;
    this.reason = reason;
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

  boolean isCurrentLeaderValid() {
    return lastRpcTime.elapsedTime().compareTo(server.properties().minRpcTimeout()) < 0;
  }

  void stopRunning() {
    this.isRunning = false;
  }

  boolean lostMajorityHeartbeatsRecently() {
    if (reason != LeaderState.StepDownReason.LOST_MAJORITY_HEARTBEATS) {
      return false;
    }
    final TimeDuration elapsed = creationTime.elapsedTime();
    final TimeDuration waitTime = server.getLeaderStepDownWaitTime();
    if (elapsed.compareTo(waitTime) >= 0) {
      return false;
    }
    LOG.info("{}: Skipping leader election since it stepped down recently (elapsed = {} < waitTime = {})",
        this, elapsed.to(TimeUnit.MILLISECONDS), waitTime);
    return true;
  }

  @Override
  public  void run() {
    final TimeDuration sleepDeviationThreshold = server.getSleepDeviationThreshold();
    while (isRunning && server.getInfo().isFollower()) {
      final TimeDuration electionTimeout = server.getRandomElectionTimeout();
      try {
        final TimeDuration extraSleep = electionTimeout.sleep();
        if (extraSleep.compareTo(sleepDeviationThreshold) > 0) {
          LOG.warn("Unexpected long sleep: sleep {} but took extra {} (> threshold = {})",
              electionTimeout, extraSleep, sleepDeviationThreshold);
          continue;
        }

        final boolean isFollower = server.getInfo().isFollower();
        if (!isRunning || !isFollower) {
          LOG.info("{}: Stopping now (isRunning? {}, isFollower? {})", this, isRunning, isFollower);
          break;
        }
        synchronized (server) {
          if (outstandingOp.get() == 0
              && isRunning
              && lastRpcTime.elapsedTime().compareTo(electionTimeout) >= 0
              && !lostMajorityHeartbeatsRecently()) {
            LOG.info("{}: change to CANDIDATE, lastRpcElapsedTime:{}, electionTimeout:{}",
                this, lastRpcTime.elapsedTime(), electionTimeout);
            server.getLeaderElectionMetrics().onLeaderElectionTimeout(); // Update timeout metric counters.
            // election timeout, should become a candidate
            server.changeToCandidate(false);
            break;
          }
        }
      } catch (InterruptedException e) {
        LOG.info("{} was interrupted", this);
        LOG.trace("TRACE", e);
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        LOG.warn("{} caught an exception", this, e);
      }
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
