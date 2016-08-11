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

import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

/**
 * Used when the peer is a follower. Used to track the election timeout.
 */
class FollowerState extends Daemon {
  static final Logger LOG = RaftServer.LOG;

  private final RaftServer server;
  private volatile long lastRpcTime = Time.monotonicNow();
  private volatile boolean monitorRunning = true;
  private volatile boolean inLogSync = false;

  FollowerState(RaftServer server) {
    this.server = server;
  }

  void updateLastRpcTime(long now, boolean inLogSync) {
    LOG.trace("{} update last rpc time to {}", server.getId(), now);
    lastRpcTime = now;
    this.inLogSync = inLogSync;
  }

  long getLastRpcTime() {
    return lastRpcTime;
  }

  boolean shouldWithholdVotes(long now) {
    return lastRpcTime + RaftConstants.ELECTION_TIMEOUT_MIN_MS > now;
  }

  void stopRunning() {
    this.monitorRunning = false;
  }

  @Override
  public  void run() {
    while (monitorRunning && server.isFollower()) {
      final long electionTimeout = RaftConstants.getRandomElectionWaitTime();
      try {
        Thread.sleep(electionTimeout);
        if (!monitorRunning || !server.isFollower()) {
          LOG.info("{} heartbeat monitor quit", server.getId());
          break;
        }
        synchronized (server) {
          final long now = Time.monotonicNow();
          if (!inLogSync && now >= lastRpcTime + electionTimeout) {
            LOG.info("{} changes to {} at {}, LastRpcTime:{}, electionTimeout:{}",
                server.getId(), Role.CANDIDATE, now, lastRpcTime,
                electionTimeout);
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
        LOG.warn(this + " caught an excpetion", e);
      }
    }
  }

  @Override
  public String toString() {
    return server.getId() + ": " + getClass().getSimpleName();
  }
}
