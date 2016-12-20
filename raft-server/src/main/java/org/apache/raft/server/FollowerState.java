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

import org.apache.hadoop.util.Daemon;
import org.apache.raft.util.RaftUtils;
import org.apache.raft.util.Timestamp;
import org.slf4j.Logger;

/**
 * Used when the peer is a follower. Used to track the election timeout.
 */
class FollowerState extends Daemon {
  static final Logger LOG = RaftServer.LOG;

  private final RaftServer server;

  private volatile Timestamp lastRpcTime = new Timestamp();
  private volatile boolean monitorRunning = true;
  private volatile boolean inLogSync = false;

  FollowerState(RaftServer server) {
    this.server = server;
  }

  void updateLastRpcTime(boolean inLogSync) {
    lastRpcTime = new Timestamp();
    LOG.trace("{} update last rpc time to {}", server.getId(), lastRpcTime);
    this.inLogSync = inLogSync;
  }

  Timestamp getLastRpcTime() {
    return lastRpcTime;
  }

  boolean shouldWithholdVotes() {
    return lastRpcTime.elapsedTimeMs() < server.minTimeout;
  }

  void stopRunning() {
    this.monitorRunning = false;
  }

  @Override
  public  void run() {
    while (monitorRunning && server.isFollower()) {
      final long electionTimeout = RaftUtils.getRandomBetween(
          server.minTimeout, server.maxTimeout);
      try {
        Thread.sleep(electionTimeout);
        if (!monitorRunning || !server.isFollower()) {
          LOG.info("{} heartbeat monitor quit", server.getId());
          break;
        }
        synchronized (server) {
          if (!inLogSync && lastRpcTime.elapsedTimeMs() >= electionTimeout) {
            LOG.info("{} changes to {}, lastRpcTime:{}, electionTimeout:{}",
                server.getId(), Role.CANDIDATE, lastRpcTime, electionTimeout);
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
