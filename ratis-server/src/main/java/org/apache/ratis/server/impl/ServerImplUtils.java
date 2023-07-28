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

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Server utilities for internal use. */
public final class ServerImplUtils {
  private ServerImplUtils() {
    //Never constructed
  }

  /** Create a {@link RaftServerProxy}. */
  public static RaftServerProxy newRaftServer(
      RaftPeerId id, RaftGroup group, RaftStorage.StartupOption option, StateMachine.Registry stateMachineRegistry,
      ThreadGroup threadGroup, RaftProperties properties, Parameters parameters) throws IOException {
    RaftServer.LOG.debug("newRaftServer: {}, {}", id, group);
    if (group != null && !group.getPeers().isEmpty()) {
      Preconditions.assertNotNull(id, "RaftPeerId %s is not in RaftGroup %s", id, group);
      Preconditions.assertNotNull(group.getPeer(id), "RaftPeerId %s is not in RaftGroup %s", id, group);
    }
    final RaftServerProxy proxy = newRaftServer(id, stateMachineRegistry, threadGroup, properties, parameters);
    proxy.initGroups(group, option);
    return proxy;
  }

  private static RaftServerProxy newRaftServer(
      RaftPeerId id, StateMachine.Registry stateMachineRegistry, ThreadGroup threadGroup, RaftProperties properties,
      Parameters parameters) throws IOException {
    final TimeDuration sleepTime = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
    final RaftServerProxy proxy;
    try {
      // attempt multiple times to avoid temporary bind exception
      proxy = JavaUtils.attemptRepeatedly(
          () -> new RaftServerProxy(id, stateMachineRegistry, properties, parameters, threadGroup),
          5, sleepTime, "new RaftServerProxy", RaftServer.LOG);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException(
          "Interrupted when creating RaftServer " + id, e);
    }
    return proxy;
  }

  public static RaftConfiguration newRaftConfiguration(List<RaftPeer> conf, List<RaftPeer> listener,
      long index, List<RaftPeer> oldConf, List<RaftPeer> oldListener) {
    final RaftConfigurationImpl.Builder b = RaftConfigurationImpl.newBuilder()
        .setConf(conf, listener)
        .setLogEntryIndex(index);
    if (!oldConf.isEmpty() || !oldListener.isEmpty()) {
      b.setOldConf(oldConf, oldListener);
    }
    return b.build();
  }

  static long effectiveCommitIndex(long leaderCommitIndex, TermIndex followerPrevious, int numAppendEntries) {
    final long p = Optional.ofNullable(followerPrevious).map(TermIndex::getIndex).orElse(RaftLog.LEAST_VALID_LOG_INDEX);
    return Math.min(leaderCommitIndex, p + numAppendEntries);
  }
}
