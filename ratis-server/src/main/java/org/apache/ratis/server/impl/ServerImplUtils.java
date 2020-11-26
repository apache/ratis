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
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Server utilities for internal use. */
public final class ServerImplUtils {
  private ServerImplUtils() {
    //Never constructed
  }
  /** For the case that all {@link RaftServerImpl} objects share the same {@link StateMachine}. */
  public static RaftServerProxy newRaftServer(
      RaftPeerId id, RaftGroup group, StateMachine.Registry stateMachineRegistry,
      RaftProperties properties, Parameters parameters) throws IOException {
    RaftServer.LOG.debug("newRaftServer: {}, {}", id, group);
    final RaftServerProxy proxy = newRaftServer(id, stateMachineRegistry, properties, parameters);
    proxy.initGroups(group);
    return proxy;
  }

  public static RaftServerProxy newRaftServer(
      RaftPeerId id, StateMachine.Registry stateMachineRegistry, RaftProperties properties, Parameters parameters)
      throws IOException {
    final TimeDuration sleepTime = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
    final RaftServerProxy proxy;
    try {
      // attempt multiple times to avoid temporary bind exception
      proxy = JavaUtils.attemptRepeatedly(
          () -> new RaftServerProxy(id, stateMachineRegistry, properties, parameters),
          5, sleepTime, "new RaftServerProxy", RaftServer.LOG);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException(
          "Interrupted when creating RaftServer " + id, e);
    }
    return proxy;
  }

  static long effectiveCommitIndex(long leaderCommitIndex, TermIndex followerPrevious, int numAppendEntries) {
    final long p = Optional.ofNullable(followerPrevious).map(TermIndex::getIndex).orElse(RaftLog.LEAST_VALID_LOG_INDEX);
    return Math.min(leaderCommitIndex, p + numAppendEntries);
  }

  public static TermIndex newTermIndex(long term, long index) {
    return new TermIndexImpl(term, index);
  }

  private static class TermIndexImpl implements TermIndex {
    private final long term;
    private final long index; //log index; first index is 1.

    TermIndexImpl(long term, long logIndex) {
      this.term = term;
      this.index = logIndex;
    }

    @Override
    public long getTerm() {
      return term;
    }

    @Override
    public long getIndex() {
      return index;
    }

    @Override
    public int compareTo(TermIndex that) {
      final int d = Long.compare(this.getTerm(), that.getTerm());
      return d != 0 ? d : Long.compare(this.getIndex(), that.getIndex());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj == null || !(obj instanceof TermIndexImpl)) {
        return false;
      }

      final TermIndexImpl that = (TermIndexImpl) obj;
      return this.getTerm() == that.getTerm()
          && this.getIndex() == that.getIndex();
    }

    @Override
    public int hashCode() {
      return Long.hashCode(term) ^ Long.hashCode(index);
    }

    @Override
    public String toString() {
      return TermIndex.toString(term, index);
    }
  }
}
