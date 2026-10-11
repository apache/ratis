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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestStateMachineUpdater {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testApplyFailureClosesStateMachine(boolean shutdownFirst) throws Exception {
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false);
    final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(
        RaftPeerId.valueOf("s0"), RaftGroupId.randomId());
    final RaftServerImpl server = Mockito.mock(RaftServerImpl.class);
    final ServerState serverState = Mockito.mock(ServerState.class);
    final RaftLog log = Mockito.mock(RaftLog.class);
    Mockito.when(server.getMemberId()).thenReturn(memberId);
    Mockito.when(serverState.getMemberId()).thenReturn(memberId);
    Mockito.when(serverState.getLog()).thenReturn(log);

    final CountDownLatch applying = new CountDownLatch(1);
    final CountDownLatch failApply = new CountDownLatch(1);
    final CountDownLatch stopIndexRead = new CountDownLatch(1);
    final CompletableFuture<Thread> applyThread = new CompletableFuture<>();
    final CompletableFuture<Void> failureClosing = new CompletableFuture<>();
    final BaseStateMachine stateMachine = Mockito.spy(new BaseStateMachine());
    Mockito.doAnswer(invocation -> {
      applyThread.complete(Thread.currentThread());
      applying.countDown();
      Assertions.assertTrue(failApply.await(10, TimeUnit.SECONDS));
      throw new IllegalStateException("Injected apply failure");
    }).when(stateMachine).notifyTermIndexUpdated(1, 1);

    final LogEntryProto entry = LogEntryProto.newBuilder().setTerm(1).setIndex(1).build();
    Mockito.when(log.get(1)).thenReturn(entry);
    Mockito.when(log.getLastCommittedIndex()).thenAnswer(invocation -> {
      if (applyThread.isDone() && Thread.currentThread() != applyThread.get()) {
        // stopAndJoin has already checked for EXCEPTION when it reads the committed index.
        stopIndexRead.countDown();
      }
      return 1L;
    });
    Mockito.when(server.applyLogToStateMachine(entry)).thenAnswer(invocation -> {
      stateMachine.notifyTermIndexUpdated(1, 1);
      return null;
    });

    final AtomicLong applied = new AtomicLong();
    final StateMachineUpdater updater = new StateMachineUpdater(
        stateMachine, server, serverState, 0, properties, applied::set);
    final LifeCycle lifeCycle = new LifeCycle("test-server");
    lifeCycle.transition(LifeCycle.State.STARTING);
    lifeCycle.transition(LifeCycle.State.RUNNING);
    Mockito.doAnswer(invocation -> {
      if (Thread.currentThread() == applyThread.get()) {
        failureClosing.complete(null);
      }
      // Match RaftServerImpl: a concurrent close does nothing once CLOSING.
      lifeCycle.checkStateAndClose(updater::stopAndJoin);
      return null;
    }).when(server).close();

    final ExecutorService closer = Executors.newSingleThreadExecutor();
    try {
      updater.start();
      Assertions.assertTrue(applying.await(10, TimeUnit.SECONDS));
      final Future<?> closing;
      if (shutdownFirst) {
        closing = closer.submit(() -> server.close());
        Assertions.assertTrue(stopIndexRead.await(10, TimeUnit.SECONDS));
      } else {
        closing = CompletableFuture.completedFuture(null);
      }
      failApply.countDown();
      failureClosing.get(10, TimeUnit.SECONDS);
      applyThread.get().join(10000);
      Assertions.assertFalse(applyThread.get().isAlive(), "Failed updater did not terminate");
      // Also join an externally initiated shutdown, if any.
      closing.get(10, TimeUnit.SECONDS);
      Assertions.assertEquals(LifeCycle.State.CLOSED, lifeCycle.getCurrentState());
      Assertions.assertEquals(0, applied.get(), "Failed entry must not advance the applied index");
      Mockito.verify(stateMachine).close();
      Mockito.verify(server).applyLogToStateMachine(entry);
    } finally {
      failApply.countDown();
      // On the unfixed code, EXCEPTION is now visible: a second stop unblocks the original join.
      failureClosing.get(10, TimeUnit.SECONDS);
      updater.stopAndJoin();
      closer.shutdownNow();
      Assertions.assertTrue(closer.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
