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
package org.apache.ratis.server.leader;

import org.apache.ratis.BaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestLogAppenderDaemon extends BaseTest {
  @Test
  public void testStopFromAppenderAllowsMetadataWrite() throws Exception {
    final LogAppenderBase appender = newAppender();
    final LogAppenderDaemon daemon = new LogAppenderDaemon(appender);
    final File metadata = new File(getTestDir(), "metadata");
    Files.createDirectories(metadata.getParentFile().toPath());
    final CompletableFuture<Void> written = new CompletableFuture<>();
    Mockito.doAnswer(invocation -> {
      try {
        daemon.tryToClose();
        // A higher-term reply stops its own appender before persisting the term.
        try (AtomicFileOutputStream out = new AtomicFileOutputStream(metadata)) {
          out.write(3);
        }
        written.complete(null);
      } catch (Throwable t) {
        written.completeExceptionally(t);
      }
      return null;
    }).when(appender).run();

    daemon.tryToStart();
    try {
      written.get(5, TimeUnit.SECONDS);
      Assertions.assertArrayEquals(new byte[]{3}, Files.readAllBytes(metadata.toPath()));
    } finally {
      Assertions.assertEquals(LifeCycle.State.CLOSED, daemon.tryToClose().get(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testExternalStopInterruptsAppender() throws Exception {
    final LogAppenderBase appender = newAppender();
    final LogAppenderDaemon daemon = new LogAppenderDaemon(appender);
    final CountDownLatch running = new CountDownLatch(1);
    final CountDownLatch waitForStop = new CountDownLatch(1);
    final CompletableFuture<Boolean> interrupted = new CompletableFuture<>();
    Mockito.doAnswer(invocation -> {
      running.countDown();
      try {
        waitForStop.await();
        interrupted.complete(false);
      } catch (InterruptedException e) {
        interrupted.complete(true);
        Thread.currentThread().interrupt();
      }
      return null;
    }).when(appender).run();

    daemon.tryToStart();
    try {
      Assertions.assertTrue(running.await(5, TimeUnit.SECONDS));
      Assertions.assertEquals(LifeCycle.State.CLOSED, daemon.tryToClose().get(5, TimeUnit.SECONDS));
      Assertions.assertTrue(interrupted.get(5, TimeUnit.SECONDS));
    } finally {
      waitForStop.countDown();
      daemon.tryToClose().get(5, TimeUnit.SECONDS);
    }
  }

  private static LogAppenderBase newAppender() {
    final RaftServer.Division server = Mockito.mock(RaftServer.Division.class);
    Mockito.when(server.getThreadGroup()).thenReturn(Thread.currentThread().getThreadGroup());
    final RaftServer raftServer = Mockito.mock(RaftServer.class);
    Mockito.when(raftServer.getProperties()).thenReturn(new RaftProperties());
    Mockito.when(server.getRaftServer()).thenReturn(raftServer);
    return Mockito.spy(new LogAppenderDefault(server, Mockito.mock(LeaderState.class), Mockito.mock(FollowerInfo.class)));
  }
}
