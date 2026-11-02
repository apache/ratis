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
package org.apache.ratis.datastream;

import org.apache.ratis.BaseTest;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.DataStreamClientImpl.DataStreamOutputImpl;
import org.apache.ratis.datastream.DataStreamTestUtils.MultiDataStreamStateMachine;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.util.CollectionUtils;
import org.apache.ratis.util.NettyUtils;
import org.apache.ratis.util.function.CheckedRunnable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

/**
 * Client event loop threads must be daemon threads, so that a client which is not closed does not keep its JVM from
 * exiting. Netty starts an event loop thread from the thread that first submits a task to it, such as a connect, so
 * without the daemon flag the thread would inherit the daemon status of that thread, often an application's main
 * thread.
 */
public class TestDataStreamClientDaemonThreads extends BaseTest
    implements MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty.FactoryGet {
  {
    setStateMachine(MultiDataStreamStateMachine.class);
  }

  private static final String WORKER_GROUP_THREAD = "NettyClientStreamRpc-workerGroup";

  @Test
  public void testDaemonEventLoopGroup() throws Exception {
    final EventLoopGroup daemon = NettyUtils.newEventLoopGroup("test-daemon", 1, false, true);
    final EventLoopGroup inheriting = NettyUtils.newEventLoopGroup("test-inheriting", 1, false);
    try {
      runOnNonDaemonThread(() -> {
        Assertions.assertTrue(daemon.submit(() -> Thread.currentThread().isDaemon()).get());
        Assertions.assertFalse(inheriting.submit(() -> Thread.currentThread().isDaemon()).get());
      });
    } finally {
      NettyUtils.shutdownGracefully(daemon, inheriting);
    }
  }

  @Test
  public void testDataStreamClientThreadsAreDaemon() throws Exception {
    runWithNewCluster(3, cluster -> {
      waitForLeader(cluster);
      final RaftPeer primary = CollectionUtils.random(cluster.getGroup().getPeers());
      try (RaftClient client = cluster.createClient(primary)) {
        // connect and stream from a non-daemon thread, like an application's main thread
        runOnNonDaemonThread(() -> {
          try (DataStreamOutputImpl out = (DataStreamOutputImpl) client.getDataStreamApi().stream(null,
              DataStreamTestUtils.getRoutingTableChainTopology(cluster.getGroup().getPeers(), primary))) {
            DataStreamTestUtils.writeAndAssertReplies(out, 1000, 10);
          }
        });

        // the client is still open, so its worker group threads are still running
        final List<Thread> threads = Thread.getAllStackTraces().keySet().stream()
            .filter(t -> t.isAlive() && t.getName().startsWith(WORKER_GROUP_THREAD))
            .collect(Collectors.toList());
        Assertions.assertFalse(threads.isEmpty(), "No " + WORKER_GROUP_THREAD + " thread");
        for (Thread t : threads) {
          Assertions.assertTrue(t.isDaemon(), t + " is not a daemon thread");
        }
      }
    });
  }

  private static void runOnNonDaemonThread(CheckedRunnable<Exception> runnable) throws Exception {
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        runnable.run();
      } catch (Throwable t) {
        error.set(t);
      }
    });
    thread.setDaemon(false);
    thread.start();
    thread.join(TimeUnit.MINUTES.toMillis(1));
    Assertions.assertFalse(thread.isAlive(), "Timed out");
    if (error.get() instanceof Exception) {
      throw (Exception) error.get();
    } else if (error.get() != null) {
      throw new IllegalStateException(error.get());
    }
  }
}
