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
package org.apache.ratis.grpc;

import org.apache.ratis.BaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.util.concurrent.EventExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGrpcEventLoops extends BaseTest {

  @Test
  public void testNewEventLoopGroupWithThreadCount() {
    final int threads = 3;
    final EventLoopGroup group = GrpcEventLoops.newEventLoopGroup(threads, "test-elg");
    try {
      Assertions.assertNotNull(group);
      int count = 0;
      for (EventExecutor ignored : group) {
        count++;
      }
      Assertions.assertEquals(threads, count);
    } finally {
      GrpcEventLoops.shutdownGracefully(group);
    }
  }

  @Test
  public void testNewEventLoopGroupRejectsNonPositiveThreads() {
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> GrpcEventLoops.newEventLoopGroup(0, "test-elg"));
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> GrpcEventLoops.newEventLoopGroup(-1, "test-elg"));
  }

  @Test
  public void testChannelTypeMatchesEpollAvailability() {
    if (GrpcEventLoops.isEpollAvailable()) {
      Assertions.assertEquals("EpollServerSocketChannel",
          GrpcEventLoops.getServerChannelType().getSimpleName());
      Assertions.assertEquals("EpollSocketChannel",
          GrpcEventLoops.getClientChannelType().getSimpleName());
    } else {
      Assertions.assertEquals("NioServerSocketChannel",
          GrpcEventLoops.getServerChannelType().getSimpleName());
      Assertions.assertEquals("NioSocketChannel",
          GrpcEventLoops.getClientChannelType().getSimpleName());
    }
  }

  @Test
  public void testConfigKeyDefaults() {
    final RaftProperties properties = new RaftProperties();
    Assertions.assertEquals(0, GrpcConfigKeys.Server.workerEventLoopThreads(properties));
    Assertions.assertEquals(0, GrpcConfigKeys.Client.workerEventLoopThreads(properties));
  }

  @Test
  public void testConfigKeyRoundtrip() {
    final RaftProperties properties = new RaftProperties();
    GrpcConfigKeys.Server.setWorkerEventLoopThreads(properties, 4);
    GrpcConfigKeys.Client.setWorkerEventLoopThreads(properties, 2);
    Assertions.assertEquals(4, GrpcConfigKeys.Server.workerEventLoopThreads(properties));
    Assertions.assertEquals(2, GrpcConfigKeys.Client.workerEventLoopThreads(properties));
  }

  @Test
  public void testShutdownNullIsNoop() {
    GrpcEventLoops.shutdownGracefully(null);
  }
}
