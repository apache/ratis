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
import org.apache.ratis.util.NettyUtils;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.Epoll;
import org.apache.ratis.thirdparty.io.netty.util.concurrent.EventExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGrpcEventLoops extends BaseTest {

  @Test
  public void testNewEventLoopGroupWithThreadCount() {
    final int threads = 3;
    final EventLoopGroup group = NettyUtils.newEventLoopGroup("test-elg", threads, false);
    try {
      Assertions.assertNotNull(group);
      int count = 0;
      for (EventExecutor ignored : group) {
        count++;
      }
      Assertions.assertEquals(threads, count);
    } finally {
      NettyUtils.shutdownGracefully(group);
    }
  }

  @Test
  public void testChannelTypeMatchesEpollAvailability() {
    final EventLoopGroup group = NettyUtils.newEventLoopGroup("test-epoll", 1, true);
    try {
      if (Epoll.isAvailable()) {
        Assertions.assertEquals("EpollServerSocketChannel",
            NettyUtils.getServerChannelClass(group).getSimpleName());
        Assertions.assertEquals("EpollSocketChannel",
            NettyUtils.getSocketChannelClass(group).getSimpleName());
      } else {
        Assertions.assertEquals("NioServerSocketChannel",
            NettyUtils.getServerChannelClass(group).getSimpleName());
        Assertions.assertEquals("NioSocketChannel",
            NettyUtils.getSocketChannelClass(group).getSimpleName());
      }
    } finally {
      NettyUtils.shutdownGracefully(group);
    }
  }

  @Test
  public void testConfigKeyDefaults() {
    final RaftProperties properties = new RaftProperties();
    final int expectedWorker = GrpcConfigKeys.Server.WORKER_GROUP_SIZE_DEFAULT;
    Assertions.assertTrue(expectedWorker > 0,
        "default worker threads should be positive, but got " + expectedWorker);
    Assertions.assertEquals(expectedWorker, GrpcConfigKeys.Server.workerGroupSize(properties));
    Assertions.assertEquals(expectedWorker, GrpcConfigKeys.Client.workerGroupSize(properties));
    Assertions.assertEquals(0, GrpcConfigKeys.Server.bossGroupSize(properties));
    Assertions.assertTrue(GrpcConfigKeys.useEpoll(properties));
  }

  @Test
  public void testConfigKeyRoundtrip() {
    final RaftProperties properties = new RaftProperties();
    GrpcConfigKeys.Server.setWorkerGroupSize(properties, 4);
    GrpcConfigKeys.Server.setBossGroupSize(properties, 1);
    GrpcConfigKeys.Client.setWorkerGroupSize(properties, 2);
    GrpcConfigKeys.setUseEpoll(properties, false);
    Assertions.assertEquals(4, GrpcConfigKeys.Server.workerGroupSize(properties));
    Assertions.assertEquals(1, GrpcConfigKeys.Server.bossGroupSize(properties));
    Assertions.assertEquals(2, GrpcConfigKeys.Client.workerGroupSize(properties));
    Assertions.assertFalse(GrpcConfigKeys.useEpoll(properties));
  }

  @Test
  public void testShutdownNullIsNoop() {
    NettyUtils.shutdownGracefully((EventLoopGroup) null);
  }
}
