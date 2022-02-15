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
package org.apache.ratis.netty;

import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.Epoll;
import org.apache.ratis.thirdparty.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.ratis.util.ConcurrentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface NettyUtils {
  Logger LOG = LoggerFactory.getLogger(NettyUtils.class);

  static EventLoopGroup newEventLoopGroup(String name, int size, boolean useEpoll) {
    if (useEpoll) {
      if (Epoll.isAvailable()) {
        LOG.info("Create EpollEventLoopGroup for {}; Thread size is {}.", name, size);
        return new EpollEventLoopGroup(size, ConcurrentUtils.newThreadFactory(name + "-"));
      } else {
        LOG.warn("Failed to create EpollEventLoopGroup for " + name + "; fall back on NioEventLoopGroup.",
            Epoll.unavailabilityCause());
      }
    }
    return new NioEventLoopGroup(size, ConcurrentUtils.newThreadFactory(name + "-"));
  }
}