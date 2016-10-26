/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.raft.netty.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Netty server handlers that respond to Network events.
 */
public class NettyServerHandler<REQUEST> extends SimpleChannelInboundHandler<REQUEST> {
  static final Logger LOG = LoggerFactory.getLogger(NettyServerHandler.class);

  private final Dispatcher<REQUEST, ?> dispatcher;

  /**
   * Constructor for server handler.
   * @param dispatcher - Dispatcher interface
   */
  public NettyServerHandler(Dispatcher<REQUEST, ?> dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, REQUEST msg)
      throws IOException {
    final Object response = dispatcher.dispatch(msg);
    ctx.writeAndFlush(response);
  }
}
