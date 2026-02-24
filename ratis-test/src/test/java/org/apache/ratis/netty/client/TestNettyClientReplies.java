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
package org.apache.ratis.netty.client;

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class TestNettyClientReplies {
  @Test
  public void testGetReplyMapDoesNotCreate() {
    final NettyClientReplies replies = new NettyClientReplies();
    final ClientInvocationId clientInvocationId =
        ClientInvocationId.valueOf(ClientId.randomId(), 1L);

    assertNull(replies.getReplyMap(clientInvocationId));

    final NettyClientReplies.ReplyMap created = replies.getOrCreateReplyMap(clientInvocationId);
    assertNotNull(created);
    assertSame(created, replies.getReplyMap(clientInvocationId));

    final ClientInvocationId other =
        ClientInvocationId.valueOf(ClientId.randomId(), 2L);
    assertNull(replies.getReplyMap(other));
  }
}
