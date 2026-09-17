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

import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

  @Test
  public void testDuplicateStreamCommandAtSameOffsetFails() throws Exception {
    final NettyClientReplies replies = new NettyClientReplies();
    final ClientInvocationId clientInvocationId =
        ClientInvocationId.valueOf(ClientId.randomId(), 1L);
    final NettyClientReplies.ReplyMap replyMap = replies.getOrCreateReplyMap(clientInvocationId);
    final DataStreamRequestHeader header = new DataStreamRequestHeader(
        clientInvocationId.getClientId(), Type.STREAM_COMMAND, clientInvocationId.getLongId(), 0, 1);
    final NettyClientReplies.RequestEntry requestEntry = new NettyClientReplies.RequestEntry(header);

    final CompletableFuture<DataStreamReply> first = new CompletableFuture<>();
    Assertions.assertNotNull(replyMap.submitRequest(requestEntry, false, first));

    final CompletableFuture<DataStreamReply> duplicate = new CompletableFuture<>();
    Assertions.assertNull(replyMap.submitRequest(requestEntry, false, duplicate));
    Assertions.assertTrue(duplicate.isCompletedExceptionally());
    final ExecutionException exception = Assertions.assertThrows(ExecutionException.class, duplicate::get);
    Assertions.assertInstanceOf(IllegalStateException.class, exception.getCause());

    final DataStreamReply reply = DataStreamReplyByteBuffer.newBuilder()
        .setClientId(clientInvocationId.getClientId())
        .setType(Type.STREAM_COMMAND)
        .setStreamId(clientInvocationId.getLongId())
        .setStreamOffset(0)
        .setSuccess(true)
        .setBytesWritten(0)
        .build();
    replyMap.receiveReply(reply);

    final CompletableFuture<DataStreamReply> afterReply = new CompletableFuture<>();
    Assertions.assertNotNull(replyMap.submitRequest(requestEntry, false, afterReply));
  }
}
