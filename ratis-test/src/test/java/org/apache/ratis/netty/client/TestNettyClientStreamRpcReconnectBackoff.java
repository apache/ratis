/**
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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.DataStreamObserver;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuf;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNettyClientStreamRpcReconnectBackoff {
  @Test
  public void testReconnectPolicyBackoffRanges() throws Exception {
    // Use a small base/max to keep the test fast and deterministic in range checks.
    final RaftProperties properties = new RaftProperties();
    final TimeDuration base = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
    final TimeDuration max = TimeDuration.valueOf(400, TimeUnit.MILLISECONDS);
    final int maxAttempts = 5;
    NettyConfigKeys.DataStream.Client.setReconnectPolicy(properties,
        "ExponentialBackoffRetry," + base + "," + max + "," + maxAttempts);

    final RaftPeer peer = RaftPeer.newBuilder()
        .setId("s1")
        .setDataStreamAddress(new InetSocketAddress("127.0.0.1", 1))
        .build();

    final NettyClientStreamRpc rpc = new NettyClientStreamRpc(peer, null, properties);
    try {
      // Verify the reconnect policy is exponential and uses the configured maxAttempts.
      final RetryPolicy policy = rpc.getReconnectPolicy();
      assertTrue(policy instanceof ExponentialBackoffRetry);
      assertFalse(policy.handleAttemptFailure(() -> maxAttempts).shouldRetry());

      // attempt=0 -> base delay; attempt=1 -> 2x base; attempt=3 -> capped by max.
      assertSleepInRange(policy, 0, base, max);
      assertSleepInRange(policy, 1, base, max);
      // Attempt 3 should be capped by max sleep time.
      assertSleepInRange(policy, 3, base, max);
    } finally {
      rpc.close();
    }
  }

  private static void assertSleepInRange(RetryPolicy policy, int attempt, TimeDuration base, TimeDuration max) {
    final RetryPolicy.Action action = policy.handleAttemptFailure(() -> attempt);
    assertTrue(action.shouldRetry());

    final long baseMillis = base.toLong(TimeUnit.MILLISECONDS);
    final long maxMillis = max.toLong(TimeUnit.MILLISECONDS);
    final long expected = Math.min(maxMillis, baseMillis * (1L << attempt));
    final long actual = action.getSleepTime().toLong(TimeUnit.MILLISECONDS);

    assertTrue(actual >= expected / 2, "delay too small: " + actual);
    assertTrue(actual <= expected + expected / 2, "delay too large: " + actual);
  }

  @Test
  public void testReadOnlyStreamingReplyCompletesObserverOnTerminalReply() {
    final ClientId clientId = ClientId.randomId();
    final long streamId = 1;
    final DataStreamRequestHeader request = new DataStreamRequestHeader(clientId, Type.STREAM_HEADER, streamId, 0, 0);
    final CompletableFuture<DataStreamReply> replyFuture = new CompletableFuture<>();
    final AtomicReference<DataStreamReplyByteBuf> observed = new AtomicReference<>();
    final AtomicBoolean completed = new AtomicBoolean();
    final NettyClientStreamRpc.ReadOnlyStreamingReply replies = new NettyClientStreamRpc.ReadOnlyStreamingReply(
        request, replyFuture, new DataStreamObserver<DataStreamReplyByteBuf>() {
          @Override
          public void onNext(DataStreamReplyByteBuf value) {
            observed.set(value);
          }

          @Override
          public void onCompleted() {
            completed.set(true);
          }
        });

    final DataStreamReplyByteBuf terminalReply = DataStreamReplyByteBuf.newBuilder()
        .setClientId(clientId)
        .setType(Type.STREAM_HEADER)
        .setStreamId(streamId)
        .setStreamOffset(0)
        .setBuf(Unpooled.EMPTY_BUFFER)
        .setSuccess(true)
        .build();
    try (DataStreamReplyByteBuf reply = terminalReply) {
      assertTrue(replies.receiveReply(reply));
    }

    assertSame(terminalReply, observed.get());
    assertTrue(completed.get());
    assertTrue(replyFuture.isDone());
  }

  @Test
  public void testReadOnlyStreamingReplyNotifiesObserverOnError() {
    final ClientId clientId = ClientId.randomId();
    final DataStreamRequestHeader request = new DataStreamRequestHeader(clientId, Type.STREAM_HEADER, 1, 0, 0);
    final CompletableFuture<DataStreamReply> replyFuture = new CompletableFuture<>();
    final AtomicReference<Throwable> observed = new AtomicReference<>();
    final NettyClientStreamRpc.ReadOnlyStreamingReply replies = new NettyClientStreamRpc.ReadOnlyStreamingReply(
        request, replyFuture, new DataStreamObserver<DataStreamReplyByteBuf>() {
          @Override
          public void onNext(DataStreamReplyByteBuf value) {
          }

          @Override
          public void onError(Throwable t) {
            observed.set(t);
          }
        });

    final Throwable cause = new IllegalStateException("test");
    replies.completeExceptionally(cause);

    assertSame(cause, observed.get());
    assertTrue(replyFuture.isCompletedExceptionally());
  }
}
