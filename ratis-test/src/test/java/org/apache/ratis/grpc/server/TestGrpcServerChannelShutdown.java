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
package org.apache.ratis.grpc.server;

import org.apache.ratis.BaseTest;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexReplyProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests that {@link GrpcServerProtocolClient#close()} uses the configured channel shutdown timeouts
 * when the peer never ends its calls.
 */
public class TestGrpcServerChannelShutdown extends BaseTest {
  private static final TimeDuration GRACEFUL = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
  private static final TimeDuration FORCEFUL = TimeDuration.valueOf(2, TimeUnit.SECONDS);

  private final CountDownLatch received = new CountDownLatch(1);
  private Server server;

  @BeforeEach
  public void startServer() throws IOException {
    // a peer that never replies, so a graceful shutdown waits until it is forced
    final ServerServiceDefinition peer = ServerServiceDefinition.builder(RaftServerProtocolServiceGrpc.SERVICE_NAME)
        .addMethod(RaftServerProtocolServiceGrpc.getAppendEntriesMethod(),
            ServerCalls.asyncBidiStreamingCall(replies -> new StreamObserver<AppendEntriesRequestProto>() {
              @Override
              public void onNext(AppendEntriesRequestProto value) {
                received.countDown();
              }

              @Override
              public void onError(Throwable t) {
              }

              @Override
              public void onCompleted() {
              }
            }))
        .addMethod(RaftServerProtocolServiceGrpc.getReadIndexMethod(),
            ServerCalls.<ReadIndexRequestProto, ReadIndexReplyProto>asyncUnaryCall(
                (request, reply) -> received.countDown()))
        .build();
    server = NettyServerBuilder.forAddress(new InetSocketAddress("localhost", 0))
        .addService(peer).build().start();
  }

  @AfterEach
  public void stopServer() throws InterruptedException {
    server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
  }

  private GrpcServerProtocolClient newClient(int connections) {
    final RaftPeer peer = RaftPeer.newBuilder().setId("s0").setAddress("localhost:" + server.getPort()).build();
    // the request timeout must not end the calls before the shutdown
    return new GrpcServerProtocolClient(peer, connections, 1 << 20, TimeDuration.valueOf(1, TimeUnit.MINUTES),
        null, false, null, GRACEFUL, FORCEFUL);
  }

  private static <T> StreamObserver<T> noop() {
    return new StreamObserver<T>() {
      @Override
      public void onNext(T value) {
      }

      @Override
      public void onError(Throwable t) {
      }

      @Override
      public void onCompleted() {
      }
    };
  }

  private long closeMillis(GrpcServerProtocolClient client) throws InterruptedException {
    Assertions.assertTrue(received.await(10, TimeUnit.SECONDS), "The request did not reach the peer");
    final long start = System.nanoTime();
    client.close();
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
  }

  private static void assertGracefulTimeoutUsed(long millis) {
    // the default graceful timeout would wait 3 s
    Assertions.assertTrue(millis >= 500, "Closed after " + millis + " ms");
    Assertions.assertTrue(millis < 2500, "Closed after " + millis + " ms");
  }

  @Test
  public void testChannel() throws Exception {
    final GrpcServerProtocolClient client = newClient(1);
    client.appendEntries(noop(), false).onNext(AppendEntriesRequestProto.getDefaultInstance());
    assertGracefulTimeoutUsed(closeMillis(client));
  }

  @Test
  public void testStubPool() throws Exception {
    final GrpcServerProtocolClient client = newClient(2);
    client.readIndex(ReadIndexRequestProto.getDefaultInstance(), noop());
    assertGracefulTimeoutUsed(closeMillis(client));
  }
}
