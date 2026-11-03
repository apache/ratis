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
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.grpc.RaftClientProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link GrpcUtil#shutdownManagedChannel(ManagedChannel, TimeDuration, TimeDuration)} with a call the server
 * never ends, so that a graceful shutdown waits for its full timeout.
 */
public class TestGrpcChannelShutdown extends BaseTest {
  private static final MethodDescriptor.Marshaller<byte[]> BYTES = new MethodDescriptor.Marshaller<byte[]>() {
    @Override
    public InputStream stream(byte[] value) {
      return new ByteArrayInputStream(value);
    }

    @Override
    public byte[] parse(InputStream stream) {
      try {
        final byte[] bytes = new byte[stream.available()];
        Assertions.assertEquals(bytes.length, stream.read(bytes));
        return bytes;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  };

  private static final MethodDescriptor<byte[], byte[]> NEVER_ENDS = MethodDescriptor.<byte[], byte[]>newBuilder()
      .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
      .setFullMethodName(MethodDescriptor.generateFullMethodName("test", "neverEnds"))
      .setRequestMarshaller(BYTES)
      .setResponseMarshaller(BYTES)
      .build();

  private final CountDownLatch received = new CountDownLatch(1);
  private Server server;

  @BeforeEach
  public void startServer() throws IOException {
    // the server never ends its calls, so a graceful shutdown waits until it is forced
    final ServerServiceDefinition test = ServerServiceDefinition.builder("test")
        .addMethod(NEVER_ENDS, neverEnds())
        .build();
    // a Raft server that never replies to a client
    final ServerServiceDefinition raft = ServerServiceDefinition.builder(RaftClientProtocolServiceGrpc.SERVICE_NAME)
        .addMethod(RaftClientProtocolServiceGrpc.getOrderedMethod(), neverEnds())
        .build();
    server = NettyServerBuilder.forAddress(new InetSocketAddress("localhost", 0))
        .addService(test).addService(raft).build().start();
  }

  private <REQUEST, REPLY> ServerCallHandler<REQUEST, REPLY> neverEnds() {
    return ServerCalls.asyncBidiStreamingCall(replies -> new StreamObserver<REQUEST>() {
      @Override
      public void onNext(REQUEST value) {
        received.countDown();
      }

      @Override
      public void onError(Throwable t) {
      }

      @Override
      public void onCompleted() {
      }
    });
  }

  @AfterEach
  public void stopServer() throws InterruptedException {
    server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** Opens a channel with a call in progress. */
  private ManagedChannel newChannelWithOpenCall() throws InterruptedException {
    final ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
    final StreamObserver<byte[]> requests = ClientCalls.asyncBidiStreamingCall(
        channel.newCall(NEVER_ENDS, CallOptions.DEFAULT), new StreamObserver<byte[]>() {
          @Override
          public void onNext(byte[] value) {
          }

          @Override
          public void onError(Throwable t) {
          }

          @Override
          public void onCompleted() {
          }
        });
    requests.onNext(new byte[] {1});
    Assertions.assertTrue(received.await(10, TimeUnit.SECONDS), "The call did not reach the server");
    return channel;
  }

  private static long shutdownMillis(ManagedChannel channel, TimeDuration graceful, TimeDuration forceful) {
    final long start = System.nanoTime();
    GrpcUtil.shutdownManagedChannel(channel, graceful, forceful);
    final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    Assertions.assertTrue(channel.isTerminated(), "The channel did not terminate");
    return millis;
  }

  @Test
  public void testGracefulTimeout() throws Exception {
    final TimeDuration graceful = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
    final long millis = shutdownMillis(newChannelWithOpenCall(), graceful, TimeDuration.valueOf(5, TimeUnit.SECONDS));
    // the graceful shutdown waits for its whole timeout, then the forceful one cancels the call right away
    Assertions.assertTrue(millis >= 500, "Returned after " + millis + " ms");
    Assertions.assertTrue(millis < 3000, "Returned after " + millis + " ms");
  }

  @Test
  public void testZeroGracefulTimeoutShutsDownNow() throws Exception {
    final long millis = shutdownMillis(newChannelWithOpenCall(), TimeDuration.ZERO,
        TimeDuration.valueOf(5, TimeUnit.SECONDS));
    // the default would wait 3 s for the graceful shutdown
    Assertions.assertTrue(millis < 1000, "Returned after " + millis + " ms");
  }

  @Test
  public void testRaftClientUsesConfiguredTimeouts() throws Exception {
    final RaftProperties properties = new RaftProperties();
    final TimeDuration graceful = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
    GrpcConfigKeys.Client.setChannelShutdownGracefulTimeout(properties, graceful);
    final RaftPeer peer = RaftPeer.newBuilder().setId("s0").setAddress("localhost:" + server.getPort()).build();
    final RaftClient client = RaftClient.newBuilder()
        .setRaftGroup(RaftGroup.valueOf(RaftGroupId.randomId(), peer))
        .setProperties(properties)
        .build();
    // the request is never answered, so its stream stays open
    client.async().send(new RaftTestUtil.SimpleMessage("m"));
    Assertions.assertTrue(received.await(10, TimeUnit.SECONDS), "The request did not reach the server");

    final long start = System.nanoTime();
    client.close();
    final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    // the default graceful timeout would wait 3 s
    Assertions.assertTrue(millis >= 500, "Closed after " + millis + " ms");
    Assertions.assertTrue(millis < 2500, "Closed after " + millis + " ms");
  }

  @Test
  public void testConfigKeys() {
    final RaftProperties properties = new RaftProperties();
    Assertions.assertEquals(3000, millis(GrpcConfigKeys.Client.channelShutdownGracefulTimeout(properties)));
    Assertions.assertEquals(2000, millis(GrpcConfigKeys.Client.channelShutdownForcefulTimeout(properties)));

    GrpcConfigKeys.Client.setChannelShutdownGracefulTimeout(properties, TimeDuration.ZERO);
    final TimeDuration forceful = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
    GrpcConfigKeys.Client.setChannelShutdownForcefulTimeout(properties, forceful);
    Assertions.assertEquals(0, millis(GrpcConfigKeys.Client.channelShutdownGracefulTimeout(properties)));
    Assertions.assertEquals(100, millis(GrpcConfigKeys.Client.channelShutdownForcefulTimeout(properties)));

    // the server keys are separate from the client keys but share their defaults
    Assertions.assertSame(GrpcConfigKeys.CHANNEL_SHUTDOWN_GRACEFUL_TIMEOUT_DEFAULT,
        GrpcConfigKeys.Client.CHANNEL_SHUTDOWN_GRACEFUL_TIMEOUT_DEFAULT);
    Assertions.assertSame(GrpcConfigKeys.CHANNEL_SHUTDOWN_GRACEFUL_TIMEOUT_DEFAULT,
        GrpcConfigKeys.Server.CHANNEL_SHUTDOWN_GRACEFUL_TIMEOUT_DEFAULT);
    Assertions.assertSame(GrpcConfigKeys.CHANNEL_SHUTDOWN_FORCEFUL_TIMEOUT_DEFAULT,
        GrpcConfigKeys.Client.CHANNEL_SHUTDOWN_FORCEFUL_TIMEOUT_DEFAULT);
    Assertions.assertSame(GrpcConfigKeys.CHANNEL_SHUTDOWN_FORCEFUL_TIMEOUT_DEFAULT,
        GrpcConfigKeys.Server.CHANNEL_SHUTDOWN_FORCEFUL_TIMEOUT_DEFAULT);
    Assertions.assertEquals(3000, millis(GrpcConfigKeys.Server.channelShutdownGracefulTimeout(properties)));
    Assertions.assertEquals(2000, millis(GrpcConfigKeys.Server.channelShutdownForcefulTimeout(properties)));

    GrpcConfigKeys.Server.setChannelShutdownGracefulTimeout(properties, TimeDuration.ONE_SECOND);
    GrpcConfigKeys.Server.setChannelShutdownForcefulTimeout(properties,
        TimeDuration.valueOf(200, TimeUnit.MILLISECONDS));
    Assertions.assertEquals(1000, millis(GrpcConfigKeys.Server.channelShutdownGracefulTimeout(properties)));
    Assertions.assertEquals(200, millis(GrpcConfigKeys.Server.channelShutdownForcefulTimeout(properties)));
    Assertions.assertEquals(0, millis(GrpcConfigKeys.Client.channelShutdownGracefulTimeout(properties)));
    Assertions.assertEquals(100, millis(GrpcConfigKeys.Client.channelShutdownForcefulTimeout(properties)));
  }

  private static long millis(TimeDuration duration) {
    return duration.toLong(TimeUnit.MILLISECONDS);
  }
}
