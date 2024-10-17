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
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.test.proto.GreeterGrpc;
import org.apache.ratis.test.proto.HelloReply;
import org.apache.ratis.test.proto.HelloRequest;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.NetUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public class TestCustomGrpcServices extends BaseTest {
  /** Add two different greeter services for client and admin. */
  class MyCustomizer implements GrpcServices.Customizer {
    final GreeterImpl clientGreeter = new GreeterImpl("Hello");
    final GreeterImpl adminGreeter = new GreeterImpl("Hi");

    @Override
    public NettyServerBuilder customize(NettyServerBuilder builder, EnumSet<GrpcServices.Type> types) {
      if (types.contains(GrpcServices.Type.CLIENT)) {
        return builder.addService(clientGreeter);
      }
      if (types.contains(GrpcServices.Type.ADMIN)) {
        return builder.addService(adminGreeter);
      }
      return builder;
    }
  }

  class GreeterImpl extends GreeterGrpc.GreeterImplBase {
    private final String prefix;

    GreeterImpl(String prefix) {
      this.prefix = prefix;
    }

    String toReply(String request) {
      return prefix + " " + request;
    }

    @Override
    public StreamObserver<HelloRequest> hello(StreamObserver<HelloReply> responseObserver) {
      return new StreamObserver<HelloRequest>() {
        @Override
        public void onNext(HelloRequest helloRequest) {
          final String reply = toReply(helloRequest.getName());
          responseObserver.onNext(HelloReply.newBuilder().setMessage(reply).build());
        }

        @Override
        public void onError(Throwable throwable) {
          LOG.error("onError", throwable);
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      };
    }
  }

  class GreeterClient implements Closeable {
    private final ManagedChannel channel;
    private final StreamObserver<HelloRequest> requestHandler;
    private final Queue<CompletableFuture<String>> replies = new ConcurrentLinkedQueue<>();

    GreeterClient(int port) {
      this.channel = ManagedChannelBuilder.forAddress(NetUtils.LOCALHOST, port)
          .usePlaintext()
          .build();

      final StreamObserver<HelloReply> responseHandler = new StreamObserver<HelloReply>() {
        @Override
        public void onNext(HelloReply helloReply) {
          Objects.requireNonNull(replies.poll(), "queue is empty")
              .complete(helloReply.getMessage());
        }

        @Override
        public void onError(Throwable throwable) {
          LOG.info("onError", throwable);
          completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {
          LOG.info("onCompleted");
          completeExceptionally(new IllegalStateException("onCompleted"));
        }

        void completeExceptionally(Throwable throwable) {
          replies.forEach(f -> f.completeExceptionally(throwable));
          replies.clear();
        }
      };
      this.requestHandler = GreeterGrpc.newStub(channel).hello(responseHandler);
    }

    @Override
    public void close() throws IOException {
      try {
        /* After the request handler is cancelled, no more life-cycle hooks are allowed,
         * see {@link org.apache.ratis.thirdparty.io.grpc.ClientCall.Listener#cancel(String, Throwable)} */
        // requestHandler.onCompleted();
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw IOUtils.toInterruptedIOException("Failed to close", e);
      }
    }

    CompletableFuture<String> send(String name) {
      LOG.info("send: {}", name);
      final HelloRequest request = HelloRequest.newBuilder().setName(name).build();
      final CompletableFuture<String> f = new CompletableFuture<>();
      try {
        requestHandler.onNext(request);
        replies.offer(f);
      } catch (IllegalStateException e) {
        // already closed
        f.completeExceptionally(e);
      }
      return f.whenComplete((r, e) -> LOG.info("reply: {}", r));
    }
  }

  @Test
  public void testCustomServices() throws Exception {
    final String[] ids = {"s0"};
    final RaftProperties properties = new RaftProperties();

    final Parameters parameters = new Parameters();
    final MyCustomizer customizer = new MyCustomizer();
    GrpcConfigKeys.Server.setServicesCustomizer(parameters, customizer);

    try(MiniRaftClusterWithGrpc cluster = new MiniRaftClusterWithGrpc(ids, properties, parameters)) {
      cluster.start();
      final RaftServerRpc server = waitForLeader(cluster).getRaftServer().getServerRpc();

      // test Raft service
      try (RaftClient client = cluster.createClient()) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("abc"));
        Assertions.assertTrue(reply.isSuccess());
      }

      // test custom client service
      final int clientPort = server.getClientServerAddress().getPort();
      try (GreeterClient client = new GreeterClient(clientPort)) {
        sendAndAssertReply("world", client, customizer.clientGreeter);
      }

      // test custom admin service
      final int adminPort = server.getAdminServerAddress().getPort();
      try (GreeterClient admin = new GreeterClient(adminPort)) {
        sendAndAssertReply("admin", admin, customizer.adminGreeter);
      }
    }
  }

  static void sendAndAssertReply(String name, GreeterClient client, GreeterImpl greeter) {
    final String computed = client.send(name).join();
    final String expected = greeter.toReply(name);
    Assertions.assertEquals(expected, computed);
  }
}
