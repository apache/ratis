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

import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.util.NettyUtils;
import org.apache.ratis.grpc.util.StreamObserverWithTimeout;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NegotiationType;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceBlockingStub;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceStub;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is a RaftClient implementation that supports streaming data to the raft
 * ring. The stream implementation utilizes gRPC.
 */
class GrpcServerProtocolClient implements Closeable {
  // Common channel
  private final ManagedChannel channel;
  private final RaftServerProtocolServiceStub asyncStub;
  private final RaftServerProtocolServiceStub hbAsyncStub;
  // All the channels: the main channel, the other appendEntries channels and the heartbeat channel
  private final List<ManagedChannel> allChannels;
  // The appendEntries streams, using either the main channel or the other channels dedicated to appendEntries
  private final GrpcAppendEntriesChannels appendEntriesStreams;
  private final GrpcStubPool<RaftServerProtocolServiceStub> pool;
  private final RaftServerProtocolServiceBlockingStub blockingStub;

  private final TimeDuration requestTimeoutDuration;
  private static final Logger LOG = LoggerFactory.getLogger(GrpcServerProtocolClient.class);
  //visible for using in log / error messages AND to use in instrumented tests
  private final RaftPeerId raftPeerId;
  private final EventLoopGroup eventLoopGroup;

  @SuppressWarnings("checkstyle:ParameterNumber")
  GrpcServerProtocolClient(RaftPeer target, int connections, int appendEntriesConnections, int flowControlWindow,
      TimeDuration requestTimeout, SslContext sslContext, boolean separateHBChannel,
      EventLoopGroup eventLoopGroup) {
    raftPeerId = target.getId();
    LOG.info("Build channel for {}", target);
    this.eventLoopGroup = eventLoopGroup;
    channel = buildChannel(target, flowControlWindow, sslContext);
    blockingStub = RaftServerProtocolServiceGrpc.newBlockingStub(channel);
    asyncStub = RaftServerProtocolServiceGrpc.newStub(channel);

    final List<ManagedChannel> all = new ArrayList<>();
    all.add(channel);
    List<ManagedChannel> otherChannels = null;
    if (appendEntriesConnections > 1) {
      otherChannels = new ArrayList<>();
      for (int i = 0; i < appendEntriesConnections; i++) {
        otherChannels.add(buildChannel(target, flowControlWindow, sslContext));
      }
      all.addAll(otherChannels);
    }
    this.appendEntriesStreams = new GrpcAppendEntriesChannels(channel, otherChannels);

    // Channel and stub for heartbeat
    if (separateHBChannel) {
      final ManagedChannel hbChannel = buildChannel(target, flowControlWindow, sslContext);
      all.add(hbChannel);
      hbAsyncStub = RaftServerProtocolServiceGrpc.newStub(hbChannel);
    } else {
      hbAsyncStub = null;
    }
    requestTimeoutDuration = requestTimeout;
    this.pool = connections == 1? null : newGrpcStubPool(target.getAddress(), sslContext, connections);
    this.allChannels = Collections.unmodifiableList(all);
  }

  GrpcStubPool<RaftServerProtocolServiceStub> newGrpcStubPool(String address, SslContext sslContext, int connections) {
    return new GrpcStubPool<>(connections, address, sslContext, RaftServerProtocolServiceGrpc::newStub, 16,
        eventLoopGroup);
  }

  private ManagedChannel buildChannel(RaftPeer target, int flowControlWindow, SslContext sslContext) {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forTarget(target.getAddress());
    // ignore any http proxy for grpc
    channelBuilder.proxyDetector(uri -> null);

    if (sslContext != null) {
      channelBuilder.useTransportSecurity().sslContext(sslContext);
    } else {
      channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
    }
    channelBuilder.disableRetry();
    if (eventLoopGroup != null) {
      channelBuilder.channelType(NettyUtils.getSocketChannelClass(eventLoopGroup))
          .eventLoopGroup(eventLoopGroup);
    }
    return channelBuilder.flowControlWindow(flowControlWindow).build();
  }

  @Override
  public void close() {
    LOG.info("{} Close channels", raftPeerId);
    GrpcUtil.shutdownManagedChannels(allChannels);
    if (pool != null) {
      pool.close();
    }
  }

  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) {
    // the StatusRuntimeException will be handled by the caller
    RequestVoteReplyProto r =
        blockingStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
            .requestVote(request);
    return r;
  }

  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) {
    StartLeaderElectionReplyProto r =
        blockingStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
            .startLeaderElection(request);
    return r;
  }

  void readIndex(ReadIndexRequestProto request, StreamObserver<ReadIndexReplyProto> s) {
    if (pool == null) {
      asyncStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
          .readIndex(request, s);
    } else {
      GrpcStubPool.Stub<RaftServerProtocolServiceStub> p;
      try {
        p = pool.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        s.onError(e);
        return;
      }
      p.getStub().withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
          .readIndex(request, new StreamObserver<ReadIndexReplyProto>() {
            @Override
            public void onNext(ReadIndexReplyProto v) {
              s.onNext(v);
            }

            @Override
            public void onError(Throwable t) {
              try {
                s.onError(t);
              } finally {
                p.release();
              }
            }

            @Override
            public void onCompleted() {
              try {
                s.onCompleted();
              } finally {
                p.release();
              }
            }
          });
    }
  }

  /**
   * Create a new appendEntries stream.
   * A heartbeat stream uses the heartbeat channel if there is a separate one.
   * Otherwise, the stream is created by {@link GrpcAppendEntriesChannels#newStream(StreamObserver)}.
   */
  CallStreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseHandler, boolean isHeartbeat) {
    if (isHeartbeat && hbAsyncStub != null) {
      return (CallStreamObserver<AppendEntriesRequestProto>) hbAsyncStub.appendEntries(responseHandler);
    } else {
      return appendEntriesStreams.newStream(responseHandler);
    }
  }

  GrpcAppendEntriesChannels getAppendEntriesStreamsForTesting() {
    return appendEntriesStreams;
  }

  ManagedChannel getMainChannelForTesting() {
    return channel;
  }

  StreamObserver<InstallSnapshotRequestProto> installSnapshot(
      String name, TimeDuration timeout, int limit, StreamObserver<InstallSnapshotReplyProto> responseHandler) {
    return StreamObserverWithTimeout.newInstance(name, ServerStringUtils::toInstallSnapshotRequestString,
        () -> timeout, limit, i -> asyncStub.withInterceptors(i).installSnapshot(responseHandler));
  }

  // short-circuit the backoff timer and make them reconnect immediately.
  public void resetConnectBackoff() {
    allChannels.forEach(ManagedChannel::resetConnectBackoff);
  }
}
