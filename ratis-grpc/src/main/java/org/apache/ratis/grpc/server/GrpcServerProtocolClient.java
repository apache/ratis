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
package org.apache.ratis.grpc.server;

import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcSslContexts;
import org.apache.ratis.thirdparty.io.grpc.netty.NegotiationType;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceBlockingStub;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceStub;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * This is a RaftClient implementation that supports streaming data to the raft
 * ring. The stream implementation utilizes gRPC.
 */
public class GrpcServerProtocolClient implements Closeable {
  // Common channel
  private final ManagedChannel channel;
  // Channel and stub for heartbeat
  private final ManagedChannel hbChannel;
  private final RaftServerProtocolServiceStub hbAsyncStub;
  private final RaftServerProtocolServiceStub asyncStub;
  private final RaftServerProtocolServiceBlockingStub blockingStub;

  private final TimeDuration requestTimeoutDuration;
  private static final Logger LOG = LoggerFactory.getLogger(GrpcServerProtocolClient.class);
  //visible for using in log / error messages AND to use in instrumented tests
  private final RaftPeerId raftPeerId;

  public GrpcServerProtocolClient(RaftPeer target, int flowControlWindow,
      TimeDuration requestTimeout, GrpcTlsConfig tlsConfig) {
    raftPeerId = target.getId();
    LOG.info("Build channel for {}", raftPeerId);
    channel = buildChannel(target, flowControlWindow, tlsConfig);
    hbChannel = buildChannel(target, flowControlWindow, tlsConfig);
    blockingStub = RaftServerProtocolServiceGrpc.newBlockingStub(channel);
    asyncStub = RaftServerProtocolServiceGrpc.newStub(channel);
    hbAsyncStub = RaftServerProtocolServiceGrpc.newStub(hbChannel);
    requestTimeoutDuration = requestTimeout;
  }

  private ManagedChannel buildChannel(RaftPeer target, int flowControlWindow,
      GrpcTlsConfig tlsConfig) {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forTarget(target.getAddress());

    if (tlsConfig!= null) {
      SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
      if (tlsConfig.isFileBasedConfig()) {
        sslContextBuilder.trustManager(tlsConfig.getTrustStoreFile());
      } else {
        sslContextBuilder.trustManager(tlsConfig.getTrustStore());
      }
      if (tlsConfig.getMtlsEnabled()) {
        if (tlsConfig.isFileBasedConfig()) {
          sslContextBuilder.keyManager(tlsConfig.getCertChainFile(),
              tlsConfig.getPrivateKeyFile());
        } else {
          sslContextBuilder.keyManager(tlsConfig.getPrivateKey(),
              tlsConfig.getCertChain());
        }
      }
      try {
        channelBuilder.useTransportSecurity().sslContext(sslContextBuilder.build());
      } catch (Exception ex) {
        throw new IllegalArgumentException("Failed to build SslContext, peerId=" + raftPeerId
            + ", tlsConfig=" + tlsConfig, ex);
      }
    } else {
      channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
    }
    return channelBuilder.flowControlWindow(flowControlWindow).build();
  }

  @Override
  public void close() {
    LOG.info("Close channel for {}", raftPeerId);
    GrpcUtil.shutdownManagedChannel(hbChannel);
    GrpcUtil.shutdownManagedChannel(channel);
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

  StreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseHandler, boolean isHeartbeat) {
    if (isHeartbeat) {
      return hbAsyncStub.appendEntries(responseHandler);
    } else {
      return asyncStub.appendEntries(responseHandler);
    }
  }

  StreamObserver<InstallSnapshotRequestProto> installSnapshot(
      StreamObserver<InstallSnapshotReplyProto> responseHandler) {
    return asyncStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
        .installSnapshot(responseHandler);
  }

  // short-circuit the backoff timer and make them reconnect immediately.
  public void resetConnectBackoff() {
    hbChannel.resetConnectBackoff();
    channel.resetConnectBackoff();
  }
}
