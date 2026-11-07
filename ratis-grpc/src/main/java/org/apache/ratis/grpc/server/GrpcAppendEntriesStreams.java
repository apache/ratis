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

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceStub;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * The appendEntries streams to a peer.
 * <p>
 * When there are no other channels, all the streams use the main channel,
 * which is shared with the other RPCs (requestVote, installSnapshot, etc.).
 * Otherwise, the streams use only the other channels, which are dedicated to appendEntries;
 * each stream uses one channel and the new streams take the channels in turn.
 * <p>
 * All the channels, including the other channels, are owned by the caller,
 * which is also responsible for shutting them down.
 */
class GrpcAppendEntriesStreams {
  // The main channel; null if the other channels are used for appendEntries.
  private final ManagedChannel mainChannel;
  private final RaftServerProtocolServiceStub mainStub;
  // The channels dedicated to appendEntries; null if the main channel is used for appendEntries.
  private final List<ManagedChannel> otherChannels;
  private final List<RaftServerProtocolServiceStub> otherStubs;
  // The index of the channel in otherChannels for the next stream
  private final AtomicInteger streamIndex = new AtomicInteger();

  /**
   * @param mainChannel   the main channel, used for appendEntries only if there are no other channels;
   *                      otherwise, it is ignored.
   * @param otherChannels the channels dedicated to appendEntries;
   *                      null means that there are no other channels, otherwise it must be non-empty.
   */
  GrpcAppendEntriesStreams(ManagedChannel mainChannel, List<ManagedChannel> otherChannels) {
    if (otherChannels == null) {
      this.mainChannel = mainChannel;
      this.mainStub = RaftServerProtocolServiceGrpc.newStub(mainChannel);
      this.otherChannels = null;
      this.otherStubs = null;
    } else {
      this.mainChannel = null;
      this.mainStub = null;
      Preconditions.assertTrue(!otherChannels.isEmpty(), "otherChannels is empty");
      this.otherChannels = Collections.unmodifiableList(otherChannels);
      this.otherStubs = Collections.unmodifiableList(otherChannels.stream()
          .map(RaftServerProtocolServiceGrpc::newStub)
          .collect(Collectors.toList()));
    }
  }

  /**
   * Create a new appendEntries stream.
   * Use the main channel if there are no other channels;
   * otherwise, use the next other channel.
   */
  CallStreamObserver<AppendEntriesRequestProto> newStream(StreamObserver<AppendEntriesReplyProto> responseHandler) {
    final RaftServerProtocolServiceStub stub = otherStubs != null ? otherStubs.get(nextStreamIndex()) : mainStub;
    return (CallStreamObserver<AppendEntriesRequestProto>) stub.appendEntries(responseHandler);
  }

  /**
   * This method must be called only when there are other channels.
   *
   * @return the index in the other channels of the channel for a new stream.
   */
  int nextStreamIndex() {
    Objects.requireNonNull(otherStubs, "otherStubs == null");
    return streamIndex.getAndUpdate(n -> (n + 1) % otherStubs.size());
  }

  List<ManagedChannel> getAppendEntriesChannelsForTesting() {
    return otherChannels != null ? otherChannels : Collections.singletonList(mainChannel);
  }

  List<ManagedChannel> getOtherChannelsForTesting() {
    return otherChannels;
  }
}
