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
package org.apache.ratis.datastream.impl;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamPacket;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

@SuppressWarnings("checkstyle:HiddenField")
abstract class DataStreamReplyBuilder<B extends DataStreamReplyBuilder<B>> {
  private ClientId clientId;
  private RaftProtos.DataStreamPacketHeaderProto.Type type;
  private long streamId;
  private long streamOffset;

  private boolean success;
  private long bytesWritten;
  private Collection<RaftProtos.CommitInfoProto> commitInfos = Collections.emptyList();

  abstract B getThis();

  public final ClientId getClientId() {
    return clientId;
  }

  public final B setClientId(ClientId clientId) {
    this.clientId = clientId;
    return getThis();
  }

  public final RaftProtos.DataStreamPacketHeaderProto.Type getType() {
    return type;
  }

  public final B setType(RaftProtos.DataStreamPacketHeaderProto.Type type) {
    this.type = type;
    return getThis();
  }

  public final long getStreamId() {
    return streamId;
  }

  public final B setStreamId(long streamId) {
    this.streamId = streamId;
    return getThis();
  }

  public final long getStreamOffset() {
    return streamOffset;
  }

  public final B setStreamOffset(long streamOffset) {
    this.streamOffset = streamOffset;
    return getThis();
  }

  public final boolean isSuccess() {
    return success;
  }

  public final B setSuccess(boolean success) {
    this.success = success;
    return getThis();
  }

  public final long getBytesWritten() {
    return bytesWritten;
  }

  public final B setBytesWritten(long bytesWritten) {
    this.bytesWritten = bytesWritten;
    return getThis();
  }

  public final Collection<RaftProtos.CommitInfoProto> getCommitInfos() {
    return commitInfos;
  }

  public final B setCommitInfos(Collection<RaftProtos.CommitInfoProto> commitInfos) {
    this.commitInfos = commitInfos != null
        ? Collections.unmodifiableCollection(new ArrayList<>(commitInfos))
        : Collections.emptyList();
    return getThis();
  }

  public final B setDataStreamPacket(DataStreamPacket packet) {
    return setClientId(packet.getClientId())
        .setType(packet.getType())
        .setStreamId(packet.getStreamId())
        .setStreamOffset(packet.getStreamOffset());
  }
}
