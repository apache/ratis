/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.datastream.impl;

import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamPacket;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamReplyHeader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Implements {@link DataStreamReply} with multiple {@link ByteBuffer}s.
 *
 * This class is immutable.
 */
public final class DataStreamReplyByteBuffers extends DataStreamPacketImpl implements DataStreamReply {
  public static final class Builder {
    private ClientId clientId;
    private Type type;
    private long streamId;
    private long streamOffset;
    private Iterable<ByteBuffer> buffers;

    private boolean success;
    private long bytesWritten;
    private Collection<CommitInfoProto> commitInfos;

    private Builder() {
    }

    public Builder setClientId(ClientId clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setType(Type type) {
      this.type = type;
      return this;
    }

    public Builder setStreamId(long streamId) {
      this.streamId = streamId;
      return this;
    }

    public Builder setStreamOffset(long streamOffset) {
      this.streamOffset = streamOffset;
      return this;
    }

    public Builder setBuffers(Iterable<ByteBuffer> buffers) {
      this.buffers = buffers;
      return this;
    }

    public Builder setSuccess(boolean success) {
      this.success = success;
      return this;
    }

    public Builder setBytesWritten(long bytesWritten) {
      this.bytesWritten = bytesWritten;
      return this;
    }

    public Builder setCommitInfos(Collection<CommitInfoProto> commitInfos) {
      this.commitInfos = commitInfos;
      return this;
    }

    public Builder setDataStreamReplyHeader(DataStreamReplyHeader header) {
      return setDataStreamPacket(header)
          .setSuccess(header.isSuccess())
          .setBytesWritten(header.getBytesWritten())
          .setCommitInfos(header.getCommitInfos());
    }

    public Builder setDataStreamPacket(DataStreamPacket packet) {
      return setClientId(packet.getClientId())
          .setType(packet.getType())
          .setStreamId(packet.getStreamId())
          .setStreamOffset(packet.getStreamOffset());
    }

    public DataStreamReplyByteBuffers build() {
      return new DataStreamReplyByteBuffers(clientId, type, streamId,
          streamOffset, buffers, success, bytesWritten, commitInfos);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final List<ByteBuffer> buffers;
  private final long dataLength;
  private final boolean success;
  private final long bytesWritten;
  private final Collection<CommitInfoProto> commitInfos;

  @SuppressWarnings("parameternumber")
  private DataStreamReplyByteBuffers(ClientId clientId, Type type,
      long streamId, long streamOffset, Iterable<ByteBuffer> buffers,
      boolean success, long bytesWritten,
      Collection<CommitInfoProto> commitInfos) {
    super(clientId, type, streamId, streamOffset);
    final List<ByteBuffer> readOnlyBuffers = new ArrayList<>();
    long length = 0;
    if (buffers != null) {
      for (ByteBuffer buffer : buffers) {
        final ByteBuffer readOnly = buffer.asReadOnlyBuffer();
        readOnlyBuffers.add(readOnly);
        length += readOnly.remaining();
      }
    }
    this.buffers = Collections.unmodifiableList(readOnlyBuffers);
    this.dataLength = length;
    this.success = success;
    this.bytesWritten = bytesWritten;
    this.commitInfos = commitInfos != null ? commitInfos
        : Collections.emptyList();
  }

  @Override
  public long getDataLength() {
    return dataLength;
  }

  public List<ByteBuffer> slices() {
    final List<ByteBuffer> slices = new ArrayList<>(buffers.size());
    for (ByteBuffer buffer : buffers) {
      slices.add(buffer.slice());
    }
    return slices;
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  public Collection<CommitInfoProto> getCommitInfos() {
    return commitInfos;
  }

  @Override
  public String toString() {
    return super.toString()
        + "," + (success ? "SUCCESS" : "FAILED")
        + ",bytesWritten=" + bytesWritten;
  }
}
