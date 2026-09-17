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

import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamReplyHeader;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * Implements {@link DataStreamReply} with {@link ByteBuffer}.
 * <p>
 * This class is immutable.
 */
public final class DataStreamReplyByteBuffer extends DataStreamPacketByteBuffer implements DataStreamReply {
  public static final class Builder extends DataStreamReplyBuilder<Builder> {
    private ByteBuffer buffer;

    private Builder() {}

    @Override
    Builder getThis() {
      return this;
    }

    public Builder setDataStreamReplyHeader(DataStreamReplyHeader header) {
      return setDataStreamPacket(header)
          .setSuccess(header.isSuccess())
          .setBytesWritten(header.getBytesWritten())
          .setCommitInfos(header.getCommitInfos());
    }

    public Builder setBuffer(ByteBuffer buffer) {
      this.buffer = buffer;
      return getThis();
    }

    public DataStreamReplyByteBuffer build() {
      return new DataStreamReplyByteBuffer(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final boolean success;
  private final long bytesWritten;
  private final Collection<CommitInfoProto> commitInfos;

  private DataStreamReplyByteBuffer(Builder b) {
    super(b.getClientId(), b.getType(), b.getStreamId(), b.getStreamOffset(), b.buffer);

    this.success = b.isSuccess();
    this.bytesWritten = b.getBytesWritten();
    this.commitInfos = b.getCommitInfos();
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
        + "," + (success? "SUCCESS": "FAILED")
        + ",bytesWritten=" + bytesWritten;
  }
}
