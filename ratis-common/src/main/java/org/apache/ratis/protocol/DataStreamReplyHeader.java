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

package org.apache.ratis.protocol;

import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;

import java.util.Collection;
import java.util.Collections;

/** The header format is {@link DataStreamPacketHeader}, bytesWritten and flags. */
public class DataStreamReplyHeader extends DataStreamPacketHeader implements DataStreamReply {
  private final long bytesWritten;
  private final boolean success;
  private final Collection<CommitInfoProto> commitInfos;

  @SuppressWarnings("parameternumber")
  public DataStreamReplyHeader(ClientId clientId, Type type, long streamId, long streamOffset, long dataLength,
      long bytesWritten, boolean success, Collection<CommitInfoProto> commitInfos) {
    super(clientId, type, streamId, streamOffset, dataLength);
    this.bytesWritten = bytesWritten;
    this.success = success;
    this.commitInfos = commitInfos != null? commitInfos: Collections.emptyList();
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  public boolean isSuccess() {
    return success;
  }

  @Override
  public Collection<CommitInfoProto> getCommitInfos() {
    return commitInfos;
  }
}