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

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamPacket;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.util.JavaUtils;

/**
 * This is an abstract implementation of {@link DataStreamPacket}.
 *
 * This class is immutable.
 */
public abstract class DataStreamPacketImpl implements DataStreamPacket {
  private final ClientId clientId;
  private final Type type;
  private final long streamId;
  private final long streamOffset;

  public DataStreamPacketImpl(ClientId clientId, Type type, long streamId, long streamOffset) {
    this.clientId = clientId;
    this.type = type;
    this.streamId = streamId;
    this.streamOffset = streamOffset;
  }

  @Override
  public ClientId getClientId() {
    return clientId;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public long getStreamId() {
    return streamId;
  }

  @Override
  public long getStreamOffset() {
    return streamOffset;
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass())
        + ":clientId=" + getClientId()
        + ",type=" + getType()
        + ",id=" + getStreamId()
        + ",offset=" + getStreamOffset()
        + ",length=" + getDataLength();
  }
}
