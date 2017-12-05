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
package org.apache.ratis.examples.filestore;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.ExamplesProtos.*;
import org.apache.ratis.util.CheckedFunction;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/** A standalone server using raft with a configurable state machine. */
public class FileStoreClient implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(FileStoreClient.class);

  private final RaftClient client;

  public FileStoreClient(RaftGroup group, RaftProperties properties)
      throws IOException {
    this.client = RaftClient.newBuilder()
        .setProperties(properties)
        .setRaftGroup(group)
        .build();
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  static ByteString send(
      ByteString request, CheckedFunction<Message, RaftClientReply, IOException> sendFunction)
      throws IOException {
    final RaftClientReply reply = sendFunction.apply(() -> request);
    final StateMachineException sme = reply.getStateMachineException();
    if (sme != null) {
      throw new IOException("Failed to send request " + request, sme);
    }
    Preconditions.assertTrue(reply.isSuccess(), () -> "reply=" + reply);
    return reply.getMessage().getContent();
  }

  private ByteString send(ByteString request) throws IOException {
    return send(request, client::send);
  }

  private ByteString sendReadOnly(ByteString request) throws IOException {
    return send(request, client::sendReadOnly);
  }

  public ByteString read(String path, long offset, long length) throws IOException {
    return readImpl(path, offset, length).getData();
  }

  private ReadReplyProto readImpl(String path, long offset, long length) throws IOException {
    final ReadRequestProto read = ReadRequestProto.newBuilder()
        .setPath(ProtoUtils.toByteString(path))
        .setOffset(offset)
        .setLength(length)
        .build();

    return ReadReplyProto.parseFrom(sendReadOnly(read.toByteString()));
  }

  public long write(String path, long offset, boolean close, ByteBuffer buffer)
      throws IOException {
    final int chunkSize = FileStoreCommon.getChunkSize(buffer.remaining());
    buffer.limit(chunkSize);
    final WriteReplyProto proto = writeImpl(path, offset, close, ByteString.copyFrom(buffer));
    return proto.getLength();
  }

  private WriteReplyProto writeImpl(String path, long offset, boolean close, ByteString data)
      throws IOException {
    final WriteRequestHeaderProto.Builder header = WriteRequestHeaderProto.newBuilder()
        .setPath(ProtoUtils.toByteString(path))
        .setOffset(offset)
        .setClose(close);

    final WriteRequestProto.Builder write = WriteRequestProto.newBuilder()
        .setHeader(header)
        .setData(data);

    final FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setWrite(write).build();
    return WriteReplyProto.parseFrom(send(request.toByteString()));
  }

  private DeleteReplyProto deleteImpl(String path) throws IOException {
    final DeleteRequestProto.Builder delete = DeleteRequestProto.newBuilder()
        .setPath(ProtoUtils.toByteString(path));
    final FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setDelete(delete).build();
    return DeleteReplyProto.parseFrom(send(request.toByteString()));
  }

  public void delete(String path) throws IOException {
    deleteImpl(path);
  }
}
