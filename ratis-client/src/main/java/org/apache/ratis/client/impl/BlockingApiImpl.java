package org.apache.ratis.client.impl;

import java.io.IOException;
import java.util.Objects;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;

class BlockingApiImpl implements BlockingApi {
  private final RaftClientImpl client;

  BlockingApiImpl(RaftClientImpl client) {
    this.client = Objects.requireNonNull(client, "client == null");
  }

  @Override
  public RaftClientReply send(Message message) throws IOException {
    return client.send(RaftClientRequest.writeRequestType(), message, null);
  }

  @Override
  public RaftClientReply sendReadOnly(Message message) throws IOException {
    return client.send(RaftClientRequest.readRequestType(), message, null);
  }

  @Override
  public RaftClientReply sendStaleRead(Message message, long minIndex, RaftPeerId server)
      throws IOException {
    return client.send(RaftClientRequest.staleReadRequestType(minIndex), message, server);
  }

  @Override
  public RaftClientReply sendWatch(long index, ReplicationLevel replication) throws IOException {
    return client.send(RaftClientRequest.watchRequestType(index, replication), null, null);
  }
}
