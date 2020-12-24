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

import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;

import java.util.Objects;

import static org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase.*;

/**
 * Request from client to server
 */
public class RaftClientRequest extends RaftClientMessage {
  private static final Type DATA_STREAM_DEFAULT = new Type(DataStreamRequestTypeProto.getDefaultInstance());
  private static final Type FORWARD_DEFAULT = new Type(ForwardRequestTypeProto.getDefaultInstance());
  private static final Type WRITE_DEFAULT = new Type(WriteRequestTypeProto.getDefaultInstance());
  private static final Type WATCH_DEFAULT = new Type(
      WatchRequestTypeProto.newBuilder().setIndex(0L).setReplication(ReplicationLevel.MAJORITY).build());

  private static final Type READ_DEFAULT = new Type(ReadRequestTypeProto.getDefaultInstance());
  private static final Type STALE_READ_DEFAULT = new Type(StaleReadRequestTypeProto.getDefaultInstance());

  public static Type writeRequestType() {
    return WRITE_DEFAULT;
  }

  public static Type dataStreamRequestType() {
    return DATA_STREAM_DEFAULT;
  }

  public static Type forwardRequestType() {
    return FORWARD_DEFAULT;
  }

  public static Type messageStreamRequestType(long streamId, long messageId, boolean endOfRequest) {
    return new Type(MessageStreamRequestTypeProto.newBuilder()
        .setStreamId(streamId)
        .setMessageId(messageId)
        .setEndOfRequest(endOfRequest)
        .build());
  }

  public static Type readRequestType() {
    return READ_DEFAULT;
  }

  public static Type staleReadRequestType(long minIndex) {
    return minIndex == 0L? STALE_READ_DEFAULT
        : new Type(StaleReadRequestTypeProto.newBuilder().setMinIndex(minIndex).build());
  }

  public static Type watchRequestType() {
    return WATCH_DEFAULT;
  }
  public static Type watchRequestType(long index, ReplicationLevel replication) {
    return new Type(WatchRequestTypeProto.newBuilder().setIndex(index).setReplication(replication).build());
  }

  /** The type of a request (oneof write, read, staleRead, watch; see the message RaftClientRequestProto). */
  public static final class Type {
    public static Type valueOf(WriteRequestTypeProto write) {
      return WRITE_DEFAULT;
    }

    public static Type valueOf(DataStreamRequestTypeProto dataStream) {
      return DATA_STREAM_DEFAULT;
    }

    public static Type valueOf(ForwardRequestTypeProto forward) {
      return FORWARD_DEFAULT;
    }

    public static Type valueOf(ReadRequestTypeProto read) {
      return READ_DEFAULT;
    }

    public static Type valueOf(StaleReadRequestTypeProto staleRead) {
      return staleRead.getMinIndex() == 0? STALE_READ_DEFAULT: new Type(staleRead);
    }

    public static Type valueOf(WatchRequestTypeProto watch) {
      return watchRequestType(watch.getIndex(), watch.getReplication());
    }

    public static Type valueOf(MessageStreamRequestTypeProto messageStream) {
      return messageStreamRequestType(
          messageStream.getStreamId(), messageStream.getMessageId(), messageStream.getEndOfRequest());
    }

    /**
     * The type case of the proto.
     * Only the corresponding proto (must be non-null) is used.
     * The other protos are ignored.
     */
    private final RaftClientRequestProto.TypeCase typeCase;
    private final Object proto;

    private Type(RaftClientRequestProto.TypeCase typeCase, Object proto) {
      this.typeCase = Objects.requireNonNull(typeCase, "typeCase == null");
      this.proto = Objects.requireNonNull(proto, "proto == null");
    }

    private Type(WriteRequestTypeProto write) {
      this(WRITE, write);
    }

    private Type(DataStreamRequestTypeProto dataStream) {
      this(DATASTREAM, dataStream);
    }

    private Type(ForwardRequestTypeProto forward) {
      this(FORWARD, forward);
    }

    private Type(MessageStreamRequestTypeProto messageStream) {
      this(MESSAGESTREAM, messageStream);
    }

    private Type(ReadRequestTypeProto read) {
      this(READ, read);
    }

    private Type(StaleReadRequestTypeProto staleRead) {
      this(STALEREAD, staleRead);
    }

    private Type(WatchRequestTypeProto watch) {
      this(WATCH, watch);
    }

    public boolean is(RaftClientRequestProto.TypeCase tCase) {
      return getTypeCase().equals(tCase);
    }

    public RaftClientRequestProto.TypeCase getTypeCase() {
      return typeCase;
    }

    public WriteRequestTypeProto getWrite() {
      Preconditions.assertTrue(is(WRITE));
      return (WriteRequestTypeProto)proto;
    }

    public DataStreamRequestTypeProto getDataStream() {
      Preconditions.assertTrue(is(DATASTREAM));
      return (DataStreamRequestTypeProto)proto;
    }

    public ForwardRequestTypeProto getForward() {
      Preconditions.assertTrue(is(FORWARD));
      return (ForwardRequestTypeProto)proto;
    }

    public MessageStreamRequestTypeProto getMessageStream() {
      Preconditions.assertTrue(is(MESSAGESTREAM), () -> "proto = " + proto);
      return (MessageStreamRequestTypeProto)proto;
    }

    public ReadRequestTypeProto getRead() {
      Preconditions.assertTrue(is(READ));
      return (ReadRequestTypeProto)proto;
    }

    public StaleReadRequestTypeProto getStaleRead() {
      Preconditions.assertTrue(is(STALEREAD));
      return (StaleReadRequestTypeProto)proto;
    }

    public WatchRequestTypeProto getWatch() {
      Preconditions.assertTrue(is(WATCH));
      return (WatchRequestTypeProto)proto;
    }

    public static String toString(ReplicationLevel replication) {
      return replication == ReplicationLevel.MAJORITY? "": "-" + replication;
    }

    public static String toString(WatchRequestTypeProto w) {
      return "Watch" + toString(w.getReplication()) + "(" + w.getIndex() + ")";
    }

    public static String toString(MessageStreamRequestTypeProto s) {
      return "MessageStream" + s.getStreamId() + "-" + s.getMessageId() + (s.getEndOfRequest()? "-eor": "");
    }

    @Override
    public String toString() {
      switch (typeCase) {
        case WRITE:
          return "RW";
        case DATASTREAM:
          return "DataStream";
        case FORWARD:
          return "Forward";
        case MESSAGESTREAM:
          return toString(getMessageStream());
        case READ:
          return "RO";
        case STALEREAD:
          return "StaleRead(" + getStaleRead().getMinIndex() + ")";
        case WATCH:
          return toString(getWatch());
        default:
          throw new IllegalStateException("Unexpected request type: " + typeCase);
      }
    }
  }

  /**
   * To build {@link RaftClientRequest}
   */
  public static class Builder {
    private ClientId clientId;
    private RaftPeerId serverId;
    private RaftGroupId groupId;
    private long callId;

    private Message message;
    private Type type;
    private SlidingWindowEntry slidingWindowEntry;
    private RoutingTable routingTable;
    private long timeoutMs;

    public RaftClientRequest build() {
      return new RaftClientRequest(
          clientId, serverId, groupId, callId, message, type, slidingWindowEntry, routingTable, timeoutMs);
    }

    public Builder setClientId(ClientId clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setServerId(RaftPeerId serverId) {
      this.serverId = serverId;
      return this;
    }

    public Builder setGroupId(RaftGroupId groupId) {
      this.groupId = groupId;
      return this;
    }

    public Builder setCallId(long callId) {
      this.callId = callId;
      return this;
    }

    public Builder setMessage(Message message) {
      this.message = message;
      return this;
    }

    public Builder setType(Type type) {
      this.type = type;
      return this;
    }

    public Builder setSlidingWindowEntry(SlidingWindowEntry slidingWindowEntry) {
      this.slidingWindowEntry = slidingWindowEntry;
      return this;
    }

    public Builder setRoutingTable(RoutingTable routingTable) {
      this.routingTable = routingTable;
      return this;
    }

    public Builder setTimeoutMs(long timeoutMs) {
      this.timeoutMs = timeoutMs;
      return this;
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** Convert the given request to a write request with the given message. */
  public static RaftClientRequest toWriteRequest(RaftClientRequest r, Message message) {
    return RaftClientRequest.newBuilder()
        .setClientId(r.getClientId())
        .setServerId(r.getServerId())
        .setGroupId(r.getRaftGroupId())
        .setCallId(r.getCallId())
        .setMessage(message)
        .setType(RaftClientRequest.writeRequestType())
        .setSlidingWindowEntry(r.getSlidingWindowEntry())
        .build();
  }

  private final Message message;
  private final Type type;

  private final SlidingWindowEntry slidingWindowEntry;

  private final RoutingTable routingTable;

  private final long timeoutMs;

  protected RaftClientRequest(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId, Type type) {
    this(clientId, serverId, groupId, callId, null, type, null, null, 0);
  }

  protected RaftClientRequest(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId, Type type,
      long timeoutMs) {
    this(clientId, serverId, groupId, callId, null, type, null, null, timeoutMs);
  }

  @SuppressWarnings("parameternumber")
  private RaftClientRequest(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId,
      long callId, Message message, Type type, SlidingWindowEntry slidingWindowEntry,
      RoutingTable routingTable, long timeoutMs) {
    super(clientId, serverId, groupId, callId);
    this.message = message;
    this.type = type;
    this.slidingWindowEntry = slidingWindowEntry != null? slidingWindowEntry: SlidingWindowEntry.getDefaultInstance();
    this.routingTable = routingTable;
    this.timeoutMs = timeoutMs;
  }

  @Override
  public final boolean isRequest() {
    return true;
  }

  public SlidingWindowEntry getSlidingWindowEntry() {
    return slidingWindowEntry;
  }

  public Message getMessage() {
    return message;
  }

  public Type getType() {
    return type;
  }

  public boolean is(RaftClientRequestProto.TypeCase typeCase) {
    return getType().is(typeCase);
  }

  public RoutingTable getRoutingTable() {
    return routingTable;
  }

  public long getTimeoutMs() {
    return timeoutMs;
  }

  @Override
  public String toString() {
    return super.toString() + ", seq=" + ProtoUtils.toString(slidingWindowEntry) + ", "
        + type + ", " + getMessage();
  }
}
