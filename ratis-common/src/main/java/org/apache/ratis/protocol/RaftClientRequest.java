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
package org.apache.ratis.protocol;

import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.util.Preconditions;

import java.util.Objects;

import static org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase.*;

/**
 * Request from client to server
 */
public class RaftClientRequest extends RaftClientMessage {
  private static final Type WRITE_DEFAULT = new Type(WriteRequestTypeProto.getDefaultInstance());
  private static final Type WRITE_ALL = new Type(
      WriteRequestTypeProto.newBuilder().setReplication(ReplicationLevel.ALL).build());

  private static final Type DEFAULT_READ = new Type(ReadRequestTypeProto.getDefaultInstance());
  private static final Type DEFAULT_STALE_READ = new Type(StaleReadRequestTypeProto.getDefaultInstance());

  public static Type writeRequestType(ReplicationLevel replication) {
    switch (replication) {
      case MAJORITY: return WRITE_DEFAULT;
      case ALL: return WRITE_ALL;
      default:
        throw new IllegalArgumentException("Unexpected replication: " + replication);
    }
  }

  public static Type readRequestType() {
    return DEFAULT_READ;
  }

  public static Type staleReadRequestType(long minIndex) {
    return minIndex == 0L? DEFAULT_STALE_READ
        : new Type(StaleReadRequestTypeProto.newBuilder().setMinIndex(minIndex).build());
  }

  public static Type watchRequestType(long index, ReplicationLevel replication) {
    return new Type(WatchRequestTypeProto.newBuilder().setIndex(index).setReplication(replication).build());
  }

  /** The type of a request (oneof write, read, staleRead, watch; see the message RaftClientRequestProto). */
  public static class Type {
    public static Type valueOf(WriteRequestTypeProto write) {
      return writeRequestType(write.getReplication());
    }

    public static Type valueOf(ReadRequestTypeProto read) {
      return DEFAULT_READ;
    }

    public static Type valueOf(StaleReadRequestTypeProto staleRead) {
      return staleRead.getMinIndex() == 0? DEFAULT_STALE_READ
          : new Type(staleRead);
    }

    public static Type valueOf(WatchRequestTypeProto watch) {
      return watchRequestType(watch.getIndex(), watch.getReplication());
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

    private Type(ReadRequestTypeProto read) {
      this(READ, read);
    }

    private Type(StaleReadRequestTypeProto staleRead) {
      this(STALEREAD, staleRead);
    }

    private Type(WatchRequestTypeProto watch) {
      this(WATCH, watch);
    }

    public boolean is(RaftClientRequestProto.TypeCase typeCase) {
      return getTypeCase().equals(typeCase);
    }

    public RaftClientRequestProto.TypeCase getTypeCase() {
      return typeCase;
    }

    public WriteRequestTypeProto getWrite() {
      Preconditions.assertTrue(is(WRITE));
      return (WriteRequestTypeProto)proto;
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

    static String toString(ReplicationLevel replication) {
      return replication == ReplicationLevel.MAJORITY? "": "-" + replication;
    }

    public static String toString(WatchRequestTypeProto w) {
      return "Watch" + toString(w.getReplication()) + "(" + w.getIndex() + ")";
    }

    @Override
    public String toString() {
      switch (typeCase) {
        case WRITE:
          return "RW" + toString(getWrite().getReplication());
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

  private final long callId;
  private final long seqNum;

  private final Message message;
  private final Type type;

  public RaftClientRequest(ClientId clientId, RaftPeerId serverId,
      RaftGroupId groupId, long callId) {
    this(clientId, serverId, groupId, callId, 0L, null, WRITE_DEFAULT);
  }

  public RaftClientRequest(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId,
      long callId, long seqNum, Message message, Type type) {
    super(clientId, serverId, groupId);
    this.callId = callId;
    this.seqNum = seqNum;
    this.message = message;
    this.type = type;
  }

  @Override
  public final boolean isRequest() {
    return true;
  }

  public long getCallId() {
    return callId;
  }

  public long getSeqNum() {
    return seqNum;
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

  @Override
  public String toString() {
    return super.toString() + ", cid=" + callId + ", seq=" + seqNum + " "
        + type + ", " + getMessage();
  }
}
