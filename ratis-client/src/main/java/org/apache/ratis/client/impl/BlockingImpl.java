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
package org.apache.ratis.client.impl;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.client.retry.ClientRetryEvent;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.AlreadyExistsException;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Blocking api implementations. */
class BlockingImpl implements BlockingApi {
  static final Logger LOG = LoggerFactory.getLogger(BlockingImpl.class);

  private final RaftClientImpl client;

  BlockingImpl(RaftClientImpl client) {
    this.client = Objects.requireNonNull(client, "client == null");
  }

  @Override
  public RaftClientReply send(Message message) throws IOException {
    return send(RaftClientRequest.writeRequestType(), message, null);
  }

  @Override
  public RaftClientReply sendReadOnly(Message message) throws IOException {
    return send(RaftClientRequest.readRequestType(), message, null);
  }

  @Override
  public RaftClientReply sendStaleRead(Message message, long minIndex, RaftPeerId server)
      throws IOException {
    return send(RaftClientRequest.staleReadRequestType(minIndex), message, server);
  }

  @Override
  public RaftClientReply watch(long index, ReplicationLevel replication) throws IOException {
    return send(RaftClientRequest.watchRequestType(index, replication), null, null);
  }

  private RaftClientReply send(RaftClientRequest.Type type, Message message, RaftPeerId server)
      throws IOException {
    if (!type.is(TypeCase.WATCH)) {
      Objects.requireNonNull(message, "message == null");
    }

    final long callId = CallId.getAndIncrement();
    return sendRequestWithRetry(() -> client.newRaftClientRequest(server, callId, message, type, null));
  }

  RaftClientReply sendRequestWithRetry(Supplier<RaftClientRequest> supplier) throws IOException {
    RaftClientImpl.PendingClientRequest pending = new RaftClientImpl.PendingClientRequest() {
      @Override
      public RaftClientRequest newRequestImpl() {
        return supplier.get();
      }
    };
    while (true) {
      final RaftClientRequest request = pending.newRequest();
      IOException ioe = null;
      try {
        final RaftClientReply reply = sendRequest(request);

        if (reply != null) {
          return client.handleReply(request, reply);
        }
      } catch (GroupMismatchException | StateMachineException | TransferLeadershipException |
          LeaderSteppingDownException | AlreadyClosedException | AlreadyExistsException e) {
        throw e;
      } catch (IOException e) {
        ioe = e;
      }

      pending.incrementExceptionCount(ioe);
      ClientRetryEvent event = new ClientRetryEvent(request, ioe, pending);
      final RetryPolicy retryPolicy = client.getRetryPolicy();
      final RetryPolicy.Action action = retryPolicy.handleAttemptFailure(event);
      TimeDuration sleepTime = client.getEffectiveSleepTime(ioe, action.getSleepTime());

      if (!action.shouldRetry()) {
        throw (IOException)client.noMoreRetries(event);
      }

      try {
        sleepTime.sleep();
      } catch (InterruptedException e) {
        throw new InterruptedIOException("retry policy=" + retryPolicy);
      }
    }
  }

  private RaftClientReply sendRequest(RaftClientRequest request) throws IOException {
    LOG.debug("{}: send {}", client.getId(), request);
    RaftClientReply reply;
    try {
      reply = client.getClientRpc().sendRequest(request);
    } catch (GroupMismatchException gme) {
      throw gme;
    } catch (IOException ioe) {
      client.handleIOException(request, ioe);
      throw ioe;
    }
    LOG.debug("{}: receive {}", client.getId(), reply);
    reply = client.handleLeaderException(request, reply);
    reply = RaftClientImpl.handleRaftException(reply, Function.identity());
    return reply;
  }
}
