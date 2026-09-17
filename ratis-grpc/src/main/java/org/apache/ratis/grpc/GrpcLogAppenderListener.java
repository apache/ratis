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
package org.apache.ratis.grpc;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;

/**
 * Observes a single peer log appender. Callbacks run on Ratis threads, may be concurrent and may
 * hold an appender lock. They must not block or retain request payloads. Exceptions are isolated
 * from replication. Callbacks describe lifecycle activity, not application-level outcomes:
 * consumers are responsible for correlating requests, handling racing terminal notifications,
 * and filtering messages. This interface currently observes AppendEntries, not InstallSnapshot.
 * Append request registration, client reset and reply inconsistency callbacks are serialized per
 * appender. Replies and other terminal callbacks may race with these callbacks. Consumers must
 * handle these races; no exactly-once terminal notification is guaranteed.
 */
public interface GrpcLogAppenderListener {
  /** Creates a separate listener for each appender, including after leadership changes. */
  @FunctionalInterface
  interface Factory {
    /** @return the listener, or null to disable observation for this appender. */
    GrpcLogAppenderListener create(RaftGroupMemberId source, RaftPeer destination);
  }

  /** @return the AppendEntries listener, or null to disable its callbacks. Called once per appender. */
  default AppendEntries appendEntries() {
    return null;
  }

  /** Observes append attempts and their response streams, including the separate heartbeat stream. */
  interface AppendEntries {
    /** An append attempt is registered, before establishing or writing its stream. */
    default void onRequest(AppendEntriesRequestProto request) { }

    /** A response was received, possibly after its request timed out or was invalidated. */
    default void onReply(AppendEntriesReplyProto reply) { }

    /**
     * An INCONSISTENCY reply is being handled and pending append requests are about to be cleared.
     * Called after onReply for that reply, with the appender write lock held, without resetting the client.
     */
    default void onReplyInconsistency() { }

    /** A local send error occurred; a later stream notification may follow. */
    default void onFailure(long callId, Throwable error) { }

    /** A pending request timed out. */
    default void onTimeout(long callId) { }

    /** A response stream completed, including after the appender stopped. */
    default void onCompleted() { }

    /** A response stream failed, including after the appender stopped. */
    default void onError(Throwable error) { }
  }

  /**
   * The client is about to be reset, which may invalidate pending append attempts.
   * The reason is diagnostic text, not a stable identifier; error may be null.
   */
  default void onResetClient(String reason, Throwable error) { }

  /** The appender run loop exited, normally or exceptionally. Pending attempts may remain. */
  default void onNotRunning() { }
}
