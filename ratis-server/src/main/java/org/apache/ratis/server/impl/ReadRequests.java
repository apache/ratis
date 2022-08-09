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
package org.apache.ratis.server.impl;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.server.leader.LogAppender;

/** For supporting linearizable read. */
class ReadRequests {
  /** The acknowledgement from a {@link LogAppender} of a heartbeat for a particular call id. */
  static class HeartbeatAck {
    private final LogAppender appender;
    private final long minCallId;
    private volatile boolean acknowledged = false;

    HeartbeatAck(LogAppender appender) {
      this.appender = appender;
      this.minCallId = appender.getCallId();
    }

    /** Is the heartbeat (for a particular call id) acknowledged? */
    boolean isAcknowledged() {
      return acknowledged;
    }

    /**
     * @return true if the acknowledged state is changed from false to true;
     *         otherwise, the acknowledged state remains unchanged, return false.
     */
    boolean receive(AppendEntriesReplyProto reply) {
      if (acknowledged) {
        return false;
      }
      synchronized (this) {
        if (!acknowledged && isValid(reply)) {
          acknowledged = true;
          return true;
        }
        return false;
      }
    }

    private boolean isValid(AppendEntriesReplyProto reply) {
      if (reply == null || !reply.getServerReply().getSuccess()) {
        return false;
      }
      // valid only if the reply has a later call id than the min.
      return appender.getCallIdComparator().compare(reply.getServerReply().getCallId(), minCallId) >= 0;
    }
  }
}
