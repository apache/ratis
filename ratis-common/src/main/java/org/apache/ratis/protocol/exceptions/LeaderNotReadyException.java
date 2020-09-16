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
package org.apache.ratis.protocol.exceptions;

import org.apache.ratis.protocol.RaftGroupMemberId;

/**
 * This exception is sent from the server to a client. The server has just
 * become the current leader, but has not committed its first place-holder
 * log entry yet. Thus the leader cannot accept any new client requests since
 * it cannot determine whether a request is just a retry.
 */
public class LeaderNotReadyException extends ServerNotReadyException {
  private final RaftGroupMemberId serverId;

  public LeaderNotReadyException(RaftGroupMemberId id) {
    super(id + " is in LEADER state but not ready yet.");
    this.serverId = id;
  }

  public RaftGroupMemberId getServerId() {
    return serverId;
  }
}
