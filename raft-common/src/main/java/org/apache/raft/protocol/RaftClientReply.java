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
package org.apache.raft.protocol;

public class RaftClientReply extends RaftRpcMessage.Reply {
  /** non-null if the server is not leader */
  private final NotLeaderException notLeaderException;

  public RaftClientReply(String requestorId, String replierId, boolean success,
      NotLeaderException notLeaderException) {
    super(requestorId, replierId, success);
    this.notLeaderException = notLeaderException;
  }

  public RaftClientReply(RaftClientRequest request, boolean success,
      NotLeaderException notLeaderException) {
    super(request.getRequestorId(), request.getReplierId(), success);
    this.notLeaderException = notLeaderException;
  }

  public NotLeaderException getNotLeaderException() {
    return notLeaderException;
  }

  public boolean isNotLeader() {
    return notLeaderException != null;
  }
}
