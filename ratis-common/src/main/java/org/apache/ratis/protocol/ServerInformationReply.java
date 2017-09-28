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

/**
 * The response of server information request. Sent from server to client.
 *
 * TODO : currently, only information returned is the info of the group the
 * server belongs to.
 */
public class ServerInformationReply extends RaftClientReply {
  RaftGroup group;

  public ServerInformationReply(RaftClientRequest request, Message message,
      RaftGroup group) {
    super(request, message);
    this.group = group;
  }

  public ServerInformationReply(RaftClientRequest request,
      RaftException ex) {
    super(request, ex);
  }

  public RaftGroup getGroup() {
    return group;
  }

  public ServerInformationReply(ClientId clientId, RaftPeerId serverId,
      RaftGroupId groupId, long callId, boolean success, Message message,
      RaftException exception, RaftGroup group) {
    super(clientId, serverId, groupId, callId, success, message, exception);
    this.group = group;
  }
}
