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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos.SlidingWindowEntry;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.CallId;

/** Interface for testing raft client. */
public interface RaftClientTestUtil {
  static void assertAsyncRequestSemaphore(RaftClient client, int expectedAvailablePermits, int expectedQueueLength) {
    ((RaftClientImpl) client).getOrderedAsync().assertRequestSemaphore(expectedAvailablePermits, expectedQueueLength);
  }

  static ClientInvocationId getClientInvocationId(RaftClient client) {
    return ClientInvocationId.valueOf(client.getId(), CallId.get());
  }

  static RaftClientRequest newRaftClientRequest(RaftClient client, RaftPeerId server,
      long callId, Message message, RaftClientRequest.Type type, SlidingWindowEntry slidingWindowEntry) {
    return ((RaftClientImpl)client).newRaftClientRequest(server, callId, message, type, slidingWindowEntry);
  }
}
