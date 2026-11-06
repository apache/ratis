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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestWatchRequests {
  @Test
  public void testQueueForEveryReplicationLevel() {
    final RetryCache.Statistics statistics = Mockito.mock(RetryCache.Statistics.class);
    final RaftServerMetricsImpl metrics = new RaftServerMetricsImpl(
        RaftServerTestUtil.TEST_MEMBER_ID, peer -> 0L, () -> statistics);
    final WatchRequests requests = new WatchRequests("test", new RaftProperties(), metrics);
    try {
      for (ReplicationLevel level : ReplicationLevel.values()) {
        requests.update(level, 7);
        requests.update(level, 6);
        if (level == ReplicationLevel.UNRECOGNIZED) {
          continue;
        }
        final RaftClientRequest request = RaftClientRequest.newBuilder()
            .setClientId(ClientId.randomId())
            .setServerId(RaftPeerId.valueOf("s0"))
            .setGroupId(RaftGroupId.randomId())
            .setCallId(1)
            .setType(RaftClientRequest.watchRequestType(7, level))
            .build();
        Assertions.assertEquals(7L, requests.add(request).join().longValue());
      }
    } finally {
      requests.close();
      metrics.unregister();
    }
  }
}
