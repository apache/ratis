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
package org.apache.hadoop.raft.server.simulation;

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.server.RaftConfiguration;
import org.apache.hadoop.raft.server.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerRequest;
import org.apache.hadoop.raft.server.protocol.RaftServerResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EventQueues {
  private final Map<String, EventQueue> queues;

  public EventQueues(RaftConfiguration conf) {
    Map<String, EventQueue> map = new HashMap<>();
    for (RaftPeer peer : conf.getPeers()) {
      map.put(peer.getId(), new EventQueue());
    }
    queues = Collections.unmodifiableMap(map);
  }

  public RaftServerResponse request(RaftServerRequest request)
      throws InterruptedException {
    EventQueue q = queues.get(request.getToId());
    return q.request(request);
  }

  public RaftServerRequest takeRequest(String peerId)
      throws InterruptedException {
    return queues.get(peerId).takeRequest();
  }

  public void response(RaftServerRequest request, RaftServerResponse response) {
    Preconditions.checkArgument(request.getToId().equals(response.getPeerId()));
    queues.get(request.getToId()).response(request, response);
  }
}
