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

import org.apache.hadoop.raft.server.protocol.RaftServerRequest;
import org.apache.hadoop.raft.server.protocol.RaftServerResponse;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

class EventQueue {
  private final BlockingQueue<RaftServerRequest> requestQueue;
  private final Map<RaftServerRequest, RaftServerResponse> responseMap;

  EventQueue() {
    this.requestQueue = new LinkedBlockingQueue<>();
    this.responseMap = new ConcurrentHashMap<>();
  }

  RaftServerResponse request(RaftServerRequest request)
      throws InterruptedException {
    requestQueue.add(request);
    synchronized (this) {
      while (!responseMap.containsKey(request)) {
        this.wait();
      }
    }
    return responseMap.remove(request);
  }

  RaftServerRequest takeRequest() throws InterruptedException {
    return requestQueue.take();
  }

  void response(RaftServerRequest request, RaftServerResponse response) {
    responseMap.put(request, response);
    synchronized (this) {
      this.notifyAll();
    }
  }
}
