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
package org.apache.hadoop.raft.util;

import org.apache.hadoop.raft.RaftTestUtil;
import org.apache.hadoop.raft.server.RaftServer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Inject code to block particular servers receiving server requests. */
public class BlockReceiveServerRequest
    implements CodeInjectionForTesting.Code {
  private static final BlockReceiveServerRequest INSTANCE = new BlockReceiveServerRequest();

  static {
    CodeInjectionForTesting.put(RaftServer.REQUEST_VOTE, INSTANCE);
    CodeInjectionForTesting.put(RaftServer.APPEND_ENTRIES, INSTANCE);
  }

  static void block(final Boolean b) {
    if (b == null) {
      return;
    }
    try {
      RaftTestUtil.block(() -> b);
    } catch (InterruptedException e) {
      RaftUtils.toInterruptedIOException("", e);
    }
  }

  public static Map<String, Boolean> getSources() {
    return INSTANCE.sources;
  }

  public static Map<String, Boolean> getDestinations() {
    return INSTANCE.destinations;
  }

  private final Map<String, Boolean> sources = new ConcurrentHashMap<>();
  private final Map<String, Boolean> destinations = new ConcurrentHashMap<>();

  @Override
  public Object execute(Object... args) throws IOException {
    block(destinations.get(args[0]));
    block(sources.get(args[1]));
    return null;
  }

  private BlockReceiveServerRequest() {}
}
