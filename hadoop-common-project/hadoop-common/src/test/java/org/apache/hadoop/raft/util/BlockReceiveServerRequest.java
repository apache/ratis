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

import org.apache.hadoop.raft.server.RaftServer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Inject code to block particular servers receiving server requests. */
public class BlockReceiveServerRequest implements CodeInjectionForTesting.Code {
  private static final BlockReceiveServerRequest INSTANCE = new BlockReceiveServerRequest();
  static {
    CodeInjectionForTesting.put(RaftServer.REQUEST_VOTE, INSTANCE);
    CodeInjectionForTesting.put(RaftServer.APPEND_ENTRIES, INSTANCE);
  }

  private static final Map<String, BlockForTesting> blocks = new ConcurrentHashMap<>();

  public static void clear() {
    blocks.clear();
  }

  public static void setBlocked(String id, boolean block) {
    BlockForTesting b = blocks.get(id);
    if (b == null) {
      blocks.put(id, b = new BlockForTesting());
    }
    b.setBlocked(block);
  }

  private BlockReceiveServerRequest() {}

  @Override
  public Object execute(Object... args) throws IOException {
    final Object id = args[0];
    final BlockForTesting b = blocks.get(id);
    if (b == null) {
      return null;
    }
    LOG.info(id + ": " + b + ", args=" + Arrays.toString(args));
    try {
      b.blockForTesting();
    } catch (InterruptedException e) {
      RaftUtils.toInterruptedIOException("", e);
    }
    return null;
  }
}
