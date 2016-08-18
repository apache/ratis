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
package org.apache.raft.hadoopRpc;

import org.apache.raft.RaftTestUtil;
import org.apache.raft.hadoopRpc.server.HadoopRpcService;
import org.apache.raft.util.CodeInjectionForTesting;
import org.apache.raft.util.RaftUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Inject code to delay particular servers sending out server requests. */
public class DelaySendServerRequest
    implements CodeInjectionForTesting.Code {
  private static final DelaySendServerRequest INSTANCE = new DelaySendServerRequest();
  static {
    CodeInjectionForTesting.put(HadoopRpcService.SEND_SERVER_REQUEST, INSTANCE);
  }

  private static final Map<String, AtomicInteger> delays = new ConcurrentHashMap<>();

  public static void clear() {
    delays.clear();
  }

  public static void setDelayMs(String id, int delayMs) {
    AtomicInteger d = delays.get(id);
    if (d == null) {
      delays.put(id, d = new AtomicInteger());
    }
    d.set(delayMs);
  }

  private DelaySendServerRequest() {}

  @Override
  public Object execute(Object... args) throws IOException {
    final Object id = args[0];
    final AtomicInteger d = delays.get(id);
    if (d == null) {
      return null;
    }
    LOG.info(id + ": " + d + ", args=" + Arrays.toString(args));
    try {
      RaftTestUtil.delay(d::get);
    } catch (InterruptedException e) {
      RaftUtils.toInterruptedIOException("", e);
    }
    return null;
  }
}
