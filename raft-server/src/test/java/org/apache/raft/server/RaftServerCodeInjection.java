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
package org.apache.raft.server;

import org.apache.raft.RaftTestUtil;
import org.apache.raft.util.CodeInjectionForTesting;
import org.apache.raft.util.RaftUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Inject code to block server requests. */
public class RaftServerCodeInjection
    implements CodeInjectionForTesting.Code {
  private static final RaftServerCodeInjection INSTANCE = new RaftServerCodeInjection();

  static {
    CodeInjectionForTesting.put(RaftServer.REQUEST_VOTE, INSTANCE);
    CodeInjectionForTesting.put(RaftServer.APPEND_ENTRIES, INSTANCE);
    CodeInjectionForTesting.put(RaftServer.INSTALL_SNAPSHOT, INSTANCE);
  }

  public static Map<String, Boolean> getRequestors() {
    return INSTANCE.requestors;
  }

  public static Map<String, Boolean> getRepliers() {
    return INSTANCE.repliers;
  }

  private final Map<String, Boolean> requestors = new ConcurrentHashMap<>();
  private final Map<String, Boolean> repliers = new ConcurrentHashMap<>();

  private RaftServerCodeInjection() {}

  @Override
  public Object execute(Object... args) {
    final Object requestorId = args[1];
    final Object replierId = args[0];
    try {
      RaftTestUtil.block(() -> nullAsFalse(requestors.get(requestorId))
          || nullAsFalse(repliers.get(replierId)));
    } catch (InterruptedException e) {
      throw new RuntimeException(RaftUtils.toInterruptedIOException("", e));
    }
    return null;
  }

  static boolean nullAsFalse(Boolean b) {
    return b != null && b;
  }

}
