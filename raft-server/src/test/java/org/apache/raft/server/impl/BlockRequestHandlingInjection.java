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
package org.apache.raft.server.impl;

import org.apache.raft.RaftTestUtil;
import org.apache.raft.util.CodeInjectionForTesting;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Inject code to block a server from handling incoming requests. */
public class BlockRequestHandlingInjection implements CodeInjectionForTesting.Code {
  private static final BlockRequestHandlingInjection INSTANCE =
      new BlockRequestHandlingInjection();

  static {
    CodeInjectionForTesting.put(RaftServer.REQUEST_VOTE, INSTANCE);
    CodeInjectionForTesting.put(RaftServer.APPEND_ENTRIES, INSTANCE);
    CodeInjectionForTesting.put(RaftServer.INSTALL_SNAPSHOT, INSTANCE);
  }

  public static BlockRequestHandlingInjection getInstance() {
    return INSTANCE;
  }

  private final Map<String, Boolean> requestors = new ConcurrentHashMap<>();
  private final Map<String, Boolean> repliers = new ConcurrentHashMap<>();

  private BlockRequestHandlingInjection() {}

  public void blockRequestor(String requestor) {
    requestors.put(requestor, true);
  }

  public void unblockRequestor(String requestor) {
    requestors.remove(requestor);
  }

  public void blockReplier(String replier) {
    repliers.put(replier, true);
  }

  public void unblockReplier(String replier) {
    repliers.remove(replier);
  }

  public void unblockAll() {
    requestors.clear();
    repliers.clear();
  }

  @Override
  public boolean execute(String localId, String remoteId, Object... args) {
    if (shouldBlock(localId, remoteId)) {
      try {
        RaftTestUtil.block(() -> shouldBlock(localId, remoteId));
        return true;
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while blocking request handling from " + remoteId
            + " to " + localId);
      }
    }
    return false;
  }

  private boolean shouldBlock(String localId, String remoteId) {
    return repliers.containsKey(localId) || requestors.containsKey(remoteId);
  }
}
