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
package org.apache.hadoop.raft.server;

import org.apache.hadoop.raft.protocol.RaftRpcMessage;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.util.ExitUtil.terminate;

public  interface RequestHandler<REQUEST extends RaftRpcMessage,
    REPLY extends RaftRpcMessage> {
  static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

  boolean isRunning();

  REPLY handleRequest(REQUEST r) throws IOException;

  /**
   * A thread keep polling requests from the request queue. Used for simulation.
   */
  static class HandlerDaemon<REQUEST extends RaftRpcMessage,
                               REPLY extends RaftRpcMessage> extends Daemon {
    private final String id;
    private final RaftRpc<REQUEST, REPLY> serverRpc;
    private final RequestHandler<REQUEST, REPLY> handler;

    HandlerDaemon(String id, RequestHandler<REQUEST, REPLY> handler,
                  RaftRpc<REQUEST, REPLY> serverRpc) {
      this.id = id;
      this.serverRpc = serverRpc;
      this.handler = handler;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "-" + id;
    }

    @Override
    public void run() {
      while (handler.isRunning()) {
        try {
          final REQUEST request = serverRpc.takeRequest(id);
          final REPLY reply = handler.handleRequest(request);
          serverRpc.sendReply(request, reply);
        } catch (Throwable t) {
          if (!handler.isRunning()) {
            LOG.info(this + " is stopped.");
            break;
          } else if (t instanceof InterruptedException) {
            LOG.info(this + " is interrupted.");
            break;
          }
          LOG.error(this + " caught " + t, t);
          terminate(1, t);
        }
      }
    }
  }
}
