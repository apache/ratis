/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.raft.server;

import com.google.common.base.Preconditions;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.util.RaftUtils;

import java.io.IOException;

public class PendingRequest implements Comparable<PendingRequest> {
  private final Long index;
  private final RaftClientRequest request;

  private RaftClientReply reply = null;
  private IOException exception;

  PendingRequest(long index, RaftClientRequest request) {
    this.index = index;
    this.request = request;
  }

  PendingRequest(SetConfigurationRequest request) {
    this(RaftConstants.INVALID_LOG_INDEX, request);
  }

  long getIndex() {
    return index;
  }

  RaftClientRequest getRequest() {
    return request;
  }

  synchronized void setReply(IOException e) {
    Preconditions.checkState(reply == null);
    Preconditions.checkState(exception == null);
    reply = e != null? null: new RaftClientReply(getRequest(), true);
    exception = e;
    notifyAll();
  }

  private RaftClientReply getReply() throws IOException {
    if (exception != null) {
      throw new IOException("Caught exception for " + this, exception);
    }
    return reply;
  }

  public synchronized RaftClientReply waitForReply() throws IOException {
    final RaftClientReply r = getReply();
    if (r != null) {
      return r;
    }

    try {
      wait();
    } catch (InterruptedException e) {
      throw RaftUtils.toInterruptedIOException(
          "waitForRely interrupted, " + this, e);
    }
    return getReply();
  }

  boolean sendReply(RaftServerRpc rpc) {
    try {
      rpc.sendClientReply(request, reply, exception);
      return true;
    } catch (IOException ioe) {
      RaftServer.LOG.error(this + " has " + ioe);
      RaftServer.LOG.trace("TRACE", ioe);
      return false;
    }
  }

  @Override
  public int compareTo(PendingRequest that) {
    return Long.compare(this.index, that.index);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(index=" + index
        + ", request=" + request + ", reply=" + reply + ")";
  }
}
