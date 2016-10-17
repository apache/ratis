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
package org.apache.raft.protocol;

public abstract class RaftRpcMessage {

  public abstract boolean isRequest();

  public abstract String getRequestorId();

  public abstract String getReplierId();

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + getRequestorId()
        + (isRequest()? "->": "<-") + getReplierId() + ")";
  }

  public static class Request extends RaftRpcMessage {
    private final String requestorId;
    private final String replierId;

    public Request(String requestorId, String replierId) {
      this.requestorId = requestorId;
      this.replierId = replierId;
    }

    @Override
    public final boolean isRequest() {
      return true;
    }

    @Override
    public String getRequestorId() {
      return requestorId;
    }

    @Override
    public String getReplierId() {
      return replierId;
    }
  }

  public static class Reply extends RaftRpcMessage {
    private final String requestorId;
    private final String replierId;
    private final boolean success;

    public Reply(String requestorId, String replierId, boolean success) {
      this.requestorId = requestorId;
      this.replierId = replierId;
      this.success = success;
    }

    @Override
    public final boolean isRequest() {
      return false;
    }

    @Override
    public String getRequestorId() {
      return requestorId;
    }

    @Override
    public String getReplierId() {
      return replierId;
    }

    @Override
    public String toString() {
      return super.toString() + ", success: " + isSuccess();
    }

    public boolean isSuccess() {
      return success;
    }
  }
}
