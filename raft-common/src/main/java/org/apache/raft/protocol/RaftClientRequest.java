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

public class RaftClientRequest extends RaftRpcMessage.Request {
  private final Message message;
  private final boolean readOnly;

  public RaftClientRequest(String  requestorId, String replierId,
                           Message message) {
    this(requestorId, replierId, message, false);
  }

  public RaftClientRequest(String requestorId, String replierId,
       Message message, boolean readOnly) {
    super(requestorId, replierId);
    this.message = message;
    this.readOnly = readOnly;
  }

  public Message getMessage() {
    return message;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  @Override
  public String toString() {
    return super.toString() + ", " + (isReadOnly()? "RO": "RW")
        + " message: " + getMessage();
  }
}
