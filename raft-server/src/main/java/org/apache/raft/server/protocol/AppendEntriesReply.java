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
package org.apache.raft.server.protocol;

/**
 * Reply to an {@link AppendEntriesRequest}.
 */
public class AppendEntriesReply extends RaftServerReply {
  public enum AppendResult {
    SUCCESS,
    NOT_LEADER,  // fail because the requester's term is not large enough
    INCONSISTENCY  // fail because of gap between the local log and the entries
  }

  /** the expected next index for appending */
  private final long nextIndex;
  private final AppendResult result;

  public AppendEntriesReply(String requestorId, String replierId, long term,
      long nextIndex, AppendResult result) {
    super(requestorId, replierId, term, result == AppendResult.SUCCESS);
    this.nextIndex = nextIndex;
    this.result = result;
  }

  @Override
  public String toString() {
    return super.toString() + ", result: " + result
        + ", nextIndex: " + nextIndex;
  }

  public long getNextIndex() {
    return nextIndex;
  }

  public AppendResult getResult() {
    return result;
  }
}
