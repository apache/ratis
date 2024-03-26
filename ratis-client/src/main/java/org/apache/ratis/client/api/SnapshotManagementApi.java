/*
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
package org.apache.ratis.client.api;

import org.apache.ratis.protocol.RaftClientReply;

import java.io.IOException;

/**
 * An API to support control snapshot
 * such as create and list snapshot file.
 */
public interface SnapshotManagementApi {

  /** The same as create(0, timeoutMs). */
  default RaftClientReply create(long timeoutMs) throws IOException {
    return create(0, timeoutMs);
  }

    /** The same as create(force? 1 : 0, timeoutMs). */
  default RaftClientReply create(boolean force, long timeoutMs) throws IOException {
    return create(force? 1 : 0, timeoutMs);
  }

    /**
   * Trigger to create a snapshot.
   *
   * @param creationGap When (creationGap > 0) and (astAppliedIndex - lastSnapshotIndex < creationGap),
   *                    return lastSnapshotIndex; otherwise, take a new snapshot and then return its index.
   *                    When creationGap == 0, use the server configured value as the creationGap.
   * @return a reply.  When {@link RaftClientReply#isSuccess()} is true,
   *         {@link RaftClientReply#getLogIndex()} is the snapshot index fulfilling the operation.
   */
  RaftClientReply create(long creationGap, long timeoutMs) throws IOException;
}
