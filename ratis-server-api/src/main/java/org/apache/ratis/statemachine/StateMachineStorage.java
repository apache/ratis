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
package org.apache.ratis.statemachine;

import java.io.IOException;

import org.apache.ratis.server.storage.RaftStorage;

public interface StateMachineStorage {

  void init(RaftStorage raftStorage) throws IOException;

  /**
   * Returns the information for the latest durable snapshot.
   */
  SnapshotInfo getLatestSnapshot();

  // TODO: StateMachine can decide to compact the files independently of concurrent install snapshot
  // etc requests. We should have ref counting for the SnapshotInfo with a release mechanism
  // so that raft server will release the files after the snapshot file copy in case a compaction
  // is waiting for deleting these files.

  void format() throws IOException;

  void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) throws IOException;
}
