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
package org.apache.ratis.statemachine;

import java.util.List;

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;

/**
 * SnapshotInfo represents a durable state by the state machine. The state machine implementation is
 * responsible for the layout of the snapshot files as well as making the data durable. Latest term,
 * latest index, and the raft configuration must be saved together with any data files in the
 * snapshot.
 */
public interface SnapshotInfo {

  /**
   * Returns the term and index corresponding to this snapshot.
   * @return The term and index corresponding to this snapshot.
   */
  TermIndex getTermIndex();

  /**
   * Returns the term corresponding to this snapshot.
   * @return The term corresponding to this snapshot.
   */
  long getTerm();

  /**
   * Returns the index corresponding to this snapshot.
   * @return The index corresponding to this snapshot.
   */
  long getIndex();

  /**
   * Returns a list of files corresponding to this snapshot. This list should include all
   * the files that the state machine keeps in its data directory. This list of files will be
   * copied as to other replicas in install snapshot RPCs.
   * @return a list of Files corresponding to the this snapshot.
   */
  List<FileInfo> getFiles();
}
