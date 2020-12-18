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

import java.util.List;

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;

/**
 * The information of a state machine snapshot,
 * where a snapshot captures the states at a particular {@link TermIndex}.
 * Each state machine implementation must define its snapshot format and persist snapshots in a durable storage.
 */
public interface SnapshotInfo {

  /**
   * @return The term and index corresponding to this snapshot.
   */
  TermIndex getTermIndex();

  /**
   * @return The term corresponding to this snapshot.
   */
  default long getTerm() {
    return getTermIndex().getTerm();
  }

  /**
   * @return The index corresponding to this snapshot.
   */
  default long getIndex() {
    return getTermIndex().getIndex();
  }

  /**
   * @return a list of underlying files of this snapshot.
   */
  List<FileInfo> getFiles();
}
