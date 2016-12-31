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
package org.apache.raft.statemachine;

import org.apache.raft.server.impl.RaftConfiguration;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.FileInfo;

import java.util.List;

public class SnapshotInfoImpl implements SnapshotInfo {

  protected final RaftConfiguration raftConfiguration;
  protected final List<FileInfo> files;
  protected final TermIndex termIndex;

  public SnapshotInfoImpl(RaftConfiguration raftConfiguration,
                          List<FileInfo> files, long term, long index) {
    this.raftConfiguration = raftConfiguration;
    this.files = files;
    this.termIndex = new TermIndex(term, index);
  }

  @Override
  public TermIndex getTermIndex() {
    return termIndex;
  }

  @Override
  public long getTerm() {
    return termIndex.getTerm();
  }

  @Override
  public long getIndex() {
    return termIndex.getIndex();
  }

  @Override
  public List<FileInfo> getFiles() {
    return files;
  }

  @Override
  public RaftConfiguration getRaftConfiguration() {
    return raftConfiguration;
  }

  @Override
  public String toString() {
    return raftConfiguration + "." + files + "." + termIndex;
  }
}
