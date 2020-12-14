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
package org.apache.ratis.statemachine.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.JavaUtils;

/**
 * Each snapshot has a list of files.
 *
 * The objects of this class are immutable.
 */
public class FileListSnapshotInfo implements SnapshotInfo {
  private final TermIndex termIndex;
  private final List<FileInfo> files;

  public FileListSnapshotInfo(List<FileInfo> files, long term, long index) {
    this.termIndex = TermIndex.valueOf(term, index);
    this.files = Collections.unmodifiableList(new ArrayList<>(files));
  }

  @Override
  public TermIndex getTermIndex() {
    return termIndex;
  }

  @Override
  public List<FileInfo> getFiles() {
    return files;
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + getTermIndex() + ":" + files;
  }
}
