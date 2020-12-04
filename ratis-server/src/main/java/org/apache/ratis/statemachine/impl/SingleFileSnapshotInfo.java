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

import java.util.Collections;

import org.apache.ratis.server.storage.FileInfo;

/**
 * Each snapshot only has a single file.
 *
 * The objects of this class are immutable.
 */
public class SingleFileSnapshotInfo extends FileListSnapshotInfo {
  public SingleFileSnapshotInfo(FileInfo fileInfo, long term, long endIndex) {
    super(Collections.singletonList(fileInfo), term, endIndex);
  }

  /** @return the file associated with the snapshot. */
  public FileInfo getFile() {
    return getFiles().get(0);
  }
}
