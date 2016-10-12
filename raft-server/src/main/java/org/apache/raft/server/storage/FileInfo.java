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
package org.apache.raft.server.storage;

import org.apache.hadoop.io.MD5Hash;

import java.nio.file.Path;

/**
 * Metadata about a file.
 */
public class FileInfo {
  private Path path;
  private MD5Hash fileDigest;
  private long fileSize;

  public FileInfo(Path path, MD5Hash fileDigest) {
    this.path = path;
    this.fileDigest = fileDigest;
    this.fileSize = path.toFile().length();
  }

  @Override
  public String toString() {
    return path.toString();
  }

  public Path getPath() {
    return path;
  }

  public MD5Hash getFileDigest() {
    return fileDigest;
  }

  public long getFileSize() {
    return fileSize;
  }
}
