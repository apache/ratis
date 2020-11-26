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
package org.apache.ratis.io;

import java.io.File;

/**
 * Encapsulate a {@link File} with a starting position and a byte count.
 *
 * The class is immutable.
 */
public final class FilePositionCount {
  public static FilePositionCount valueOf(File file, long position, long count) {
    return new FilePositionCount(file, position, count);
  }

  private final File file;
  private final long position;
  private final long count;

  private FilePositionCount(File file, long position, long count) {
    this.file = file;
    this.position = position;
    this.count = count;
  }

  /** @return the file. */
  public File getFile() {
    return file;
  }

  /** @return the starting position. */
  public long getPosition() {
    return position;
  }

  /** @return the byte count. */
  public long getCount() {
    return count;
  }
}
