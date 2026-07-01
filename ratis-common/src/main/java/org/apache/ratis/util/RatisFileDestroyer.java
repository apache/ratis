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
package org.apache.ratis.util;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

/**
 * Pluggable file destruction hook used by {@link FileUtils}.
 */
public interface RatisFileDestroyer {
  RatisFileDestroyer DEFAULT = new RatisFileDestroyer() {
  };

  /**
   * Delete the given path.
   */
  default void delete(Path path) throws IOException {
    Files.delete(path);
  }

  /**
   * Delete the given path if it exists.
   * The default implementation delegates to {@link #delete(Path)} so that overriding delete also covers this method.
   */
  default void deleteIfExists(Path path) throws IOException {
    try {
      delete(path);
    } catch (NoSuchFileException ignored) {
      // Same behavior as Files.deleteIfExists for a missing file.
    }
  }

  /**
   * Truncate the file backing the given channel. Implementations must not close the channel.
   */
  default void truncate(FileChannel channel, Path path, long targetLength) throws IOException {
    channel.truncate(targetLength);
  }
}
