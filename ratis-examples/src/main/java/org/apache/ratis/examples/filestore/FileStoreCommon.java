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
package org.apache.ratis.examples.filestore;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public interface FileStoreCommon {
  String STATEMACHINE_PREFIX = "example.filestore.statemachine";

  String STATEMACHINE_DIR_KEY = STATEMACHINE_PREFIX + ".dir";

  String STATEMACHINE_WRITE_THREAD_NUM = STATEMACHINE_PREFIX + ".write.thread.num";

  String STATEMACHINE_READ_THREAD_NUM = STATEMACHINE_PREFIX + ".read.thread.num";

  String STATEMACHINE_COMMIT_THREAD_NUM = STATEMACHINE_PREFIX + ".commit.thread.num";

  String STATEMACHINE_DELETE_THREAD_NUM = STATEMACHINE_PREFIX + ".delete.thread.num";

  SizeInBytes MAX_CHUNK_SIZE = SizeInBytes.valueOf(64, TraditionalBinaryPrefix.MEGA);

  static int getChunkSize(long suggestedSize) {
    return Math.toIntExact(Math.min(suggestedSize, MAX_CHUNK_SIZE.getSize()));
  }

  static ByteString toByteString(Path p) {
    return ProtoUtils.toByteString(p.toString());
  }

  static <T> CompletableFuture<T> completeExceptionally(
      long index, String message) {
    return completeExceptionally(index, message, null);
  }

  static <T> CompletableFuture<T> completeExceptionally(
      long index, String message, Throwable cause) {
    return completeExceptionally(message + ", index=" + index, cause);
  }

  static <T> CompletableFuture<T> completeExceptionally(
      String message, Throwable cause) {
    return JavaUtils.completeExceptionally(new IOException(message, cause));
  }
}
