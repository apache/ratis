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
package org.apache.ratis.client.api;

import org.apache.ratis.io.CloseAsync;
import org.apache.ratis.io.FilePositionCount;
import org.apache.ratis.io.WriteOption;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientReply;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;

/** An asynchronous output stream supporting zero buffer copying. */
public interface DataStreamOutput extends CloseAsync<DataStreamReply> {
  /**
   * Send out the data in the source buffer asynchronously.
   *
   * @param src the source buffer to be sent.
   * @param options - options specifying how the data was written
   * @return a future of the reply.
   */
  CompletableFuture<DataStreamReply> writeAsync(ByteBuffer src, WriteOption... options);


  /**
   * The same as writeAsync(src, 0, src.length(), sync_default).
   * where sync_default depends on the underlying implementation.
   */
  default CompletableFuture<DataStreamReply> writeAsync(File src) {
    return writeAsync(src, 0, src.length());
  }

  /**
   * The same as writeAsync(FilePositionCount.valueOf(src, position, count), options).
   */
  default CompletableFuture<DataStreamReply> writeAsync(File src, long position, long count, WriteOption... options) {
    return writeAsync(FilePositionCount.valueOf(src, position, count), options);
  }

  /**
   * Send out the data in the source file asynchronously.
   *
   * @param src the source file with the starting position and the number of bytes.
   * @param options options specifying how the data was written
   * @return a future of the reply.
   */
  CompletableFuture<DataStreamReply> writeAsync(FilePositionCount src, WriteOption... options);

  /**
   * Return the future of the {@link RaftClientReply}
   * which will be received once this stream has been closed successfully.
   * Note that this method does not trigger closing this stream.
   *
   * @return the future of the {@link RaftClientReply}.
   */
  CompletableFuture<RaftClientReply> getRaftClientReplyFuture();

  /**
   * @return a {@link WritableByteChannel} view of this {@link DataStreamOutput}.
   */
  WritableByteChannel getWritableByteChannel();
}
