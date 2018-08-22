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
package org.apache.ratis.logservice.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous client interface to read from a LogStream.
 */
public interface AsyncLogReader extends AutoCloseable {

  /**
   * Seeks to the position before the record at the provided {@code recordId} in the LogStream.
   *
   * @param recordId A non-negative, recordId in the LogStream
   * @return A future for when the operation is completed.
   */
  CompletableFuture<Void> seek(long recordId) throws IOException;

  /**
   * Reads the next record from the LogStream at the current position and advances the current position
   * to after the record which was just returned.
   *
   * @return A future providing the data for the next record.
   */
  CompletableFuture<ByteBuffer> readNext() throws IOException;

  /**
   * Reads the next record from the LogStream at the current position into the provided {@link buffer} and
   * advances the current position to the point after the record just read.
   *
   * @param buffer A buffer to read the record into
   * @return A future acknowledging when the operation is complete
   */
  CompletableFuture<Void> readNext(ByteBuffer buffer) throws IOException;

  /**
   * Reads the next {@code numRecords} records from the LogStream, starting at the current position. This method
   * may return fewer than requested records if the LogStream does not have sufficient records to return.
   *
   * @param numRecords The number of records to return
   * @return A future providing the records, no more than the requested {@code numRecords} amount.
   */
  CompletableFuture<List<ByteBuffer>> readBulk(int numRecords) throws IOException;

  /**
   * Returns the current position of this Reader. The position is a {@code recordId}.
   */
  CompletableFuture<Long> getPosition();

  /**
   * Overrides {@link close()} in {@link AutoCloseable} to throw an IOException.
   */
  void close() throws IOException;
}
