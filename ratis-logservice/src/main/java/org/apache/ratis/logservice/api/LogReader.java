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

/**
 * Synchronous client interface to read from a LogStream.
 */
public interface LogReader extends AutoCloseable {

  /**
   * Seeks to the position before the record at the provided {@code offset} in the LogStream.
   *
   * @param offset A non-negative, offset in the LogStream
   * @return A future for when the operation is completed.
   */
  void seek(long recordId) throws IOException;

  /**
   * Reads the next record from the LogStream at the current position and advances the current position
   * to after the record which was just returned.
   *
   * @return The data for the next record.
   */
  ByteBuffer readNext() throws IOException;

  /**
   * Reads the next {@code numRecords} records from the LogStream, starting at the current position. This method
   * may return fewer than requested records if the LogStream does not have sufficient records to return.
   *
   * @param numRecords The number of records to return
   * @return The records, no more than the requested {@code numRecords} amount.
   */
  List<ByteBuffer> readBulk(int numRecords) throws IOException;

  /**
   * Overrides {@link close()} in {@link AutoCloseable} to throw an IOException.
   */
  void close() throws IOException;
}
