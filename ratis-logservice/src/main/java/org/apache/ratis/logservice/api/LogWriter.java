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
 * Synchronous client interface to write to a LogStream.
 */
public interface LogWriter extends AutoCloseable {

  /**
   * Appends the given data as a record in the LogStream.
   *
   * @param data The record to append
   * @return The recordId for the record just written
   */
  long write(ByteBuffer data) throws IOException;

  /**
   * Appends each entry of data as a new record in the LogStream. If this method returns
   * successfully, all records can be considered persisted. Otherwise, none can be assumed
   * to have been written.
   *
   * @param records Records to append
   * @return The recordIds assigned to the records written
   */
  List<Long> write(List<ByteBuffer> records) throws IOException;

  /**
   * Guarantees that all previous data appended by {@link #write(ByteBuffer)} are persisted
   * and durable in the LogStream.
   *
   * @return The recordId prior to which all records are durable
   */
  long sync() throws IOException;

  /**
   * Overrides {@link #close()} in {@link AutoCloseable} to throw an IOException.
   */
  void close() throws IOException;
}
