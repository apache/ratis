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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A distributed log with "infinite" length that supports reads and writes.
 */
public interface LogStream {

  /**
   * Returns the unique name to identify this log.
   */
  LogName getName();

  /**
   * Returns the size of this LogStream in bytes.
   */
  long getSizeInBytes();

  /**
   * Returns the number of records in this LogStream.
   */
  long getSizeInRecords();

  /**
   * Creates a reader to read this LogStream asynchronously.
   *
   * @return An asynchronous reader
   */
  AsyncLogReader createAsyncReader();

  /**
   * Creates a writer to write to this LogStream asynchronously.
   *
   * @return An asynchronous writer
   */
  AsyncLogWriter createAsyncWriter();

  /**
   * Creates a reader to read this LogStream synchronously.
   *
   * @return A synchronous reader
   */
  LogReader createReader();

  /**
   * Creates a write to write to this LogStream synchronously.
   *
   * @return A synchronous writer
   */
  LogWriter createWriter();

  /**
   * Removes the elements in this LogStream prior to the given recordId.
   *
   * @param recordId A non-negative recordId for this LogStream
   */
  CompletableFuture<Void> truncateBefore(long recordId);

  /**
   * Returns the recordId which is the start of the LogStream. When there are records which were truncated
   * from the LogStream, this will return a value larger than {@code 0}.
   */
  CompletableFuture<Long> getFirstRecordId();

  /**
   * Copies all records from the beginning of the LogStream until the given {@code recordId}
   * to the configured archival storage.
   *
   * @param recordId A non-negative recordId for this LogStream
   */
  CompletableFuture<Void> archiveBefore(long recordId);

  /**
   * Returns the recordId, prior to which, all records in the LogStream are archived.
   */
  CompletableFuture<Long> getArchivalPoint();

  /**
   * Registers a {@link RecordListener} with this LogStream which will receive all records written.
   *
   * @param listener The listener to register
   */
  void addRecordListener(RecordListener listener);

  /**
   * Removes a {@link RecordListener) from this LogStream.
   *
   * @param listener The listener to remove
   */
  void removeRecordListener(RecordListener listener);

  /**
   * Returns all {@link RecordListeners} for this LogStream.
   */
  List<RecordListener> getRecordListeners();
}
