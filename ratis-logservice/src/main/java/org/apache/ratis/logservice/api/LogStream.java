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
import java.util.Collection;

import org.apache.ratis.client.RaftClient;

/**
 * A distributed log with "infinite" length that supports reads and writes.
 */
public interface LogStream extends AutoCloseable{

  /**
   * An enumeration that defines the current state of a LogStream
   */
  public enum State {
    OPEN,
    CLOSED;
  }

  /**
   * Returns the unique name to identify this log.
   */
  LogName getName();

  /**
   * Returns the current state of this log.
   */
  State getState();

  /**
   * Returns the size of this LogStream in bytes.
   * @throws IOException
   */
  long getSize() throws IOException;

  /**
   * Creates a reader to read this LogStream.
   *
   * @return A synchronous reader
   */
  LogReader createReader();

  /**
   * Creates a write to write to this LogStream.
   *
   * @return A synchronous writer
   */
  LogWriter createWriter();

  /**
   * Returns the recordId of the last record in this LogStream. For an empty log, the recordId is {@code 0}.
   * @throws IOException
   */
  long getLastRecordId() throws IOException;

  /**
   * Returns the recordId of the first record in this LogStream. For an empty log, the recordId is {@code 0}.
   * @throws IOException
   */
  long getStartRecordId() throws IOException;

  /**
   * Returns all {@link RecordListeners} for this LogStream.
   */
  Collection<RecordListener> getRecordListeners();

  /**
   * Returns a copy of the Configuration for this LogStream.
   */
  LogServiceConfiguration getConfiguration();

  /**
   * Add new log record listener
   * @param listener listener
   */
  void addRecordListener(RecordListener listener);


  /**
   * Remove record listener
   * @param listener listener
   * @return true, if successful, false - otherwise
   */
  boolean removeRecordListener (RecordListener listener);

  /**
   * Get Raft Client
   * @return Raft client
   */

  RaftClient getRaftClient();
}
