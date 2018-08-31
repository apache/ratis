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
import java.util.Iterator;

import org.apache.ratis.logservice.api.LogStream.State;

/**
 * Entry point for interacting with the Ratis LogService.
 */
public interface LogService extends AutoCloseable {
  /*
   * How to create a LogStream
   */

  /**
   * Creates a new {@link LogStream} identified by the given name with default
   * configuration. Throws an exception if a {@link LogStream} with the given
   * name already exists.
   *
   * @param name Unique name for this LogStream.
   */
  LogStream createLog(LogName name);

  /**
   * Creates a new {@link LogStream} identified by the given name. Throws
   * an exception if a {@link LogStream} with the given name already exists.
   *
   * @param name Unique name for this LogStream.
   * @param config Configuration object for this LogStream
   */
  LogStream createLog(LogName name, LogStreamConfiguration config);

  /*
   * How to get LogStreams that already exist
   */
  /**
   * Fetches the {@link LogStream} identified by the given name.
   *
   * @param name The name of the LogStream
   */
  LogStream getLog(LogName name);

  /**
   * Lists all {@link LogStream} instances known by this LogService.
   */
  Iterator<LogStream> listLogs();

  /*
   * How to close, archive, and delete LogStreams
   */

  /**
   * Moves the {@link LogStream} identified by the {@code name} from {@link State.OPEN} to {@link State.CLOSED}.
   * If the log is not {@link State#OPEN}, this method returns an error.
   *
   * @param name The name of the log to close
   */
  // TODO this name sucks, confusion WRT the Java Closeable interface.
  void closeLog(LogName name);

  /**
   * Returns the current {@link State} of the log identified by {@code name}.
   *
   * @param name The name of a log
   */
  State getState(LogName name);

  /**
   * Archives the given log out of the state machine and into a configurable long-term storage. A log must be
   * in {@link State#CLOSED} to archive it.
   *
   * @param name The name of the log to archive.
   */
  void archiveLog(LogName name);

  /**
   * Deletes the {@link LogStream}.
   * @param name The name of the LogStream
   */
  void deleteLog(LogName name);

  /*
   * Change the configuration of a LogStream or manipulate a LogStream's listeners
   */

  /**
   * Updates a log with the new configuration object, overriding
   * the previous configuration.
   *
   * @param config The new configuration object
   */
  void updateConfiguration(LogName name, LogStreamConfiguration config);

  /**
   * Registers a {@link RecordListener} with the log which will receive all records written using
   * the unique name provided by {@link RecorderListener#getName()}.
   *
   * Impl spec: The name returned by a {@link RecordListener} instance uniquely identifies it against other
   * instances.
   *
   * @param the log's name
   * @param listener The listener to register
   */
  void addRecordListener(LogName name, RecordListener listener);

  /**
   * Removes a {@link RecordListener) for the log.
   *
   * Impl spec: The name returned by a {@link RecordListener} instance uniquely identifies it against
   * other instances.
   *
   * @param the log's name
   * @param listener The listener to remove
   */
  void removeRecordListener(LogName name, RecordListener listener);

  /**
   * Overrides {@link #close()} in {@link AutoCloseable} to throw an IOException.
   */
  void close() throws IOException;
}
