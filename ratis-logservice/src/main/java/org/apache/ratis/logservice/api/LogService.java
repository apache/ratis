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

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Entry point for interacting with the Ratis LogService.
 */
public interface LogService {

  /**
   * Creates a new {@link LogStream} identified by the given name. Throws
   * an exception if a {@link LogStream} with the given name already exists.
   *
   * @param name Unique name for this LogStream.
   */
  CompletableFuture<LogStream> createLog(LogName name);

  /**
   * Fetches the {@link LogStream} identified by the given name.
   *
   * @param name The name of the LogStream
   */
  CompletableFuture<LogStream> getLog(LogName name);

  /**
   * Lists all {@link LogStream} instances known by this LogService.
   */
  CompletableFuture<Iterator<LogStream>> listLogs();

  /**
   * Deletes the {@link LogStream}.
   * @param name The name of the LogStream
   */
  CompletableFuture<Void> deleteLog(LogName name);
}
