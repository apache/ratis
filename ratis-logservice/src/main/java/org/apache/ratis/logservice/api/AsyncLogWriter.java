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
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous client interface to write to a LogStream.
 */
public interface AsyncLogWriter extends AutoCloseable {

  /**
   * Appends the given data as a record in the LogStream.
   *
   * @param data The record to append
   * @return The recordId for the record just written
   */
  CompletableFuture<Long> write(ByteBuffer data) throws IOException;

  /**
   * Guarantees that all previous data appended by {@link #write(ByteBuffer)} are persisted
   * and durable in the LogStream.
   *
   * @return TODO Unknown?
   */
  CompletableFuture<Long> commit() throws IOException;
}
