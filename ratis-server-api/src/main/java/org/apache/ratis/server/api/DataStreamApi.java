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
package org.apache.ratis.server.api;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/** Server side data stream API. */
public interface DataStreamApi {
  /**
   * Similar to {@link java.nio.channels.FileChannel#transferTo(long, long, WritableByteChannel)}
   * except that the parameter of this method is request message
   * but not position, count in FileChannel.
   *
   * @param request the request message
   * @param stream the output stream to send the results
   * @return the number of bytes transferred
   */
  long transferTo(Message request, WritableByteChannel stream) throws IOException;

  /** For resolving {@link DataStreamApi}. */
  @FunctionalInterface
  interface Resolver {
    /** @return the data API handling this request, or null if the API is not found. */
    DataStreamApi resolve(RaftClientRequest request);
  }
}
