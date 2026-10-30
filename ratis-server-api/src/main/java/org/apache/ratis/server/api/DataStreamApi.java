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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/** Server side data stream API. */
public interface DataStreamApi {
  /**
   * The channel for sending the results of {@link #transferTo(Message, WritableChannel)}.
   * <p>
   * Unlike {@link WritableByteChannel}, this channel cannot be closed by the implementation of
   * {@link #transferTo(Message, WritableChannel)}.
   * The lifecycle of the underlying stream is owned by Ratis:
   * the stream is closed successfully when {@link #transferTo(Message, WritableChannel)} returns normally,
   * or it is failed when {@link #transferTo(Message, WritableChannel)} throws an exception.
   */
  @FunctionalInterface
  interface WritableChannel {
    /**
     * Write the bytes from the given buffer to this channel.
     *
     * @param src the buffer from which bytes are to be retrieved.
     * @return the number of bytes written.
     * @see WritableByteChannel#write(ByteBuffer)
     */
    int write(ByteBuffer src) throws IOException;
  }

  /**
   * Similar to {@link java.nio.channels.FileChannel#transferTo(long, long, WritableByteChannel)}
   * except that the parameter of this method is request message
   * but not position, count in FileChannel.
   *
   * @param request the request message
   * @param stream the output stream to send the results
   * @return the number of bytes transferred
   * @deprecated use {@link #transferTo(Message, WritableChannel)}, which does not expose the stream lifecycle.
   */
  @Deprecated
  default long transferTo(Message request, WritableByteChannel stream) throws IOException {
    throw new UnsupportedOperationException(getClass().getName()
        + " does not support transferTo(Message, WritableByteChannel)");
  }

  /**
   * Similar to {@link java.nio.channels.FileChannel#transferTo(long, long, WritableByteChannel)}
   * except that the parameter of this method is request message
   * but not position, count in FileChannel.
   * <p>
   * The implementation should either return normally after all the results are written to the given channel,
   * or throw an exception to indicate a failure.
   * Ratis is responsible to close the underlying stream in both cases.
   * The given channel must not be used after this method returns.
   * <p>
   * When an implementation overrides this method, Ratis invokes this method
   * instead of the deprecated {@link #transferTo(Message, WritableByteChannel)}.
   *
   * @param request the request message
   * @param channel the channel to send the results
   * @return the number of bytes transferred
   */
  default long transferTo(Message request, WritableChannel channel) throws IOException {
    throw new UnsupportedOperationException(getClass().getName()
        + " does not support transferTo(Message, WritableChannel)");
  }

  /** For resolving {@link DataStreamApi}. */
  @FunctionalInterface
  interface Resolver {
    /** @return the data API handling this request, or null if the API is not found. */
    DataStreamApi resolve(RaftClientRequest request) throws IOException;
  }
}
