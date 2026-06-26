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
package org.apache.ratis.client.api;

import org.apache.ratis.protocol.DataStreamPacket;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.util.ReferenceCountedObject;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * An asynchronous input stream supporting zero buffer copying.
 */
public interface DataStreamInput extends Closeable {
  /**
   * Read the next chunk in the stream asynchronously.
   * The caller owns the returned {@link DataStreamReply} which is a {@link ReferenceCountedObject}.
   * and a {@link DataStreamPacket}. Access the buffer via {@link DataStreamPacket#nioBuffer()}
   * or {@link DataStreamPacket#nioBuffers()}.
   * It must call {@link ReferenceCountedObject#release()} after consuming it.
   *
   * @return a future of the reference-counted reply.
   */
  CompletableFuture<ReferenceCountedObject<DataStreamReply>> readAsync();
}
