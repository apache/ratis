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
package org.apache.ratis.protocol;

import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.ReferenceCountedObject;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Asynchronous version of {@link RaftClientProtocol}. */
public interface RaftClientAsynchronousProtocol {
  /**
   * It is recommended to override {@link #submitClientRequestAsync(ReferenceCountedObject)} instead.
   * Then, it does not have to override this method.
   */
  default CompletableFuture<RaftClientReply> submitClientRequestAsync(
      RaftClientRequest request) throws IOException {
    return submitClientRequestAsync(ReferenceCountedObject.wrap(request));
  }

  /**
   * A referenced counted request is submitted from a client for processing.
   * Implementations of this method should retain the request, process it and then release it.
   * The request may be retained even after the future returned by this method has completed.
   *
   * @return a future of the reply
   * @see ReferenceCountedObject
   */
  default CompletableFuture<RaftClientReply> submitClientRequestAsync(
      ReferenceCountedObject<RaftClientRequest> requestRef) {
    try {
      // for backward compatibility
      return submitClientRequestAsync(requestRef.retain());
    } catch (Exception e) {
      return JavaUtils.completeExceptionally(e);
    } finally {
      requestRef.release();
    }
  }
}