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
package org.apache.ratis.client;

import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.retry.RetryPolicy;

/** An {@link RetryPolicy.Event} specific to client request failure. */
public class ClientRetryEvent implements RetryPolicy.Event {
  private final int attemptCount;
  private final RaftClientRequest request;
  private final Throwable cause;

  public ClientRetryEvent(int attemptCount, RaftClientRequest request, Throwable cause) {
    this.attemptCount = attemptCount;
    this.request = request;
    this.cause = cause;
  }

  public ClientRetryEvent(int attemptCount, RaftClientRequest request) {
    this(attemptCount, request, null);
  }

  @Override
  public int getAttemptCount() {
    return attemptCount;
  }

  public RaftClientRequest getRequest() {
    return request;
  }

  public Throwable getCause() {
    return cause;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":attempt=" + attemptCount + ",request=" + request + ",cause=" + cause;
  }
}
