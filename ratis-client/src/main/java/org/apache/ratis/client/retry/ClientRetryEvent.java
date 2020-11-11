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
package org.apache.ratis.client.retry;

import org.apache.ratis.client.impl.RaftClientImpl.PendingClientRequest;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;

/** An {@link RetryPolicy.Event} specific to client request failure. */
public class ClientRetryEvent implements RetryPolicy.Event {
  private final int attemptCount;
  private final int causeCount;
  private final RaftClientRequest request;
  private final Throwable cause;
  private PendingClientRequest pending;

  @VisibleForTesting
  public ClientRetryEvent(int attemptCount, RaftClientRequest request, Throwable cause) {
    this(attemptCount, request, attemptCount, cause);
  }

  public ClientRetryEvent(RaftClientRequest request, Throwable t, PendingClientRequest pending) {
    this(pending.getAttemptCount(), request, pending.getExceptionCount(t), t);
    this.pending = pending;
  }

  private ClientRetryEvent(int attemptCount, RaftClientRequest request, int causeCount, Throwable cause) {
    this.attemptCount = attemptCount;
    this.causeCount = causeCount;
    this.request = request;
    this.cause = cause;
  }

  @Override
  public int getAttemptCount() {
    return attemptCount;
  }

  @Override
  public int getCauseCount() {
    return causeCount;
  }

  public RaftClientRequest getRequest() {
    return request;
  }

  @Override
  public Throwable getCause() {
    return cause;
  }

  boolean isRequestTimeout(TimeDuration timeout) {
    return pending != null && pending.isRequestTimeout(timeout);
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass())
        + ":attempt=" + attemptCount
        + ",request=" + request
        + ",cause=" + cause;
  }
}
