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
package org.apache.ratis.protocol.exceptions;

import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.retry.RetryPolicy;

/**
 * Retry failure as per the {@link RetryPolicy} defined.
 */
public class RaftRetryFailureException extends RaftException {

  private final int attemptCount;

  public RaftRetryFailureException(
      RaftClientRequest request, int attemptCount, RetryPolicy retryPolicy, Throwable cause) {
    super("Failed " + request + " for " + attemptCount + " attempts with " + retryPolicy, cause);
    this.attemptCount = attemptCount;
  }

  public int getAttemptCount() {
    return attemptCount;
  }
}