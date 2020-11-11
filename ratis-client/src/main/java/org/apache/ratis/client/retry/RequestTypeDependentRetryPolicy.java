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

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A {@link org.apache.ratis.protocol.RaftClientRequest.Type} dependent {@link RetryPolicy}
 * such that each type can be set to use an individual policy.
 * When the policy is not set for a particular type,
 * the {@link RetryPolicies#retryForeverNoSleep()} policy is used as the default.
 */
public final class RequestTypeDependentRetryPolicy implements RetryPolicy {
  public static class Builder {
    private final EnumMap<RaftProtos.RaftClientRequestProto.TypeCase, RetryPolicy>
        retryPolicyMap = new EnumMap<>(RaftProtos.RaftClientRequestProto.TypeCase.class);
    private EnumMap<RaftProtos.RaftClientRequestProto.TypeCase, TimeDuration>
        timeoutMap = new EnumMap<>(RaftProtos.RaftClientRequestProto.TypeCase.class);

    /** Set the given policy for the given type. */
    public Builder setRetryPolicy(RaftProtos.RaftClientRequestProto.TypeCase type, RetryPolicy policy) {
      final RetryPolicy previous = retryPolicyMap.put(type, policy);
      Preconditions.assertNull(previous, () -> "The retryPolicy for type " + type + " is already set to " + previous);
      return this;
    }

    public Builder setTimeout(RaftProtos.RaftClientRequestProto.TypeCase type, TimeDuration timeout) {
      final TimeDuration previous = timeoutMap.put(type, timeout);
      Preconditions.assertNull(previous, () -> "The timeout for type " + type + " is already set to " + previous);
      return this;
    }

    public RequestTypeDependentRetryPolicy build() {
      return new RequestTypeDependentRetryPolicy(retryPolicyMap, timeoutMap);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final Map<RaftProtos.RaftClientRequestProto.TypeCase, RetryPolicy> retryPolicyMap;
  private final Map<RaftProtos.RaftClientRequestProto.TypeCase, TimeDuration> timeoutMap;
  private final Supplier<String> myString;

  private RequestTypeDependentRetryPolicy(
      EnumMap<RaftProtos.RaftClientRequestProto.TypeCase, RetryPolicy> map,
      EnumMap<RaftProtos.RaftClientRequestProto.TypeCase, TimeDuration> timeoutMap) {
    this.retryPolicyMap = Collections.unmodifiableMap(map);
    this.timeoutMap = timeoutMap;
    this.myString = () -> {
      final StringBuilder b = new StringBuilder(JavaUtils.getClassSimpleName(getClass())).append("{");
      map.forEach((key, value) -> b.append(key).append("->").append(value).append(", "));
      b.setLength(b.length() - 2);
      return b.append("}").toString();
    };
  }

  @Override
  public Action handleAttemptFailure(Event event) {
    if (!(event instanceof ClientRetryEvent)) {
      return RetryPolicies.retryForeverNoSleep().handleAttemptFailure(event);
    }
    final ClientRetryEvent clientEvent = (ClientRetryEvent) event;
    final TimeDuration timeout = timeoutMap.get(clientEvent.getRequest().getType().getTypeCase());
    if (timeout != null && clientEvent.isRequestTimeout(timeout)) {
      return NO_RETRY_ACTION;
    }
    return Optional.ofNullable(
        retryPolicyMap.get(clientEvent.getRequest().getType().getTypeCase()))
        .orElse(RetryPolicies.retryForeverNoSleep())
        .handleAttemptFailure(event);
  }

  @Override
  public String toString() {
    return myString.get();
  }
}
