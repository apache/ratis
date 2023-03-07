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
package org.apache.ratis.grpc.util;

import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.thirdparty.io.grpc.ClientInterceptor;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntSupplier;

public final class StreamObserverWithTimeout<T> implements StreamObserver<T> {
  public static final Logger LOG = LoggerFactory.getLogger(StreamObserverWithTimeout.class);

  public static <T> StreamObserverWithTimeout<T> newInstance(String name, TimeDuration timeout,
      Function<ClientInterceptor, StreamObserver<T>> newStreamObserver) {
    final AtomicInteger responseCount = new AtomicInteger();
    final ResponseNotifyClientInterceptor interceptor = new ResponseNotifyClientInterceptor(
        r -> responseCount.getAndIncrement());
    return new StreamObserverWithTimeout<>(
        name, timeout, responseCount::get, newStreamObserver.apply(interceptor));
  }

  private final String name;
  private final TimeDuration timeout;
  private final StreamObserver<T> observer;
  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();

  private final AtomicBoolean isClose = new AtomicBoolean();
  private final AtomicInteger requestCount = new AtomicInteger();
  private final IntSupplier responseCount;

  private StreamObserverWithTimeout(String name, TimeDuration timeout, IntSupplier responseCount,
      StreamObserver<T> observer) {
    this.name = JavaUtils.getClassSimpleName(getClass()) + "-" + name;
    this.timeout = timeout;
    this.responseCount = responseCount;
    this.observer = observer;
  }

  @Override
  public void onNext(T request) {
    observer.onNext(request);
    final int id = requestCount.incrementAndGet();
    scheduler.onTimeout(timeout, () -> handleTimeout(id, request),
        LOG, () -> name + ": Timeout check failed for request: " + request);
  }

  private void handleTimeout(int id, T request) {
    if (id > responseCount.getAsInt()) {
      onError(new TimeoutIOException(name + ": Timed out " + timeout + " for sending request " + request));
    }
  }

  @Override
  public void onError(Throwable throwable) {
    if (isClose.compareAndSet(false, true)) {
      observer.onError(throwable);
    }
  }

  @Override
  public void onCompleted() {
    if (isClose.compareAndSet(false, true)) {
      observer.onCompleted();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
