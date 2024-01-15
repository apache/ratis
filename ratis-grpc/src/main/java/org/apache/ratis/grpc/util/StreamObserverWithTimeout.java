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
import org.apache.ratis.util.ResourceSemaphore;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.apache.ratis.util.function.StringSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

public final class StreamObserverWithTimeout<T> implements StreamObserver<T> {
  public static final Logger LOG = LoggerFactory.getLogger(StreamObserverWithTimeout.class);

  public static <T> StreamObserverWithTimeout<T> newInstance(
      String name, Function<T, String> request2String,
      Supplier<TimeDuration> timeout, int outstandingLimit,
      Function<ClientInterceptor, StreamObserver<T>> newStreamObserver) {
    final AtomicInteger responseCount = new AtomicInteger();
    final ResourceSemaphore semaphore = outstandingLimit > 0? new ResourceSemaphore(outstandingLimit): null;
    final ResponseNotifyClientInterceptor interceptor = new ResponseNotifyClientInterceptor(r -> {
      responseCount.getAndIncrement();
      if (semaphore != null) {
        semaphore.release();
      }
    });
    return new StreamObserverWithTimeout<>(name, request2String,
        timeout, responseCount::get, semaphore, newStreamObserver.apply(interceptor));
  }

  private final String name;
  private final Function<T, String> requestToStringFunction;

  private final Supplier<TimeDuration> timeoutSupplier;
  private final StreamObserver<T> observer;
  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();

  private final AtomicBoolean isClose = new AtomicBoolean();
  private final AtomicInteger requestCount = new AtomicInteger();
  private final IntSupplier responseCount;
  private final ResourceSemaphore semaphore;

  private StreamObserverWithTimeout(String name, Function<T, String> requestToStringFunction,
      Supplier<TimeDuration> timeoutSupplier, IntSupplier responseCount, ResourceSemaphore semaphore,
      StreamObserver<T> observer) {
    this.name = JavaUtils.getClassSimpleName(getClass()) + "-" + name;
    this.requestToStringFunction = requestToStringFunction;

    this.timeoutSupplier = timeoutSupplier;
    this.responseCount = responseCount;
    this.semaphore = semaphore;
    this.observer = observer;
  }

  private void acquire(StringSupplier request, TimeDuration timeout) {
    if (semaphore == null) {
      return;
    }
    boolean acquired = false;
    for (; !acquired && !isClose.get(); ) {
      try {
        acquired = semaphore.tryAcquire(timeout.getDuration(), timeout.getUnit());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted onNext " + request, e);
      }
    }
    if (!acquired) {
      throw new IllegalStateException("Failed onNext " + request + ": already closed.");
    }
  }

  @Override
  public void onNext(T request) {
    final StringSupplier requestString = StringSupplier.get(() -> requestToStringFunction.apply(request));
    final TimeDuration timeout = timeoutSupplier.get();
    acquire(requestString, timeout);
    observer.onNext(request);
    final int id = requestCount.incrementAndGet();
    LOG.debug("{}: send {} with timeout={}: {}", name, id, timeout, requestString);
    scheduler.onTimeout(timeout, () -> handleTimeout(id, timeout, requestString),
        LOG, () -> name + ": Timeout check failed for request: " + requestString);
  }

  private void handleTimeout(int id, TimeDuration timeout, StringSupplier request) {
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
