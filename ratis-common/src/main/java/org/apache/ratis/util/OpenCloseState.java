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
package org.apache.ratis.util;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The state of objects that can be unopened, opened, closed or exception.
 */
public class OpenCloseState {
  private static class OpenTrace extends Throwable {
    OpenTrace(String message) {
      super(message);
    }
  }

  private static class CloseTrace extends Throwable {
    CloseTrace(String message) {
      super(message);
    }
  }

  private final String name;
  private final Throwable initTrace;
  /**
   * The referenced {@link Throwable} indicates the state:
   *   InitTrace: unopened
   *   OpenTrace: opened
   *   CloseTrace: closed
   *   Other {@link Throwable} subclass: exception
   */
  private final AtomicReference<Throwable> state;

  public OpenCloseState(String name) {
    this.name = name;
    this.initTrace = new Throwable("Initialize " + name);
    this.state = new AtomicReference<>(initTrace);
  }

  /**
   * Assert this is open.
   */
  public void assertOpen() {
    final Throwable t = state.get();
    if (!(t instanceof OpenTrace)) {
      final String s = name + " is expected to be opened but it is " + toString(t);
      throw new IllegalStateException(s, t);
    }
  }

  public boolean isUnopened() {
    return state.get() == initTrace;
  }

  public boolean isOpened() {
    return state.get() instanceof OpenTrace;
  }

  public boolean isClosed() {
    return state.get() instanceof CloseTrace;
  }

  public Throwable getThrowable() {
    return state.get();
  }

  /**
   * Transit to open state.
   * The method is NOT idempotent.
   */
  public void open() throws IOException {
    final OpenTrace openTrace = new OpenTrace("Open " + name);
    final Throwable t = state.updateAndGet(previous -> previous == initTrace? openTrace: previous);
    if (t != openTrace) {
      throw new IOException("Failed to open " + name +" since it is " + toString(t));
    }
  }

  private boolean readyToClose(Throwable t) {
    return t == initTrace || t instanceof OpenTrace;
  }

  /**
   * Transit to close state.
   * The method is idempotent.
   *
   * @return true if the state is transited to close as a result of the invocation of this method.
   *         Otherwise, return false since it is already closed.
   *
   * @throws IOException if the current state is not allowed to transit to close.
   */
  public boolean close() throws IOException {
    final Throwable previous = state.getAndUpdate(prev -> readyToClose(prev)? new CloseTrace("Close "+ name): prev);
    if (readyToClose(previous)) {
      return true;
    } else if (previous instanceof CloseTrace) {
      return false; // already closed
    }
    throw new IOException("Failed to close " + name + " since it is " + toString(previous));
  }

  @Override
  public String toString() {
    return toString(state.get());
  }

  private String toString(Throwable t) {
    return t == initTrace? "UNOPENED"
        : t instanceof OpenTrace? "OPENED"
        : t instanceof CloseTrace? "CLOSED"
        : t.toString();
  }
}
