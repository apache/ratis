/**
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
      throw new IllegalArgumentException(s, t);
    }
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

  /**
   * Transit to close state.
   * The method is idempotent.
   */
  public void close() throws IOException {
    final Throwable t = state.updateAndGet(
        previous -> previous instanceof OpenTrace? new CloseTrace("Close "+ name): previous);
    // If t is CloseTrace, t is either the previous or the newly created object.
    if (!(t instanceof CloseTrace)) {
      throw new IOException("Failed to close " + name + " since it is " + toString(t));
    }
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
