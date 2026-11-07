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
package org.apache.ratis.examples.counter.server;

import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCounterSnapshotOutput {
  @Test
  public void testCloseOnSnapshotWriteFailure() throws IOException {
    final AtomicInteger closed = new AtomicInteger();
    final OutputStream sink = new OutputStream() {
      @Override
      public void write(int value) throws IOException {
        throw new IOException("injected snapshot write failure");
      }

      @Override
      public void close() {
        closed.incrementAndGet();
      }
    };
    final File file = new File("failed-counter-snapshot");
    final CounterStateMachine machine = new CounterStateMachine();
    final CounterStateMachine.CounterState state = new CounterStateMachine.CounterState(TermIndex.valueOf(1, 1), 42);
    try (MockedStatic<Files> ignored = Mockito.mockStatic(Files.class, invocation -> {
      if ("newOutputStream".equals(invocation.getMethod().getName())
          && file.toPath().equals(invocation.getArgument(0))) {
        return sink;
      }
      return invocation.callRealMethod();
    })) {
      Assertions.assertThrows(IOException.class, () -> machine.saveSnapshot(state, file));
    }
    Assertions.assertEquals(1, closed.get(), "snapshot output must be closed after a failed flush");
  }
}
