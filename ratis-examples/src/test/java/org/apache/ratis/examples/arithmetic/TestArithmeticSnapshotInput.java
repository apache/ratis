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
package org.apache.ratis.examples.arithmetic;

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TestArithmeticSnapshotInput {
  @TempDir
  private Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"accepted", "truncated", "rejected-class"})
  public void testSnapshotInputOwnership(String mode) throws Exception {
    final Path path = Files.createFile(directory.resolve(SimpleStateMachineStorage.getSnapshotFileName(1, 10)));
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject("rejected-class".equals(mode) ? new HashMap<String, Double>()
          : Collections.<String, Double>emptyMap());
    }
    final AtomicInteger closed = new AtomicInteger();
    final InputStream source = new FilterInputStream(
        new ByteArrayInputStream("truncated".equals(mode) ? new byte[0] : bytes.toByteArray())) {
      @Override
      public void close() throws IOException {
        closed.incrementAndGet();
        super.close();
      }
    };
    final ArithmeticStateMachine machine = new ArithmeticStateMachine();
    final SingleFileSnapshotInfo snapshot = new SingleFileSnapshotInfo(
        new FileInfo(path, null), TermIndex.valueOf(1, 10));
    try (MockedStatic<FileUtils> files = Mockito.mockStatic(FileUtils.class)) {
      files.when(() -> FileUtils.newInputStream(path.toFile())).thenReturn(source);
      if (!"accepted".equals(mode)) {
        Assertions.assertThrows(IOException.class, () -> machine.loadSnapshot(snapshot));
      } else {
        Assertions.assertEquals(10L, machine.loadSnapshot(snapshot));
        Assertions.assertEquals(snapshot.getTermIndex(), machine.getLastAppliedTermIndex());
      }
    }
    Assertions.assertTrue(closed.get() > 0, "snapshot input must close even if object-stream construction fails");
  }
}
