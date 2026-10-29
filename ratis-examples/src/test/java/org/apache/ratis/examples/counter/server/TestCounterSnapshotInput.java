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
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestCounterSnapshotInput {
  @TempDir
  private Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testSnapshotInputOwnership(boolean corrupt) throws Exception {
    try (RaftStorage raftStorage = RaftStorageTestUtils.newRaftStorage(directory.toFile())) {
      final CounterStateMachine machine = new CounterStateMachine();
      machine.getStateMachineStorage().init(raftStorage);
      final File file = machine.getStateMachineStorage().getSnapshotFile(1, 10);
      Files.createDirectories(file.toPath().getParent());
      if (corrupt) {
        Files.write(file.toPath(), new byte[0]);
        Assertions.assertThrows(IOException.class, machine::reinitialize);
      } else {
        try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(file.toPath()))) {
          out.writeInt(42);
        }
        machine.reinitialize();
        Assertions.assertEquals(42, machine.getState().getCounter());
        Assertions.assertEquals(TermIndex.valueOf(1, 10), machine.getLastAppliedTermIndex());
      }
      Files.delete(file.toPath());
    }
  }
}
