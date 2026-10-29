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
package org.apache.ratis.examples.filestore;

import org.apache.ratis.BaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;

public class TestFileStore extends BaseTest {
  @Test
  public void testDirectoryConfiguration() {
    final RaftProperties properties = new RaftProperties();
    final RaftPeerId id = RaftPeerId.valueOf("s0");
    final NullPointerException exception = Assertions.assertThrows(NullPointerException.class,
        () -> new FileStore(() -> id, properties));
    Assertions.assertEquals(FileStoreCommon.STATEMACHINE_DIR_KEY + " is not set.", exception.getMessage());

    properties.setFile(FileStoreCommon.STATEMACHINE_DIR_KEY, new File(getClassTestDir(), "filestore"));
    try (FileStore store = new FileStore(() -> id, properties)) {
      Assertions.assertEquals(id, store.getId());
    }
  }
}
