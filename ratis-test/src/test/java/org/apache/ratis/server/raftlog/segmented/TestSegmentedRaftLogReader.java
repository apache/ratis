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
package org.apache.ratis.server.raftlog.segmented;

import org.apache.ratis.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/** Tests for {@link SegmentedRaftLogReader}. */
public class TestSegmentedRaftLogReader extends BaseTest {
  @Test
  public void testLimitedInputStreamPosition() throws IOException {
    final byte[] data = {1, 2, 3, 4, 5};
    try (SegmentedRaftLogReader.LimitedInputStream in =
        new SegmentedRaftLogReader.LimitedInputStream(new ByteArrayInputStream(data))) {
      in.mark(data.length);
      final byte[] buffer = new byte[data.length];
      Assertions.assertEquals(data.length, in.read(buffer));
      Assertions.assertArrayEquals(data, buffer);
      Assertions.assertEquals(data.length, in.getPos());

      in.reset();
      try(DataInputStream dis = new DataInputStream(in)) {
        Assertions.assertEquals(data.length, dis.read(buffer));
        Assertions.assertArrayEquals(data, buffer);
        Assertions.assertEquals(data.length, in.getPos());
      }
    }
  }
}
