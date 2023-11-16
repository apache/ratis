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

import org.apache.ratis.util.Preconditions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public interface SegmentedRaftLogFormat {
  class Internal {
    private static final ByteBuffer HEADER;
    private static final byte TERMINATOR_BYTE = 0;

    static {
      final byte[] bytes = "RaftLog1".getBytes(StandardCharsets.UTF_8);
      final ByteBuffer header = ByteBuffer.allocateDirect(bytes.length);
      header.put(bytes).flip();
      HEADER = header.asReadOnlyBuffer();
    }
  }

  static int getHeaderLength() {
    return Internal.HEADER.remaining();
  }

  static ByteBuffer getHeaderBytebuffer() {
    return Internal.HEADER.duplicate();
  }

  static int matchHeader(byte[] bytes, int offset, int length) {
    Preconditions.assertTrue(length <= getHeaderLength());
    for(int i = 0; i < length; i++) {
      if (bytes[offset + i] != Internal.HEADER.get(i)) {
        return i;
      }
    }
    return length;
  }

  static byte getTerminator() {
    return Internal.TERMINATOR_BYTE;
  }

  static boolean isTerminator(byte b) {
    return b == Internal.TERMINATOR_BYTE;
  }

  static boolean isTerminator(byte[] bytes, int offset, int length) {
    return indexOfNonTerminator(bytes, offset, length) == -1;
  }

  /**
   * @return The index of the first non-terminator if it exists.
   *         Otherwise, return -1, i.e. all bytes are terminator.
   */
  static int indexOfNonTerminator(byte[] bytes, int offset, int length) {
    for(int i = 0; i < length; i++) {
      if (!isTerminator(bytes[offset + i])) {
        return i;
      }
    }
    return -1;
  }
}
