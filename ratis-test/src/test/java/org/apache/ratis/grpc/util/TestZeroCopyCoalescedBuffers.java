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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.ratis.BaseTest;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.HasByteBuffer;
import org.apache.ratis.thirdparty.io.grpc.internal.CompositeReadableBuffer;
import org.apache.ratis.thirdparty.io.grpc.internal.ReadableBuffers;
import org.junit.jupiter.api.Test;

/**
 * gRPC coalesces small buffers of a message into an array (see {@link CompositeReadableBuffer}), which has no
 * {@link ByteBuffer}; {@link ZeroCopyMessageMarshaller} must still not copy the rest of the message.
 */
public class TestZeroCopyCoalescedBuffers extends BaseTest {
  private static AppendEntriesRequestProto newRequest(int dataSize) {
    final byte[] data = new byte[dataSize];
    new Random(1).nextBytes(data);
    final StateMachineLogEntryProto smLogEntry = StateMachineLogEntryProto.newBuilder()
        .setLogData(ByteString.copyFromUtf8("write"))
        .setStateMachineEntry(StateMachineEntryProto.newBuilder().setStateMachineData(ByteString.copyFrom(data)))
        .build();
    final LogEntryProto entry = LogEntryProto.newBuilder().setTerm(1).setIndex(2)
        .setStateMachineLogEntry(smLogEntry).build();
    return AppendEntriesRequestProto.newBuilder().setLeaderTerm(1).addEntries(entry).build();
  }

  private static ByteBuffer direct(byte[] bytes, int from, int to) {
    final ByteBuffer b = ByteBuffer.allocateDirect(to - from);
    b.put(bytes, from, to - from).flip();
    return b;
  }

  /** The message in direct buffers, with two small ones in the middle, which gRPC coalesces into an array. */
  private static InputStream newStream(byte[] bytes) {
    final int mid = bytes.length / 2;
    final CompositeReadableBuffer composite = new CompositeReadableBuffer();
    composite.addBuffer(ReadableBuffers.wrap(direct(bytes, 0, mid)));
    composite.addBuffer(ReadableBuffers.wrap(direct(bytes, mid, mid + 2)));
    composite.addBuffer(ReadableBuffers.wrap(direct(bytes, mid + 2, mid + 4)));
    composite.addBuffer(ReadableBuffers.wrap(direct(bytes, mid + 4, bytes.length)));
    return ReadableBuffers.openStream(composite, true);
  }

  @Test
  public void testZeroCopyWithCoalescedBuffers() throws Exception {
    final AppendEntriesRequestProto request = newRequest(300 << 10);
    final InputStream stream = newStream(request.toByteArray());
    // the coalesced array has no ByteBuffer
    assertFalse(((HasByteBuffer) stream).byteBufferSupported());

    final ZeroCopyMessageMarshaller<AppendEntriesRequestProto> marshaller
        = new ZeroCopyMessageMarshaller<>(AppendEntriesRequestProto.getDefaultInstance());
    final AppendEntriesRequestProto parsed = marshaller.parse(stream);
    assertEquals(request, parsed);

    // not copied: the state machine data is still in the direct buffers
    final ByteString data = parsed.getEntries(0).getStateMachineLogEntry().getStateMachineEntry()
        .getStateMachineData();
    for (ByteBuffer b : data.asReadOnlyByteBufferList()) {
      assertTrue(b.isDirect(), "copied to " + b);
    }

    marshaller.release(parsed);
    marshaller.assertNoUnclosedStreams();
  }
}
