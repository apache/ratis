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
package org.apache.raft.grpc.client;

import org.apache.raft.client.ClientProtoUtils;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.RaftPeer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static org.apache.raft.grpc.RaftGrpcConfigKeys.RAFT_OUTPUTSTREAM_BUFFER_SIZE_DEFAULT;
import static org.apache.raft.grpc.RaftGrpcConfigKeys.RAFT_OUTPUTSTREAM_BUFFER_SIZE_KEY;

public class RaftOutputStream extends OutputStream {
  /** internal buffer */
  private final byte buf[];
  private int count;
  private long seqNum = 0;
  private final RaftPeer target;
  private final String sourceId;
  private final AppendStreamer streamer;

  private boolean closed = false;

  // TODO change target and recreate AppendStreamer after hitting NotLeaderException/Exception

  public RaftOutputStream(RaftProperties prop, String sourceId, RaftPeer target) {
    final int bufferSize = prop.getInt(RAFT_OUTPUTSTREAM_BUFFER_SIZE_KEY,
        RAFT_OUTPUTSTREAM_BUFFER_SIZE_DEFAULT);
    buf = new byte[bufferSize];
    count = 0;
    this.target = target;
    this.sourceId = sourceId;
    streamer = new AppendStreamer(new RaftClientProtocolClient(target), prop);
  }

  @Override
  public void write(int b) throws IOException {
    checkClosed();
    buf[count++] = (byte)b;
    flushIfNecessary();
  }

  private void flushIfNecessary() throws IOException {
    if(count == buf.length) {
      flushToStreamer();
    }
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    checkClosed();
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    int total = 0;
    while (total < len) {
      int toWrite = Math.min(len - total, buf.length - count);
      System.arraycopy(b, off + total, buf, count, toWrite);
      count += toWrite;
      total += toWrite;
      flushIfNecessary();
    }
  }

  private void flushToStreamer() throws IOException {
    if (count > 0) {
      // wrap the current buffer into a RaftClientRequestProto
      // TODO avoid copy
      RaftClientRequest request = new RaftClientRequest(sourceId,
          target.getId(), seqNum++, () -> Arrays.copyOf(buf, count));
      streamer.write(ClientProtoUtils.toRaftClientRequestProto(request));
      count = 0;
    }
  }

  @Override
  public void flush() throws IOException {
    checkClosed();
    flushToStreamer();
    streamer.flush();
  }

  @Override
  public void close() throws IOException {
    flushToStreamer();
    streamer.close(); // streamer will flush
    this.closed = true;
  }

  @Override
  public String toString() {
    return "RaftOutputStream-" + target;
  }

  private void checkClosed() throws IOException {
    if (closed) {
      throw new IOException(this.toString() + " was closed.");
    }
  }
}
