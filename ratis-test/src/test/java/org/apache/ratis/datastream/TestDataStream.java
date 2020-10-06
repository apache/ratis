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

package org.apache.ratis.datastream;

import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.client.impl.DataStreamClientImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.DataStreamServerImpl;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.NetUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

public class TestDataStream extends BaseTest {
  static final int MODULUS = 23;

  static byte pos2byte(int pos) {
    return (byte) ('A' + pos%MODULUS);
  }

  class SingleDataStreamStateMachine extends BaseStateMachine {
    final WritableByteChannel channel = new WritableByteChannel() {
      private volatile boolean open = true;

      @Override
      public int write(ByteBuffer src) {
        if (!open) {
          throw new IllegalStateException("Already closed");
        }
        final int remaining = src.remaining();
        for(; src.remaining() > 0; ) {
          Assert.assertEquals(pos2byte(byteWritten), src.get());
          byteWritten++;
        }
        return remaining;
      }

      @Override
      public boolean isOpen() {
        return open;
      }

      @Override
      public void close() {
        open = false;
      }
    };

    final DataStream stream = new DataStream() {
      @Override
      public WritableByteChannel getWritableByteChannel() {
        return channel;
      }

      @Override
      public CompletableFuture<?> cleanUp() {
        try {
          channel.close();
        } catch (Throwable t) {
          return JavaUtils.completeExceptionally(t);
        }
        return CompletableFuture.completedFuture(null);
      }
    };

    @Override
    public CompletableFuture<DataStream> stream(RaftClientRequest request) {
      return CompletableFuture.completedFuture(stream);
    }
  }

  private RaftPeer[] peers;
  private RaftProperties properties;
  private DataStreamServerImpl server;
  private DataStreamClientImpl client;
  private int byteWritten = 0;

  public void setupServer(){
    server = new DataStreamServerImpl(peers[0], new SingleDataStreamStateMachine(), properties, null);
    server.getServerRpc().startServer();
  }

  public void setupClient(){
    client = new DataStreamClientImpl(peers[0], properties, null);
    client.start();
  }

  public void shutDownSetup(){
    client.close();
    server.close();
  }

  @Test
  public void testDataStream(){
    properties = new RaftProperties();
    peers = Arrays.stream(MiniRaftCluster.generateIds(1, 0))
                       .map(RaftPeerId::valueOf)
                       .map(id -> new RaftPeer(id, NetUtils.createLocalServerAddress()))
                       .toArray(RaftPeer[]::new);

    setupServer();
    setupClient();
    runTestDataStream();
  }

  public void runTestDataStream(){
    final int bufferSize = 1024*1024;
    final int bufferNum = 10;
    final DataStreamOutput out = client.stream();

    //send request
    final List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
    futures.add(sendRequest(out, 1024));

    //send data
    final int halfBufferSize = bufferSize/2;
    int dataSize = 0;
    for(int i = 0; i < bufferNum; i++) {
      final int size = halfBufferSize + ThreadLocalRandom.current().nextInt(halfBufferSize);
      final ByteBuffer bf = initBuffer(dataSize, size);
      futures.add(out.streamAsync(bf));
      dataSize += size;
    }

    //join all requests
    for(CompletableFuture<DataStreamReply> f : futures) {
      f.join();
    }
    Assert.assertEquals(dataSize, byteWritten);
    shutDownSetup();
  }

  CompletableFuture<DataStreamReply> sendRequest(DataStreamOutput out, int size) {
    // TODO RATIS-1085: create a RaftClientRequest and put it in the buffer
    final ByteBuffer buffer = initBuffer(0, size);
    return out.streamAsync(buffer);
  }

  static ByteBuffer initBuffer(int offset, int size) {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(size);
    final int length = buffer.capacity();
    buffer.position(0).limit(length);
    for (int j = 0; j < length; j++) {
      buffer.put(pos2byte(offset + j));
    }
    buffer.flip();
    Assert.assertEquals(length, buffer.remaining());
    return buffer;
  }
}
