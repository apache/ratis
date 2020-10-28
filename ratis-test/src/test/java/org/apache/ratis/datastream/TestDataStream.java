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

import java.io.IOException;
import java.util.stream.Collectors;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.client.impl.DataStreamClientImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.server.DataStreamServerRpc;
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

  static class SingleDataStreamStateMachine extends BaseStateMachine {
    private int byteWritten = 0;
    private RaftClientRequest writeRequest;

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
          byteWritten += 1;
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
      writeRequest = request;
      return CompletableFuture.completedFuture(stream);
    }

    public int getByteWritten() {
      return byteWritten;
    }

    public RaftClientRequest getWriteRequest() {
      return writeRequest;
    }
  }

  private List<RaftPeer> peers;
  private RaftProperties properties;
  private List<DataStreamServerImpl> servers;
  private DataStreamClientImpl client;
  private List<SingleDataStreamStateMachine> singleDataStreamStateMachines;

  private void setupServer(){
    servers = new ArrayList<>(peers.size());
    singleDataStreamStateMachines = new ArrayList<>(peers.size());
    // start stream servers on raft peers.
    for (int i = 0; i < peers.size(); i++) {
      SingleDataStreamStateMachine singleDataStreamStateMachine = new SingleDataStreamStateMachine();
      singleDataStreamStateMachines.add(singleDataStreamStateMachine);
      final DataStreamServerImpl streamServer = new DataStreamServerImpl(
          peers.get(i), singleDataStreamStateMachine, properties, null);
      final DataStreamServerRpc rpc = streamServer.getServerRpc();
      if (i == 0) {
        // only the first server routes requests to peers.
        List<RaftPeer> otherPeers = new ArrayList<>(peers);
        otherPeers.remove(peers.get(i));
        rpc.addRaftPeers(otherPeers);
      }
      rpc.start();
      servers.add(streamServer);
    }
  }

  private void setupClient(){
    client = new DataStreamClientImpl(peers.get(0), properties, null);
    client.start();
  }

  public void shutdown() throws IOException {
    client.close();
    for (DataStreamServerImpl server : servers) {
      server.close();
    }
  }

  @Test
  public void testDataStreamSingleServer() throws Exception {
    runTestDataStream(1, 1_000_000, 100);
    runTestDataStream(1,1_000, 10_000);
  }

  @Test
  public void testDataStreamMultipleServer() throws Exception {
    runTestDataStream(3, 1_000_000, 100);
    runTestDataStream(3, 1_000, 10_000);
  }

  void runTestDataStream(int numServers, int bufferSize, int bufferNum) throws Exception {
    properties = new RaftProperties();
    peers = Arrays.stream(MiniRaftCluster.generateIds(numServers, 0))
        .map(RaftPeerId::valueOf)
        .map(id -> new RaftPeer(id, NetUtils.createLocalServerAddress()))
        .collect(Collectors.toList());

    setupServer();
    setupClient();
    try {
      runTestDataStream(bufferSize, bufferNum);
    } finally {
      shutdown();
    }
  }

  void runTestDataStream(int bufferSize, int bufferNum) {
    final DataStreamOutput out = client.stream();
    DataStreamClientImpl.DataStreamOutputImpl impl = (DataStreamClientImpl.DataStreamOutputImpl) out;

    final List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
    final List<Integer> sizes = new ArrayList<>();

    //send data
    final int halfBufferSize = bufferSize/2;
    int dataSize = 0;
    for(int i = 0; i < bufferNum; i++) {
      final int size = halfBufferSize + ThreadLocalRandom.current().nextInt(halfBufferSize);
      sizes.add(size);

      final ByteBuffer bf = initBuffer(dataSize, size);
      futures.add(out.writeAsync(bf));
      dataSize += size;
    }

    { // check header
      final DataStreamReply reply = impl.getHeaderFuture().join();
      Assert.assertTrue(reply.isSuccess());
      Assert.assertEquals(0, reply.getBytesWritten());
      Assert.assertEquals(reply.getType(), Type.STREAM_HEADER);
    }

    // check writeAsync requests
    for(int i = 0; i < futures.size(); i++) {
      final DataStreamReply reply = futures.get(i).join();
      Assert.assertTrue(reply.isSuccess());
      Assert.assertEquals(sizes.get(i).longValue(), reply.getBytesWritten());
      Assert.assertEquals(reply.getType(), Type.STREAM_DATA);
    }

    for (SingleDataStreamStateMachine s : singleDataStreamStateMachines) {
      RaftClientRequest writeRequest = s.getWriteRequest();
      if (writeRequest.getClientId().equals(impl.getHeader().getClientId())) {
        Assert.assertEquals(writeRequest.getCallId(), impl.getHeader().getCallId());
        Assert.assertEquals(writeRequest.getRaftGroupId(), impl.getHeader().getRaftGroupId());
        Assert.assertEquals(writeRequest.getServerId(), impl.getHeader().getServerId());
      }
      Assert.assertEquals(dataSize, s.getByteWritten());
    }
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
