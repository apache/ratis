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
import org.apache.ratis.util.NetUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class TestDataStream extends BaseTest {
  static class SingleDataStreamStateMachine extends BaseStateMachine {
    final WritableByteChannel channel = new WritableByteChannel() {
      private volatile boolean open = true;

      @Override
      public int write(ByteBuffer src) {
        if (!open) {
          throw new IllegalStateException("Already closed");
        }
        final int remaining = src.remaining();
        LOG.info("write {}", remaining);
        src.position(src.position() + remaining);
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
    peers = Arrays.asList(MiniRaftCluster.generateIds(1, 0)).stream()
                       .map(RaftPeerId::valueOf)
                       .map(id -> new RaftPeer(id, NetUtils.createLocalServerAddress()))
                       .toArray(RaftPeer[]::new);

    setupServer();
    setupClient();
    runTestDataStream();
  }

  public void runTestDataStream(){
    DataStreamOutput stream = client.stream();
    ByteBuffer bf = ByteBuffer.allocateDirect(1024*1024);
    for (int i = 0; i < bf.capacity(); i++) {
      bf.put((byte)'a');
    }
    bf.flip();

    final List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();
    for(int i = 0; i < 10; i++) {
      bf.position(0).limit(bf.capacity());
      futures.add(stream.streamAsync(bf));
    }
    try {
      Thread.sleep(1000*3);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    shutDownSetup();
    for(int i = 0; i < futures.size(); i++){
      Assert.assertTrue(futures.get(i).isDone());
    }
  }
}
