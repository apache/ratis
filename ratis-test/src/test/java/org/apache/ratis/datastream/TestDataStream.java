package org.apache.ratis.datastream;

import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.client.impl.DataStreamClientImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.DataStreamServerImpl;
import org.apache.ratis.util.NetUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class TestDataStream extends BaseTest {

  private RaftPeer[] peers;
  private RaftProperties properties;
  private DataStreamServerImpl server;
  private DataStreamClientImpl client;
  private List<CompletableFuture<DataStreamReply>> futures = new ArrayList<>();

  public void setupServer(){
    server = new DataStreamServerImpl(peers[0], properties, null);
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
    int i = 0;
    while(i < 10000){
      bf.position(0).limit(bf.capacity());
      futures.add(stream.streamAsync(bf));
      i++;
    }
    shutDownSetup();
    for(i = 0; i < futures.size(); i++){
      Assert.assertTrue(futures.get(i).isDone());
    }
  }
}
