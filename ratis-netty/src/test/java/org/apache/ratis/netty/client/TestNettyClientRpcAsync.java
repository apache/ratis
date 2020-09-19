package org.apache.ratis.netty.client;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.impl.RaftClientTestUtil;
import org.apache.ratis.netty.MiniRaftClusterWithNetty;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.util.ProtoUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestNettyClientRpcAsync extends BaseTest implements MiniRaftClusterWithNetty.FactoryGet {

  @Test
  public void testClientAsyncApi() throws Exception {
    runWithNewCluster(1, this::runTestClientAsyncApi);
  }

  public void runTestClientAsyncApi(MiniRaftCluster cluster)
      throws Exception {
    final RaftServerImpl leader = RaftTestUtil.waitForLeader(cluster);

    try (final RaftClient client = cluster.createClient()) {
      final RaftClientRpc rpc = client.getClientRpc();

      final AtomicLong seqNum = new AtomicLong();
      {
        // send a request using rpc directly
        final RaftClientRequest request = newRaftClientRequest(client, leader.getId(),
            seqNum.incrementAndGet());
        final CompletableFuture<RaftClientReply> f = rpc.sendRequestAsync(request);
        Assert.assertTrue(f.get().isSuccess());
      }
    }
  }

  static RaftClientRequest newRaftClientRequest(RaftClient client, RaftPeerId serverId, long seqNum) {
    final SimpleMessage m = new SimpleMessage("m" + seqNum);
    return RaftClientTestUtil.newRaftClientRequest(client, serverId, seqNum, m,
        RaftClientRequest.writeRequestType(), ProtoUtils.toSlidingWindowEntry(seqNum, seqNum == 1L));
  }
}
