package org.apache.ratis;

import java.io.IOException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos.PauseUnpauseReplyProto;
import org.apache.ratis.proto.RaftProtos.PauseUnpauseRequestProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.Test;

public abstract class PauseUnpauseTest<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {

  public static final int NUM_SERVERS = 3;

  @Test
  public void testPause() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runPauseTest);
  }

  void runPauseTest(CLUSTER cluster) throws InterruptedException {
    RaftTestUtil.waitForLeader(cluster);
    RaftPeerId server = cluster.getFollowers().get(0).getId();

    try (RaftClient raftclient = cluster.createClient()) {
      PauseUnpauseReplyProto replyProto=
          raftclient.pause(
              PauseUnpauseRequestProto.newBuilder().setPause(true).build(),
              server);
      System.out.println("---------: " + replyProto.getSuccess());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
