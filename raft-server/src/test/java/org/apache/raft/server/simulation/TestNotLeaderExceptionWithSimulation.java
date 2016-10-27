package org.apache.raft.server.simulation;

import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftNotLeaderExceptionBaseTest;
import org.apache.raft.conf.RaftProperties;

import java.io.IOException;

public class TestNotLeaderExceptionWithSimulation extends RaftNotLeaderExceptionBaseTest {
  @Override
  public MiniRaftCluster initCluster() throws IOException {
    String[] s = MiniRaftCluster.generateIds(NUM_PEERS, 0);
    return new MiniRaftClusterWithSimulatedRpc(s, new RaftProperties(), true);
  }
}
