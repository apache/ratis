package org.apache.ratis.examples.counter;

import org.apache.ratis.protocol.*;

import java.util.*;

/**
 * Common Constant across servers and client
 */
public class CounterCommon {
  public static final List<RaftPeer> PEERS = new ArrayList<>(3);

  static {
    PEERS.add(new RaftPeer(RaftPeerId.getRaftPeerId("n1"), "127.0.0.1:6000"));
    PEERS.add(new RaftPeer(RaftPeerId.getRaftPeerId("n2"), "127.0.0.1:6001"));
    PEERS.add(new RaftPeer(RaftPeerId.getRaftPeerId("n3"), "127.0.0.1:6002"));
  }
  private static final UUID CLUSTER_GROUP_ID = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");
  public static final RaftGroup RAFT_GROUP = RaftGroup.valueOf(
      RaftGroupId.valueOf(CounterCommon.CLUSTER_GROUP_ID), PEERS);
}
