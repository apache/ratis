package org.apache.ratis.server.simulation;

import org.apache.ratis.server.impl.PreAppendLeaderStepDownTest;

public class TestPreAppendLeaderStepDownWithSimulatedRpc
    extends PreAppendLeaderStepDownTest<MiniRaftClusterWithSimulatedRpc>
    implements MiniRaftClusterWithSimulatedRpc.FactoryGet {
}
