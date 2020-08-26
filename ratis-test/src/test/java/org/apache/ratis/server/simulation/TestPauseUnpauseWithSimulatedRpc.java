package org.apache.ratis.server.simulation;

import org.apache.ratis.PauseUnpauseTest;

public class TestPauseUnpauseWithSimulatedRpc
    extends PauseUnpauseTest<MiniRaftClusterWithSimulatedRpc>
    implements MiniRaftClusterWithSimulatedRpc.FactoryGet {

}
