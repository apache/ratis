package org.apache.ratis.netty;

import org.apache.ratis.DataStreamTests;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;

public class TestDataStreamWithNetty extends DataStreamTests<MiniRaftClusterWithNetty>
        implements MiniRaftClusterWithNetty.FactoryGetWithDataStreamEnabled {

}
