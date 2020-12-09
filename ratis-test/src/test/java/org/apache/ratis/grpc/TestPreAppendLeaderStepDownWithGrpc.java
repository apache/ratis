package org.apache.ratis.grpc;

import org.apache.ratis.server.impl.PreAppendLeaderStepDownTest;

public class TestPreAppendLeaderStepDownWithGrpc
    extends PreAppendLeaderStepDownTest<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {
}
