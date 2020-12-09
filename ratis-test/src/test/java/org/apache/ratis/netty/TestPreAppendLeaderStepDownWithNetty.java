package org.apache.ratis.netty;

import org.apache.ratis.server.impl.PreAppendLeaderStepDownTest;

public class TestPreAppendLeaderStepDownWithNetty
    extends PreAppendLeaderStepDownTest<MiniRaftClusterWithNetty>
    implements MiniRaftClusterWithNetty.FactoryGet {
}
