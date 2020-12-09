package org.apache.ratis.hadooprpc;

import org.apache.ratis.server.impl.PreAppendLeaderStepDownTest;

public class TestPreAppendLeaderStepDownWithHadoopRpc
    extends PreAppendLeaderStepDownTest<MiniRaftClusterWithHadoopRpc>
    implements MiniRaftClusterWithHadoopRpc.Factory.Get {
}
