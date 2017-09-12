package org.apache.ratis.hadooprpc;

import org.apache.ratis.server.impl.ServerInformationBaseTest;

public class TestServerInformationWithHadoopRpc
    extends ServerInformationBaseTest<MiniRaftClusterWithHadoopRpc>
    implements MiniRaftClusterWithHadoopRpc.Factory.Get{
}