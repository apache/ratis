package org.apache.ratis.grpc;

import org.apache.ratis.ReadOnlyRequestTests;

public class TestReadOnlyRequestsWithGrpc
        extends ReadOnlyRequestTests<MiniRaftClusterWithGrpc>
        implements MiniRaftClusterWithGrpc.FactoryGet {

}
