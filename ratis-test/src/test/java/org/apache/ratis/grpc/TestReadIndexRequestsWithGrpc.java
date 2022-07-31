package org.apache.ratis.grpc;

import org.apache.ratis.ReadIndexRequestTests;
import org.apache.ratis.WatchRequestTests;

public class TestReadIndexRequestsWithGrpc
        extends ReadIndexRequestTests<MiniRaftClusterWithGrpc>
        implements MiniRaftClusterWithGrpc.FactoryGet {

}
