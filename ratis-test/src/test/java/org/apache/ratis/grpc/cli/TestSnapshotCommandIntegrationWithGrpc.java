package org.apache.ratis.grpc.cli;

import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.shell.cli.sh.SnapshotCommandIntegrationTest;

public class TestSnapshotCommandIntegrationWithGrpc
    extends SnapshotCommandIntegrationTest<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet{
}
