package org.apache.ratis.grpc;

import org.apache.ratis.InstallSnapshotFromLeaderTests;

public class TestLeaderInstallSnapshot
extends InstallSnapshotFromLeaderTests<MiniRaftClusterWithGrpc>
implements MiniRaftClusterWithGrpc.FactoryGet {}
