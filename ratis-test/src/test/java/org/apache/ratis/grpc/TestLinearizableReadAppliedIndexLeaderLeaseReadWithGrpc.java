package org.apache.ratis.grpc;

public class TestLinearizableReadAppliedIndexLeaderLeaseReadWithGrpc
    extends TestLinearizableLeaderLeaseReadWithGrpc {

  @Override
  public boolean readIndexUseAppliedIndex() {
    return true;
  }
}
