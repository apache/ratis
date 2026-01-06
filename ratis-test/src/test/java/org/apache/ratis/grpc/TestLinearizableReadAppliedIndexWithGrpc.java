package org.apache.ratis.grpc;

public class TestLinearizableReadAppliedIndexWithGrpc
  extends TestLinearizableReadWithGrpc {

  @Override
  public boolean readIndexUseAppliedIndex() {
    return true;
  }
}
