package org.apache.ratis.logservice.server;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.Test;

public class TestBaseServer {
  private static class MockServer extends BaseServer {
    public MockServer() {
      super(new ServerOpts());
    }

    @Override
    public void close() {}
  }

  @Test
  public void testDefaultPropertiesAreValid() {
    RaftProperties props = new RaftProperties();
    try (MockServer server = new MockServer()) {
      server.validateRaftProperties(props);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testAutoSnapshotIsInvalid() {
    RaftProperties props = new RaftProperties();
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(props, true);
    try (MockServer server = new MockServer()) {
      server.validateRaftProperties(props);
    }
  }
}
