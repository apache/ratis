package org.apache.ratis.examples.counter;

import org.apache.ratis.*;
import org.apache.ratis.client.*;
import org.apache.ratis.examples.*;
import org.apache.ratis.examples.counter.server.*;
import org.apache.ratis.protocol.*;
import org.junit.*;
import org.junit.runners.*;

import java.io.*;
import java.nio.charset.*;
import java.util.*;

public class TestCounter extends ParameterizedBaseTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    return getMiniRaftClusters(CounterStateMachine.class, 3);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Test
  public void testSeveralCounter() throws IOException, InterruptedException {
    setAndStart(cluster);
    try (final RaftClient client = cluster.createClient()) {
      for (int i = 0; i < 10; i++) {
        client.send(Message.valueOf("INCREMENT"));
      }
      RaftClientReply reply1 = client.sendReadOnly(Message.valueOf("GET"));
      Assert.assertEquals("10",
          reply1.getMessage().getContent().toString(Charset.defaultCharset()));
      for (int i = 0; i < 10; i++) {
        client.send(Message.valueOf("INCREMENT"));
      }
      RaftClientReply reply2 = client.sendReadOnly(Message.valueOf("GET"));
      Assert.assertEquals("20",
          reply2.getMessage().getContent().toString(Charset.defaultCharset()));
      for (int i = 0; i < 10; i++) {
        client.send(Message.valueOf("INCREMENT"));
      }
      RaftClientReply reply3 = client.sendReadOnly(Message.valueOf("GET"));
      Assert.assertEquals("30",
          reply3.getMessage().getContent().toString(Charset.defaultCharset()));
    }
  }
}
