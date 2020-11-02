/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.datastream;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.NetUtils;
import org.junit.Before;
import org.junit.Test;


public class TestDataStreamNetty extends DataStreamBaseTest {
  @Before
  public void setup() {
    properties = new RaftProperties();
    RaftConfigKeys.DataStream.setType(properties, SupportedDataStreamType.NETTY);
  }

  @Override
  protected RaftServer newRaftServer(RaftPeer peer, RaftProperties properties) {
    final RaftProperties p = new RaftProperties(properties);
    NettyConfigKeys.DataStream.setPort(p,  NetUtils.createSocketAddr(peer.getAddress()).getPort());
    return super.newRaftServer(peer, p);
  }

  @Test
  public void testDataStreamSingleServer() throws Exception {
    runTestDataStream(1, 2, 20, 1_000_000, 10);
    runTestDataStream(1, 2, 20, 1_000, 10_000);
  }

  @Test
  public void testDataStreamMultipleServer() throws Exception {
    runTestDataStream(3, 2, 20, 1_000_000, 100);
    runTestDataStream(3, 2, 20, 1_000, 10_000);
  }
}
