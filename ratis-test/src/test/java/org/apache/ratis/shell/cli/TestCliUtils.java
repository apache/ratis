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
package org.apache.ratis.shell.cli;

import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.List;

public class TestCliUtils {

  @Test
  public void testParseIpv4Address() {
    final InetSocketAddress addr = CliUtils.parseInetSocketAddress("127.0.0.1:6000");
    Assertions.assertEquals(6000, addr.getPort());
    Assertions.assertTrue(addr.getAddress() instanceof Inet4Address);
  }

  @Test
  public void testParseIpv6Address() {
    final InetSocketAddress addr = CliUtils.parseInetSocketAddress("[::1]:6000");
    Assertions.assertEquals(6000, addr.getPort());
    Assertions.assertTrue(addr.getAddress() instanceof Inet6Address);
  }

  @Test
  public void testParseIpv6Peers() {
    final List<RaftPeer> peers = CliUtils.parseRaftPeers("[::1]:6000,[::1]:6001");
    Assertions.assertEquals(2, peers.size());
    Assertions.assertEquals(6000, CliUtils.parseInetSocketAddress(peers.get(0).getAddress()).getPort());
    Assertions.assertEquals(6001, CliUtils.parseInetSocketAddress(peers.get(1).getAddress()).getPort());
  }

  @Test
  public void testParseMissingPort() {
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> CliUtils.parseInetSocketAddress("127.0.0.1"));
  }
}
