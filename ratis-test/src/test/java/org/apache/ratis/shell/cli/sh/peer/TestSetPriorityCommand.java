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
package org.apache.ratis.shell.cli.sh.peer;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.shell.cli.CliUtils;
import org.apache.ratis.util.NetUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** Unit tests for {@link SetPriorityCommand}. */
public class TestSetPriorityCommand {

  @Test
  public void testParseAddressPriorityMapMatchesPeerAddressFromCluster() {
    final RaftPeer peer = CliUtils.parseRaftPeers("127.0.0.1:6000").get(0);
    final Map<String, Integer> map = SetPriorityCommand.parseAddressPriorityMap(
        new String[] {"127.0.0.1:6000|5"});

    final Integer newPriority = map.get(peer.getAddress());
    Assertions.assertNotNull(newPriority);
    Assertions.assertEquals(5, newPriority.intValue());
  }

  @Test
  public void testParseAddressPriorityMapNormalizesIpv6LikeParseRaftPeers() {
    final RaftPeer peer = CliUtils.parseRaftPeers("[::1]:6000").get(0);
    final Map<String, Integer> map = SetPriorityCommand.parseAddressPriorityMap(
        new String[] {"[::1]:6000|2"});
    Assertions.assertEquals(2, map.get(peer.getAddress()));
  }

  @Test
  public void testParseAddressPriorityMapNormalizesHostAliasToCanonicalForm() {
    final String canonical = NetUtils.address2String(
        CliUtils.parseInetSocketAddress("127.0.0.1:6000"));
    final String alias = NetUtils.address2String(
        CliUtils.parseInetSocketAddress("localhost:6000"));
    Assertions.assertEquals(canonical, alias,
        "This test requires localhost to resolve to the same canonical address as 127.0.0.1");

    final RaftPeer peer = CliUtils.parseRaftPeers("127.0.0.1:6000").get(0);
    final Map<String, Integer> map = SetPriorityCommand.parseAddressPriorityMap(
        new String[] {"localhost:6000|3"});
    Assertions.assertEquals(3, map.get(peer.getAddress()));
  }

  @Test
  public void testParseAddressPriorityMapRejectsInvalidFormat() {
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> SetPriorityCommand.parseAddressPriorityMap(new String[] {"127.0.0.1:6000"}));
  }
}
