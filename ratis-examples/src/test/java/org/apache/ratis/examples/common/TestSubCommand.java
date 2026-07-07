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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ratis.examples.common;

import java.util.Collection;
import java.util.Collections;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSubCommand {

  public static Collection<String> data() {
    return Collections.singleton("127.0.0.1:6667");
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testParsePeers(String peers) {
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> SubCommandBase.parsePeers(peers));
  }

  @Test
  public void testParseIpv4Peer() {
    final RaftPeer[] peers = SubCommandBase.parsePeers("n0:127.0.0.1:6000:6001:6002:6003");
    Assertions.assertEquals(1, peers.length);
    Assertions.assertEquals("n0", peers[0].getId().toString());
    Assertions.assertEquals("127.0.0.1:6000", peers[0].getAddress());
    Assertions.assertEquals("127.0.0.1:6001", peers[0].getDataStreamAddress());
    Assertions.assertEquals("127.0.0.1:6002", peers[0].getClientAddress());
    Assertions.assertEquals("127.0.0.1:6003", peers[0].getAdminAddress());
  }

  @Test
  public void testParseIpv6Peer() {
    final RaftPeer[] peers = SubCommandBase.parsePeers("n0:[::1]:6000:6001:6002:6003");
    Assertions.assertEquals(1, peers.length);
    Assertions.assertEquals("n0", peers[0].getId().toString());
    Assertions.assertEquals("[::1]:6000", peers[0].getAddress());
    Assertions.assertEquals("[::1]:6001", peers[0].getDataStreamAddress());
    Assertions.assertEquals("[::1]:6002", peers[0].getClientAddress());
    Assertions.assertEquals("[::1]:6003", peers[0].getAdminAddress());
  }

  @Test
  public void testParseMixedPeers() {
    final RaftPeer[] peers = SubCommandBase.parsePeers("n0:[::1]:6000,n1:127.0.0.1:6001");
    Assertions.assertEquals(2, peers.length);
    Assertions.assertEquals("[::1]:6000", peers[0].getAddress());
    Assertions.assertEquals("127.0.0.1:6001", peers[1].getAddress());
  }
}
