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
package org.apache.ratis.server.impl;

import org.apache.ratis.BaseTest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.Test;

import java.util.Arrays;

public class TestRaftConfiguration extends BaseTest {
  @Test
  public void testPeerConfiguration() {
    final RaftPeer[] peers = {
        new RaftPeer(RaftPeerId.valueOf("s0")),
        new RaftPeer(RaftPeerId.valueOf("s1")),
        new RaftPeer(RaftPeerId.valueOf("s0")),
    };
    testFailureCase("Duplicated peers", () -> {
      new PeerConfiguration(Arrays.asList(peers));
    }, IllegalArgumentException.class);
  }
}