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
package org.apache.ratis.protocol;

import org.apache.ratis.BaseTest;
import org.junit.Assert;
import org.junit.Test;

public class TestRoutingTable extends BaseTest {
  @Override
  public int getGlobalTimeoutSeconds() {
    return 1;
  }

  private final RaftPeerId[] peers = new RaftPeerId[10];

  {
    for(int i = 0; i < peers.length; i++) {
      peers[i] = RaftPeerId.valueOf("s" + i);
    }
  }

  @Test
  public void testRoutingTableValidation() {
    { // empty table
      newRoutingTable();
    }

    { // 0 -> 1 -> 2
      newRoutingTable(0, 1, 1, 2);
    }

    { // 0 -> 1, 0 -> 2
      newRoutingTable(0, 1, 0, 2);
    }

    testFailureCase(" #edges < #vertices - 1", 0, 1, 1, 2, 3, 4);

    testFailureCase(" #edges > #vertices - 1", 0, 1, 1, 2, 2, 0);

    testFailureCase(">1 predecessors", 0, 1, 1, 2, 3, 4, 4, 1);

    testFailureCase("unreachable", 0, 1, 1, 2, 2, 0, 3, 4);

    testFailureCase("self-loop", 0, 1, 2, 3, 3, 3);
  }

  RoutingTable newRoutingTable(int... peerIndices) {
    final RoutingTable.Builder b = RoutingTable.newBuilder();
    for (int i = 0; i < peerIndices.length; i += 2) {
      b.addSuccessor(peers[peerIndices[i]], peers[peerIndices[i + 1]]);
    }
    return b.build();
  }

  void testFailureCase(String name, int... peerIndices) {
    Assert.assertEquals(0, peerIndices.length % 2);

    testFailureCase(name + ": " + toString(peerIndices),
        () -> newRoutingTable(peerIndices),
        IllegalStateException.class, LOG);
  }

  String toString(int... peerIndices) {
    Assert.assertEquals(0, peerIndices.length % 2);
    if (peerIndices.length == 0) {
      return "<empty>";
    }
    final StringBuilder b = new StringBuilder();
    b.append(peerIndices[0]).append("->").append(peerIndices[1]);
    for (int i = 2; i < peerIndices.length; i += 2) {
      b.append(", ").append(peerIndices[i]).append("->").append(peerIndices[i + 1]);
    }
    return b.toString();
  }
}
