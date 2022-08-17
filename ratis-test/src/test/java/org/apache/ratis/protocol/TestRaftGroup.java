/**
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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class TestRaftGroup extends BaseTest {
  @Override
  public int getGlobalTimeoutSeconds() {
    return 1;
  }

  @Test(expected = IllegalStateException.class)
  public void testDuplicatePeerId() throws Exception {
    UUID groupId = UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1");

    List<RaftPeer> peers = new LinkedList<>();
    peers.add(RaftPeer.newBuilder().setId("n0").build());
    peers.add(RaftPeer.newBuilder().setId("n0").build());
    RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peers);
  }
}
