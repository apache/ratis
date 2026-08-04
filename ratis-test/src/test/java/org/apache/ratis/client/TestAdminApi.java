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
package org.apache.ratis.client;

import org.apache.ratis.BaseTest;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** Test the default methods of {@link AdminApi}. */
public class TestAdminApi extends BaseTest {
  static AdminApi newCapturingAdminApi(AtomicReference<SetConfigurationRequest.Arguments> captured) {
    return new AdminApi() {
      @Override
      public RaftClientReply setConfiguration(SetConfigurationRequest.Arguments arguments) {
        captured.set(arguments);
        return null;
      }

      @Override
      public RaftClientReply transferLeadership(RaftPeerId newLeader, RaftPeerId leaderId, long timeoutMs) {
        throw new UnsupportedOperationException();
      }
    };
  }

  static RaftPeer newPeer(String id) {
    return RaftPeer.newBuilder().setId(id).build();
  }

  @Test
  public void testSetConfigurationWithArrays() throws Exception {
    final RaftPeer[] servers = {newPeer("s0"), newPeer("s1"), newPeer("s2")};
    final RaftPeer[] listeners = {newPeer("l0")};

    final AtomicReference<SetConfigurationRequest.Arguments> captured = new AtomicReference<>();
    newCapturingAdminApi(captured).setConfiguration(servers, listeners);

    final SetConfigurationRequest.Arguments arguments = captured.get();
    Assertions.assertEquals(Arrays.asList(servers), arguments.getServersInNewConf());
    Assertions.assertEquals(Arrays.asList(listeners), arguments.getPeersInNewConf(RaftPeerRole.LISTENER));
  }

  @Test
  public void testSetConfigurationWithLists() throws Exception {
    final List<RaftPeer> servers = Arrays.asList(newPeer("s0"), newPeer("s1"), newPeer("s2"));
    final List<RaftPeer> listeners = Arrays.asList(newPeer("l0"));

    final AtomicReference<SetConfigurationRequest.Arguments> captured = new AtomicReference<>();
    newCapturingAdminApi(captured).setConfiguration(servers, listeners);

    final SetConfigurationRequest.Arguments arguments = captured.get();
    Assertions.assertEquals(servers, arguments.getServersInNewConf());
    Assertions.assertEquals(listeners, arguments.getPeersInNewConf(RaftPeerRole.LISTENER));
  }
}
