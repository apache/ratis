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
package org.apache.ratis.server;

import java.io.IOException;
import org.apache.ratis.BaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.Preconditions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test {@link RaftServer.Builder}.
 */
public class ServerBuilderTest extends BaseTest {

    @Test
    public void testPeerIdInRaftGroup() throws Exception {
        RaftPeer peer = RaftPeer.newBuilder().setId("n0").build();
        RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), peer);
        RaftServer server = RaftServer.newBuilder()
            .setServerId(peer.getId())
            .setGroup(group)
            .setStateMachine(new BaseStateMachine())
            .setProperties(new RaftProperties())
            .build();
        server.close();
    }

    @Test
    public void testPeerIdNotInRaftGroup() {
        RaftPeer peer = RaftPeer.newBuilder().setId("n0").build();
        RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), peer);
        try {
            RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf("n1"))
                .setGroup(group)
                .setStateMachine(new BaseStateMachine())
                .setProperties(new RaftProperties())
                .build();
            Assertions.fail("did not get expected exception");
        } catch (IOException e) {
            Preconditions.assertInstanceOf(e.getCause(), IllegalStateException.class);
        }
    }

    @Test
    public void testNullPeerIdWithRaftGroup() {
        RaftPeer peer = RaftPeer.newBuilder().setId("n0").build();
        RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), peer);
        try {
            RaftServer.newBuilder()
                .setGroup(group)
                .setStateMachine(new BaseStateMachine())
                .setProperties(new RaftProperties())
                .build();
            Assertions.fail("did not get expected exception");
        } catch (IOException e) {
            Preconditions.assertInstanceOf(e.getCause(), IllegalStateException.class);
        }
    }

    @Test
    public void testPeerIdWithNullRaftGroup() throws Exception {
        RaftPeer peer = RaftPeer.newBuilder().setId("n0").build();
        RaftServer server = RaftServer.newBuilder()
            .setServerId(peer.getId())
            .setStateMachine(new BaseStateMachine())
            .setProperties(new RaftProperties())
            .build();
        server.close();
    }

    @Test
    public void testNullPeerIdWithNullRaftGroup() throws Exception {
        RaftServer server = RaftServer.newBuilder()
            .setStateMachine(new BaseStateMachine())
            .setProperties(new RaftProperties())
            .build();
        server.close();
    }
}
