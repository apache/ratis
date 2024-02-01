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
package org.apache.ratis.netty;

import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.LeaderElectionTests;
import org.junit.jupiter.api.Test;

public class TestLeaderElectionWithNetty
    extends LeaderElectionTests<MiniRaftClusterWithNetty>
    implements MiniRaftClusterWithNetty.FactoryGet {
  @Override
  @Test
  public void testEnforceLeader() throws Exception {
    super.testEnforceLeader();

    MiniRaftClusterWithNetty.sendServerRequest.clear();
    BlockRequestHandlingInjection.getInstance().unblockAll();
  }

  @Test
  public void testDisconnectLeader() throws Exception {
    super.testDisconnectLeader();
  }

}
