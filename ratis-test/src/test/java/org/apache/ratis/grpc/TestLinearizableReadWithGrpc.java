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
package org.apache.ratis.grpc;

import org.apache.ratis.LinearizableReadTests;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;

import static org.apache.ratis.ReadOnlyRequestTests.assertOption;
import static org.apache.ratis.server.RaftServerConfigKeys.Read.Option.LINEARIZABLE;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestLinearizableReadWithGrpc
  extends LinearizableReadTests<MiniRaftClusterWithGrpc>
  implements MiniRaftClusterWithGrpc.FactoryGet {

  @Override
  public boolean isLeaderLeaseEnabled() {
    return false;
  }

  @Override
  public void assertRaftProperties(RaftProperties p) {
    assertOption(LINEARIZABLE, p);
    assertFalse(RaftServerConfigKeys.Read.leaderLeaseEnabled(p));
    assertFalse(isLeaderLeaseEnabled());
  }
}
