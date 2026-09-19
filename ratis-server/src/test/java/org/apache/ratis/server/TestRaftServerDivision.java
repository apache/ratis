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
package org.apache.ratis.server;

import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRaftServerDivision {
  @Test
  void testLeadershipStatusExceptionClasses() {
    Assertions.assertNull(RaftServer.Division.LeadershipStatus.LEADER_READY.getExceptionClass());
    Assertions.assertEquals(LeaderNotReadyException.class,
        RaftServer.Division.LeadershipStatus.LEADER_NOT_READY.getExceptionClass());
    Assertions.assertEquals(LeaderSteppingDownException.class,
        RaftServer.Division.LeadershipStatus.LEADER_STEPPING_DOWN.getExceptionClass());
    Assertions.assertEquals(NotLeaderException.class,
        RaftServer.Division.LeadershipStatus.NOT_LEADER.getExceptionClass());
  }
}
