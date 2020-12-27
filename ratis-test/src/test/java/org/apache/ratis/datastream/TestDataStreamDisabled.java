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
package org.apache.ratis.datastream;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.DisabledDataStreamClientFactory;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestDataStreamDisabled extends BaseTest {
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testDataStreamDisabled() {
      final RaftProperties properties = new RaftProperties();
      Assert.assertEquals(SupportedDataStreamType.DISABLED, RaftConfigKeys.DataStream.type(properties, LOG::info));

      final RaftPeer server = RaftPeer.newBuilder().setId("s0").build();
      final RaftClient client = RaftClient.newBuilder()
        .setRaftGroup(RaftGroup.valueOf(RaftGroupId.randomId(), server))
        .setProperties(properties)
        .build();

      exception.expect(UnsupportedOperationException.class);
      exception.expectMessage(DisabledDataStreamClientFactory.class.getName()
          + "$1 does not support streamAsync");
      // stream() will create a header request, thus it will hit UnsupportedOperationException due to
      // DisabledDataStreamFactory.
      client.getDataStreamApi().stream();
  }
}
