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

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestDataStreamDisabled extends TestDataStreamBase {
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setup() {
    properties = new RaftProperties();
    RaftConfigKeys.DataStream.setType(properties, SupportedDataStreamType.DISABLED);
  }

  @Test
  public void testDataStreamDisabled() throws Exception {
    setupRaftPeers(1);
    try {
      setupServer();
      setupClient();
      exception.expect(UnsupportedOperationException.class);
      exception.expectMessage("org.apache.ratis.server.impl.DisabledDataStreamFactory$1 does not support streamAsync");
      // stream() will create a header request, thus it will hit UnsupportedOperationException due to
      // DisabledDataStreamFactory.
      client.stream();
    } finally {
      shutdown();
    }
  }
}
