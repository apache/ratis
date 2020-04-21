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
package org.apache.ratis.logservice.tool;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogServiceClient;
import org.apache.ratis.logservice.tool.VerificationTool.Operation;
import org.junit.Test;

public class TestVerificationTool {

  private static class MockOperation extends Operation {

    MockOperation(LogName logName, LogServiceClient client, int numRecords, int logFreq, int valueSize) {
      super(logName, client, numRecords, logFreq, valueSize);
    }

    @Override public void run() {
      throw new UnsupportedOperationException();
    }
  }

  Operation createOp(int valueSize) {
    return new MockOperation(null, null, 1, 1, valueSize);
  }

  @Test
  public void testValueSerialization() {
    // Test that, for value sizes from 0 to 50 bytes, we can generate correct values.
    for (int i = 0; i < 50; i++) {
      Operation op = createOp(i);
      String value = VerificationTool.MESSAGE_PREFIX + i;
      ByteBuffer serialized = op.createValue(value);
      assertEquals(i, serialized.limit() - serialized.arrayOffset());
      assertEquals(value.substring(0, Math.min(i, value.length())), op.parseValue(serialized));
    }
    
  }

}
