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
package org.apache.ratis.logservice.api;

import static org.junit.Assert.assertEquals;

import org.apache.ratis.logservice.api.LogMessage.Type;
import org.junit.Test;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

public class TestLogMessage {

  @Test
  public void testLogMessages() throws InvalidProtocolBufferException {

    LogMessage msg = new LogMessage(LogName.of("testLog"));
    assertEquals(Type.READ_REQUEST, msg.getType());
    ByteString content = msg.getContent();
    LogMessage other = LogMessage.parseFrom(content);
    assertEquals(msg.toString(), other.toString());

    msg = new LogMessage(LogName.of("testLog"), 100);
    assertEquals(Type.READ_REPLY, msg.getType());
    content = msg.getContent();
    other = LogMessage.parseFrom(content);
    assertEquals(msg.toString(), other.toString());

    msg = new LogMessage(LogName.of("testLog"), new byte[] { 0, 0, 0 });
    assertEquals(Type.WRITE, msg.getType());
    content = msg.getContent();
    other = LogMessage.parseFrom(content);
    assertEquals(msg.toString(), other.toString());
    assertEquals(msg.getData().length, other.getData().length);

  }


}
