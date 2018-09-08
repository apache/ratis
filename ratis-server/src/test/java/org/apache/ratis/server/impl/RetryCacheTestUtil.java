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
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;

public class RetryCacheTestUtil {
  public static RetryCache createRetryCache(){
    return new RetryCache(5000, TimeDuration.valueOf(60, TimeUnit.SECONDS));
  }

  public static void createEntry(RetryCache cache, RaftProtos.LogEntryProto logEntry){
    if(logEntry.getLogEntryBodyCase() == RaftProtos.LogEntryProto.LogEntryBodyCase.SMLOGENTRY){
      ClientId clientId = ClientId.valueOf(logEntry.getClientId());
      long callId = logEntry.getCallId();
      cache.getOrCreateEntry(clientId, callId);
    }
  }

  public static void assertFailure(RetryCache cache,
      RaftProtos.LogEntryProto logEntry, boolean isFailed) {
    if(logEntry.getLogEntryBodyCase() == RaftProtos.LogEntryProto.LogEntryBodyCase.SMLOGENTRY){
      ClientId clientId = ClientId.valueOf(logEntry.getClientId());
      long callId = logEntry.getCallId();
      Assert.assertEquals(isFailed, cache.get(clientId, callId).isFailed());
    }
  }

  public static void getOrCreateEntry(RetryCache cache, ClientId clientId, long callId){
    cache.getOrCreateEntry(clientId, callId);
  }
}
