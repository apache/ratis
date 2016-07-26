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
package org.apache.hadoop.raft.protocol;

import org.apache.hadoop.raft.util.RaftUtils;
import org.apache.hadoop.raft.proto.RaftProtos.ConfigurationMessageProto;

import java.util.Arrays;

public class ConfigurationMessage implements Message {
  private final RaftPeer[] members;

  public ConfigurationMessage(RaftPeer[] members) {
    this.members = members;
  }

  public RaftPeer[] getMembers() {
    return members;
  }

  @Override
  public byte[] getContent() {
    return ConfigurationMessageProto.newBuilder()
        .addAllPeers(RaftUtils.convertPeersToProtos(Arrays.asList(members)))
        .build().toByteArray();
  }
}
