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
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerMXBean;
import org.apache.ratis.util.JmxRegister;

import javax.management.ObjectName;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** JMX for {@link RaftServerImpl}. */
class RaftServerJmxAdapter extends JmxRegister implements RaftServerMXBean {
  static boolean registerMBean(String id, String groupId, RaftServerMXBean mBean, JmxRegister jmx) {
    final String prefix = "Ratis:service=RaftServer,group=" + groupId + ",id=";
    final String registered = jmx.register(mBean, Arrays.asList(
        () -> prefix + id,
        () -> prefix + ObjectName.quote(id)));
    return registered != null;
  }

  private final RaftServerImpl server;

  RaftServerJmxAdapter(RaftServerImpl server) {
    this.server = server;
  }

  boolean registerMBean() {
    return registerMBean(getId(), getGroupId(), this, this);
  }

  @Override
  public String getId() {
    return server.getId().toString();
  }

  @Override
  public String getLeaderId() {
    RaftPeerId leaderId = server.getState().getLeaderId();
    if (leaderId != null) {
      return leaderId.toString();
    } else {
      return null;
    }
  }

  @Override
  public long getCurrentTerm() {
    return server.getState().getCurrentTerm();
  }

  @Override
  public String getGroupId() {
    return server.getMemberId().getGroupId().toString();
  }

  @Override
  public String getRole() {
    return server.getRole().toString();
  }

  @Override
  public List<String> getFollowers() {
    return server.getRole().getLeaderState()
        .map(LeaderStateImpl::getFollowers)
        .orElseGet(Stream::empty)
        .map(RaftPeer::toString)
        .collect(Collectors.toList());
  }

  @Override
  public List<String> getGroups() {
    return server.getRaftServer().getGroupIds().stream()
        .map(RaftGroupId::toString)
        .collect(Collectors.toList());
  }
}
