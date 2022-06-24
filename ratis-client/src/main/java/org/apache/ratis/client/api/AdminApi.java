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
package org.apache.ratis.client.api;

import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An API to support administration
 * such as setting raft configuration and transferring leadership.
 */
public interface AdminApi {

  class SetConfigurationArguments {
    private List<RaftPeer> serversInNewConf;
    private List<RaftPeer> listenersInNewConf;
    private SetConfigurationRequest.Mode mode;

    private SetConfigurationArguments(List<RaftPeer> serversInNewConf,
        List<RaftPeer> listenersInNewConf, SetConfigurationRequest.Mode mode) {
      this.serversInNewConf = serversInNewConf;
      this.listenersInNewConf = listenersInNewConf;
      this.mode = mode;
    }

    public List<RaftPeer> getServersInNewConf() {
      return serversInNewConf;
    }

    public List<RaftPeer> getListenersInNewConf() {
      return listenersInNewConf;
    }

    public SetConfigurationRequest.Mode getMode() {
      return mode;
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public static class Builder {
      private List<RaftPeer> serversInNewConf;
      private List<RaftPeer> listenersInNewConf = Collections.emptyList();
      private SetConfigurationRequest.Mode mode = SetConfigurationRequest.Mode.SET_UNCONDITIONALLY;

      public Builder setServersInNewConf(List<RaftPeer> serversInNewConf) {
        this.serversInNewConf = serversInNewConf;
        return this;
      }

      public Builder setListenersInNewConf(List<RaftPeer> listenersInNewConf) {
        this.listenersInNewConf = listenersInNewConf;
        return this;
      }

      public Builder setServersInNewConfArray(RaftPeer[] serversInNewConfArray) {
        this.serversInNewConf = Arrays.asList(serversInNewConfArray);
        return this;
      }

      public Builder setListenersInNewConfArray(RaftPeer[] listenersInNewConfArray) {
        this.listenersInNewConf = Arrays.asList(listenersInNewConfArray);
        return this;
      }

      public Builder setMode(SetConfigurationRequest.Mode mode) {
        this.mode = mode;
        return this;
      }

      public SetConfigurationArguments build() {
        return new SetConfigurationArguments(serversInNewConf, listenersInNewConf, mode);
      }
    }
  }

  RaftClientReply setConfiguration(SetConfigurationArguments arguments)
      throws IOException;

  /** The same as setConfiguration(serversInNewConf, Collections.emptyList()). */
  default RaftClientReply setConfiguration(List<RaftPeer> serversInNewConf) throws IOException {
    return setConfiguration(SetConfigurationArguments
            .newBuilder()
            .setServersInNewConf(serversInNewConf)
            .build());
  }

  /** The same as setConfiguration(Arrays.asList(serversInNewConf)). */
  default RaftClientReply setConfiguration(RaftPeer[] serversInNewConf) throws IOException {
    return setConfiguration(SetConfigurationArguments
            .newBuilder()
            .setServersInNewConfArray(serversInNewConf)
            .build());
  }

  /** Set the configuration request to the raft service. */
  default RaftClientReply setConfiguration(List<RaftPeer> serversInNewConf, List<RaftPeer> listenersInNewConf)
      throws IOException {
    return setConfiguration(SetConfigurationArguments
        .newBuilder()
        .setServersInNewConf(serversInNewConf)
        .setListenersInNewConf(listenersInNewConf)
        .build());
  }

  /** The same as setConfiguration(Arrays.asList(serversInNewConf), Arrays.asList(listenersInNewConf)). */
  default RaftClientReply setConfiguration(RaftPeer[] serversInNewConf, RaftPeer[] listenersInNewConf)
      throws IOException {
    return setConfiguration(SetConfigurationArguments
        .newBuilder()
        .setListenersInNewConfArray(serversInNewConf)
        .setListenersInNewConfArray(listenersInNewConf)
        .build());
  }

  /** Transfer leadership to the given server.*/
  RaftClientReply transferLeadership(RaftPeerId newLeader, long timeoutMs) throws IOException;
}