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
package org.apache.ratis.protocol;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.util.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class SetConfigurationRequest extends RaftClientRequest {

  public enum Mode {
    SET_UNCONDITIONALLY,
    ADD
  }

  public static final class Arguments {
    private final List<RaftPeer> serversInNewConf;
    private final List<RaftPeer> listenersInNewConf;
    private final Mode mode;

    private Arguments(List<RaftPeer> serversInNewConf, List<RaftPeer> listenersInNewConf,Mode mode) {
      this.serversInNewConf = Optional.ofNullable(serversInNewConf)
          .map(Collections::unmodifiableList)
          .orElseGet(Collections::emptyList);
      this.listenersInNewConf = Optional.ofNullable(listenersInNewConf)
          .map(Collections::unmodifiableList)
          .orElseGet(Collections::emptyList);
      this.mode = mode;

      Preconditions.assertUnique(serversInNewConf);
      Preconditions.assertUnique(listenersInNewConf);
    }

    public List<RaftPeer> getPeersInNewConf(RaftProtos.RaftPeerRole role) {
      switch (role) {
        case FOLLOWER: return serversInNewConf;
        case LISTENER: return listenersInNewConf;
        default:
          throw new IllegalArgumentException("Unexpected role " + role);
      }
    }

    public List<RaftPeer> getServersInNewConf() {
      return serversInNewConf;
    }

    public Mode getMode() {
      return mode;
    }
    @Override
    public String toString() {
      return getMode()
          + ", servers:" + getPeersInNewConf(RaftProtos.RaftPeerRole.FOLLOWER)
          + ", listeners:" + getPeersInNewConf(RaftProtos.RaftPeerRole.LISTENER);

    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public static class Builder {
      private List<RaftPeer> serversInNewConf;
      private List<RaftPeer> listenersInNewConf = Collections.emptyList();
      private Mode mode = Mode.SET_UNCONDITIONALLY;

      public Builder setServersInNewConf(List<RaftPeer> serversInNewConf) {
        this.serversInNewConf = serversInNewConf;
        return this;
      }

      public Builder setListenersInNewConf(List<RaftPeer> listenersInNewConf) {
        this.listenersInNewConf = listenersInNewConf;
        return this;
      }

      public Builder setServersInNewConf(RaftPeer[] serversInNewConfArray) {
        this.serversInNewConf = Arrays.asList(serversInNewConfArray);
        return this;
      }

      public Builder setListenersInNewConf(RaftPeer[] listenersInNewConfArray) {
        this.listenersInNewConf = Arrays.asList(listenersInNewConfArray);
        return this;
      }

      public Builder setMode(Mode mode) {
        this.mode = mode;
        return this;
      }

      public Arguments build() {
        return new Arguments(serversInNewConf, listenersInNewConf, mode);
      }
    }
  }
  private final Arguments arguments;

  public SetConfigurationRequest(ClientId clientId, RaftPeerId serverId,
      RaftGroupId groupId, long callId, Arguments arguments) {
    super(clientId, serverId, groupId, callId, true, writeRequestType());
    this.arguments = arguments;
  }

  public Arguments getArguments() {
    return arguments;
  }

  @Override
  public String toString() {
    return super.toString() + ", " + getArguments();
  }
}
