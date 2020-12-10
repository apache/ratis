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
package org.apache.ratis.protocol.exceptions;

import org.apache.ratis.protocol.RaftGroupMemberId;

public class StateMachineException extends RaftException {
  private final boolean leaderShouldStepDown;

  public StateMachineException(RaftGroupMemberId serverId, Throwable cause) {
    // cause.getMessage is added to this exception message as the exception received through
    // RPC call contains similar message but Simulated RPC doesn't. Adding the message
    // from cause to this exception makes it consistent across simulated and other RPC implementations.
    super(cause.getClass().getName() + " from Server " + serverId + ": " + cause.getMessage(), cause);
    this.leaderShouldStepDown = true;
  }

  public StateMachineException(String msg) {
    super(msg);
    this.leaderShouldStepDown = true;
  }

  public StateMachineException(String message, Throwable cause) {
    super(message, cause);
    this.leaderShouldStepDown = true;
  }

  public StateMachineException(RaftGroupMemberId serverId, Throwable cause, boolean leaderShouldStepDown) {
    // cause.getMessage is added to this exception message as the exception received through
    // RPC call contains similar message but Simulated RPC doesn't. Adding the message
    // from cause to this exception makes it consistent across simulated and other RPC implementations.
    super(cause.getClass().getName() + " from Server " + serverId + ": " + cause.getMessage(), cause);
    this.leaderShouldStepDown = leaderShouldStepDown;
  }

  public StateMachineException(String msg, boolean leaderShouldStepDown) {
    super(msg);
    this.leaderShouldStepDown = leaderShouldStepDown;
  }

  public StateMachineException(String message, Throwable cause, boolean leaderShouldStepDown) {
    super(message, cause);
    this.leaderShouldStepDown = leaderShouldStepDown;
  }

  public boolean leaderShouldStepDown() {
    return leaderShouldStepDown;
  }
}
