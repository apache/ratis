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

import org.apache.ratis.server.RaftPeerRoleTracker;

import java.util.concurrent.CompletableFuture;

/**
 * A role-tracker wrapping class that owns resources for the tracker.
 * Closing a tracker by using its {@link AutoCloseable#close()} will release underlying raft resources.
 */
public class RoleTrackerReference extends CompletableFuture<Void> implements AutoCloseable {
  private RaftServerImpl serverImpl;
  private RaftPeerRoleTracker roleTracker;

  /** Creates a new tracker reference wrapped over given role tracker. */
  public RoleTrackerReference(RaftPeerRoleTracker roleTracker) {
    this.roleTracker = roleTracker;
  }

  /** Called internally whenever this tracker is registered to an underlying raft-server-impl. */
  public synchronized void registeredToServer(RaftServerImpl registeredServerImpl) {
    this.serverImpl = registeredServerImpl;
  }

  @Override
  public synchronized void close() throws Exception {
    if (serverImpl != null) {
      serverImpl.removeRoleTracker(roleTracker);
    }
  }
}
