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

import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;

import java.io.IOException;

/**
 * APIs to support group management operations such as add, remove, list and info to a particular server.
 */
public interface GroupManagementApi {
  /** Add a new group. */
  RaftClientReply add(RaftGroup newGroup) throws IOException;

  /** Remove a group. */
  RaftClientReply remove(RaftGroupId groupId, boolean deleteDirectory, boolean renameDirectory) throws IOException;

  /** List all the groups.*/
  GroupListReply list() throws IOException;

  /** Get the info of the given group.*/
  GroupInfoReply info(RaftGroupId group) throws IOException;
}