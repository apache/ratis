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
package org.apache.ratis.logservice.server;

import java.util.UUID;

import org.apache.ratis.logservice.common.Constants;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 * Options that are common across metadata and log server classes.
 */
public class ServerOpts {
  /**
   * Converts a string into a UUID
   */
  static class UUIDConverter implements IStringConverter<UUID> {
    @Override public UUID convert(String value) {
      return UUID.fromString(value);
    }
  }

  @Parameter(names = {"-h", "--hostname"}, description = "Hostname")
  private String host = null;

  @Parameter(names = {"-p", "--port"}, description = "Port number")
  private int port = -1;

  @Parameter(names = {"-d", "--dir"}, description = "Working directory")
  private String workingDir = null;

  @Parameter(names = {"-q", "--metaQuorum"}, description = "Metadata Service Quorum", required = true)
  private String metaQuorum = null;

  @Parameter(names = {"--metadataServerGroupId"}, description = "UUID corresponding to the RAFT metadata servers group",
      converter = UUIDConverter.class)
  private UUID metaGroupId = Constants.META_GROUP_UUID;

  @Parameter(names = {"--logServerGroupId"}, description = "UUID corresponding to the RAFT log servers group",
      converter = UUIDConverter.class)
  private UUID logServerGroupId = Constants.SERVERS_GROUP_UUID;

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public boolean isHostSet() {
    return this.host != null;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public boolean isPortSet() {
    return this.port != -1;
  }

  public String getWorkingDir() {
    return workingDir;
  }

  public void setWorkingDir(String workingDir) {
    this.workingDir = workingDir;
  }

  public boolean isWorkingDirSet() {
    return this.workingDir != null;
  }

  public String getMetaQuorum() {
    return metaQuorum;
  }

  public void setMetaQuorum(String metaQuorum) {
    this.metaQuorum = metaQuorum;
  }

  public boolean isMetaQuorumSet() {
    return this.metaQuorum != null;
  }

  public UUID getMetaGroupId() {
    return metaGroupId;
  }

  public void setMetaGroupId(UUID metaGroupId) {
    this.metaGroupId = metaGroupId;
  }

  public boolean isMetaServerGroupIdSet() {
    return this.metaGroupId != null && !this.metaGroupId.equals(Constants.META_GROUP_UUID);
  }

  public UUID getLogServerGroupId() {
    return logServerGroupId;
  }

  public void setLogServerGroupId(UUID logServerGroupId) {
    this.logServerGroupId = logServerGroupId;
  }

  public boolean isLogServerGroupIdSet() {
    return this.logServerGroupId != null &&
        !this.logServerGroupId.equals(Constants.SERVERS_GROUP_UUID);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Hostname=").append(host);
    sb.append(", port=").append(port);
    sb.append(", dir=").append(workingDir);
    sb.append(", metaQuorum=").append(metaQuorum);
    sb.append(", metaGroupId=").append(metaGroupId);
    sb.append(", logServerGroupId=").append(logServerGroupId);
    return sb.toString();
  }
}