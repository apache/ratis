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

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.proto.LogServiceProtos;
import org.apache.ratis.logservice.util.LogServiceProtoUtil;

public class ArchivalInfo {

  public enum ArchivalStatus{
    SUBMITTED,
    STARTED,
    RUNNING,
    INTERUPPTED,
    COMPLETED, FAILED
  }
  private String archiveLocation;
  private LogName archiveLogName;
  private long lastArchivedIndex;
  private ArchivalStatus status;

  public ArchivalInfo(String location) {
    this.archiveLocation = location;
  }


  public ArchivalInfo updateArchivalInfo(LogServiceProtos.ArchiveLogRequestProto archiveLog) {
    this.archiveLogName = LogServiceProtoUtil.toLogName(archiveLog.getLogName());
    this.lastArchivedIndex = archiveLog.getLastArchivedRaftIndex();
    this.status = ArchivalStatus.valueOf(archiveLog.getStatus().name());
    return this;
  }

  public String getArchiveLocation() {
    return archiveLocation;
  }

  public LogName getArchiveLogName() {
    return archiveLogName;
  }

  public long getLastArchivedIndex() {
    return lastArchivedIndex;
  }

  public ArchivalStatus getStatus(){
    return status;
  }

}
