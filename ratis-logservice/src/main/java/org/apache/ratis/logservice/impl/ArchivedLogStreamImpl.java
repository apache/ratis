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
package org.apache.ratis.logservice.impl;

import java.io.IOException;
import java.util.Collection;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.logservice.api.ArchiveLogReader;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.api.RecordListener;
import org.apache.ratis.logservice.common.Constants;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchivedLogStreamImpl implements LogStream {
  public static final Logger LOG = LoggerFactory.getLogger(ArchivedLogStreamImpl.class);
  /*
   * Directory of the archived files
   */
  private String location;

  /*
   * Log stream name
   */
  private LogName name;
  /*
   * Log stream configuration
   */
  private LogServiceConfiguration config;

  /*
   * State
   */
  private State state;

  protected void setState(State state) {
    this.state = state;
  }

  public ArchivedLogStreamImpl(LogName name, LogServiceConfiguration config) {
    this(name, config.get(Constants.LOG_SERVICE_ARCHIVAL_LOCATION_KEY));
    this.config = config;
    init();
  }

  protected ArchivedLogStreamImpl(LogName name, String location) {
    this.name = name;
    this.location = location;
  }

  protected void init() {
    this.state = State.ARCHIVED;
  }

  @Override
  public LogName getName() {
    return name;
  }

  @Override public State getState() throws IOException {
    return state;
  }

  @Override
  public long getSize() throws IOException{
    throw new UnsupportedOperationException("getSize()");
  }

  @Override
  public long getLength() throws IOException {
    throw new UnsupportedOperationException("getLength()");
  }

  @Override
  public ArchiveLogReader createReader() throws IOException {
    return new ArchiveHdfsLogReader(LogServiceUtils.getArchiveLocationForLog(location, name));
  }

  @Override
  public LogWriter createWriter() {
    throw new UnsupportedOperationException("Archived log cannot be written");
  }

  @Override
  public long getLastRecordId() throws IOException {
    throw new UnsupportedOperationException("getLastRecordId()");
  }

  @Override
  public long getStartRecordId() throws IOException {
    throw new UnsupportedOperationException("getStartRecordId()");
  }

  @Override public Collection<RecordListener> getRecordListeners() {
    throw new UnsupportedOperationException("get record listeners");
  }

  @Override
  public LogServiceConfiguration getConfiguration() {
    return config;
  }

  @Override
  public void close() throws Exception {
  }

  @Override public void addRecordListener(RecordListener listener) {
    throw new UnsupportedOperationException("Add record listener");
  }

  @Override public boolean removeRecordListener(RecordListener listener) {
    throw new UnsupportedOperationException("remove record listener");
  }

  @Override
  public RaftClient getRaftClient() {
    throw new UnsupportedOperationException("getRaftClient()");
  }

}
