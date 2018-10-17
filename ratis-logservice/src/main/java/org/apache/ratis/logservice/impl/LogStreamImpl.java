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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogService;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.api.RecordListener;
import org.apache.ratis.proto.logservice.LogServiceProtos.LogStreamProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogStreamImpl implements LogStream {
  public static final Logger LOG = LoggerFactory.getLogger(LogStreamImpl.class);

  /*
   * Log stream listeners
   */
  List<RecordListener> listeners;
  /*
   * Log stream name
   */
  LogName name;
  /*
   * Parent log service instance
   */
  LogService service;
  /*
   * Log stream configuration
   */
  LogServiceConfiguration config;
  /*
   * State
   */
  LogStream.State state;

  /*
   * Length
   */
  long length;

  public LogStreamImpl(LogStreamProto proto, LogService service) {
    this.service = service;
    this.name = LogName.of(proto.getLogName().getName());
    this.config = service.getConfiguration();
    init();
  }

  public LogStreamImpl(LogName name, LogService logService) {
    this.service = logService;
    this.name = name;
    this.config = this.service.getConfiguration();
    init();
  }

  public LogStreamImpl(LogName name, LogService logService, LogServiceConfiguration config) {
    this.service = logService;
    this.name = name;
    this.config = config;
    init();
  }

  private void init() {
    // TODO create new state machine. etc
    state = State.OPEN;
    listeners = Collections.synchronizedList(new ArrayList<RecordListener>());
  }

  @Override
  public LogName getName() {
    return name;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public long getSize() {
    // TODO use raft client to query state machine
    return 0;
  }

  @Override
  public LogReader createReader() {
    return new LogReaderImpl(this);
  }

  @Override
  public LogWriter createWriter() {
    return new LogWriterImpl(this);
  }

  @Override
  public long getLastRecordId() {
    // TODO use raft client to query state machine
    return 0;
  }

  @Override
  public Collection<RecordListener> getRecordListeners() {
    return listeners;
  }

  @Override
  public LogServiceConfiguration getConfiguration() {
    return config;
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub
    state = State.CLOSED;
  }

  @Override
  public void addRecordListener(RecordListener listener) {
    synchronized (listeners) {
      if (!listeners.contains(listener)) {
        listeners.add(listener);
      }
    }
  }

  @Override
  public boolean removeRecordListener(RecordListener listener) {
    return listeners.remove(listener);
  }

  @Override
  public LogService getLogService() {
    return service;
  }

}
