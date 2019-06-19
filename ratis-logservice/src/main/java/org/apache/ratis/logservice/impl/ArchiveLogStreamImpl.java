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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.logservice.api.ArchiveLogReader;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.api.RecordListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveLogStreamImpl implements LogStream {
  public static final Logger LOG = LoggerFactory.getLogger(ArchiveLogStreamImpl.class);
  /*
   * Directory of the archived files
   */
  private final String location;

  /*
   * Log stream listeners
   */
  List<RecordListener> listeners;
  /*
   * Log stream name
   */
  LogName name;
  /*
   * Log stream configuration
   */
  LogServiceConfiguration config;
  /*
   * State
   */
  State state;

  public ArchiveLogStreamImpl(LogName name, String location) {
    this(name,location,null);

  }

  public ArchiveLogStreamImpl(LogName name, String location, LogServiceConfiguration config) {
    this.name = name;
    this.location = location;
    if(config!=null) {
      this.config = config;
    }
    init();
  }

  private void init() {
    state = State.ARCHIVED;
    listeners = Collections.synchronizedList(new ArrayList<RecordListener>());
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
    return new ArchiveHdfsLogReader(this.location);
  }

  @Override
  public LogWriter createWriter() {
    throw new UnsupportedOperationException("Arhived log cannot be written");
  }

  @Override
  public long getLastRecordId() throws IOException {
    throw new UnsupportedOperationException("getLastRecordId()");
  }

  @Override
  public long getStartRecordId() throws IOException {
    throw new UnsupportedOperationException("getStartRecordId()");
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
  public RaftClient getRaftClient() {
    throw new UnsupportedOperationException("getRaftClient()");
  }

}
