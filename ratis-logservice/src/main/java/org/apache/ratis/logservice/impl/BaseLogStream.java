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

import java.util.Set;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStreamConfiguration;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.api.RecordListener;

public class BaseLogStream implements LogStream {
  private LogName logName;
  private State state;
  private long size;

  public BaseLogStream(LogName logName, State state, long size) {
    this.logName = logName;
    this.state = state;
    this.size = size;
  }

  @Override
  public LogName getName() {
    return logName;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public LogReader createReader() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public LogWriter createWriter() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getLastRecordId() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Set<RecordListener> getRecordListeners() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public LogStreamConfiguration getConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

}
