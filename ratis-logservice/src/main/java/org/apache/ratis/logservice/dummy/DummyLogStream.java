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
package org.apache.ratis.logservice.dummy;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStreamConfiguration;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.api.RecordListener;

public class DummyLogStream implements LogStream {
  private final LogName name;
  private final DummyLogService service;

  public DummyLogStream(DummyLogService service, LogName name) {
    this.service = Objects.requireNonNull(service);
    this.name = Objects.requireNonNull(name);
  }

  @Override
  public LogName getName() {
    return name;
  }

  @Override
  public long getSize() {
    return 0;
  }

  @Override
  public LogReader createReader() {
    return new DummyLogReader();
  }

  @Override
  public LogWriter createWriter() {
    return new DummyLogWriter();
  }

  @Override
  public Set<RecordListener> getRecordListeners() {
    Set<RecordListener> listeners = service.recordListeners.get(name);
    if (listeners == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(listeners);
  }

  @Override
  public State getState() {
    return State.OPEN;
  }

  @Override
  public long getLastRecordId() {
    return 0;
  }

  @Override
  public LogStreamConfiguration getConfiguration() {
    return null;
  }
}
