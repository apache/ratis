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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ratis.logservice.api.LogWriter;

public class DummyLogWriter implements LogWriter {
  private final AtomicLong counter;

  public DummyLogWriter() {
    this.counter = new AtomicLong(-1);
  }

  @Override public void close() {}

  @Override
  public long write(ByteBuffer data) throws IOException {
    return counter.incrementAndGet();
  }

  @Override
  public long write(List<ByteBuffer> records) throws IOException {
    return counter.addAndGet(records.size());
  }

  @Override
  public long sync() throws IOException {
    return counter.get();
  }

}
