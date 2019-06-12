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

import java.io.IOException;

import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

public interface RaftLogReader {

  /**
   * Positions this reader just before the current recordId. Use {@link #next()} to get that
   * element, but take care to check if a value is present using {@link #hasNext()} first.
   */
  public void seek(long recordId) throws IOException;

  /**
   * Returns true if there is a log entry to read.
   */
  public boolean hasNext() throws IOException;

  /**
   * Returns the next log entry. Ensure {@link #hasNext()} returns true before
   * calling this method.
   */
  public byte[] next() throws IOException;

  /**
   * Returns current raft index read
   * @return
   */
  public long getCurrentRaftIndex();

  }
