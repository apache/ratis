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
package org.apache.ratis.datastream.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.io.WriteOption;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.apache.ratis.util.Preconditions;

import java.nio.ByteBuffer;

/**
 * Implements {@link DataStreamRequest} with {@link ByteBuffer}.
 *
 * This class is immutable.
 */
public class DataStreamRequestByteBuffer extends DataStreamPacketByteBuffer implements DataStreamRequest {
  private WriteOption[] options;

  public DataStreamRequestByteBuffer(DataStreamRequestHeader header, ByteBuffer buffer) {
    super(header.getClientId(), header.getType(), header.getStreamId(), header.getStreamOffset(), buffer);
    this.options = header.getWriteOptions();
    Preconditions.assertTrue(header.getDataLength() == buffer.remaining());
  }

  @Override
  @SuppressFBWarnings("EI_EXPOSE_REP")
  public WriteOption[] getWriteOptions() {
    return options;
  }
}
