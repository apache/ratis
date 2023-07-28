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

package org.apache.ratis.protocol;

import org.apache.ratis.io.WriteOption;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.thirdparty.com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
/**
 * The header format is the same {@link DataStreamPacketHeader}
 * since there are no additional fields.
 */
public class DataStreamRequestHeader extends DataStreamPacketHeader implements DataStreamRequest {

  private final List<WriteOption> options;

  public DataStreamRequestHeader(ClientId clientId, Type type, long streamId, long streamOffset, long dataLength,
      WriteOption... options) {
    this(clientId, type, streamId, streamOffset, dataLength, Arrays.asList(options));
  }

  public DataStreamRequestHeader(ClientId clientId, Type type, long streamId, long streamOffset, long dataLength,
                                 Iterable<WriteOption> options) {
    super(clientId, type, streamId, streamOffset, dataLength);
    this.options = Collections.unmodifiableList(Lists.newArrayList(options));
  }

  @Override
  public List<WriteOption> getWriteOptionList() {
    return options;
  }
}