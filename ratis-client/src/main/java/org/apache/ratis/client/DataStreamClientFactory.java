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

package org.apache.ratis.client;

import org.apache.ratis.client.impl.RaftClientImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.DataStreamFactory;
import org.apache.ratis.protocol.ClientId;

/**
 * A factory to create streaming client.
 */
public interface DataStreamClientFactory extends DataStreamFactory {

  static DataStreamClientFactory cast(DataStreamFactory dataStreamFactory) {
    if (dataStreamFactory instanceof DataStreamClientFactory) {
      return (DataStreamClientFactory) dataStreamFactory;
    }
    throw new ClassCastException("Cannot cast " + dataStreamFactory.getClass()
        + " to " + ClientFactory.class
        + "; stream type is " + dataStreamFactory.getDataStreamType());
  }

  DataStreamClientRpc newDataStreamClientRpc(RaftClientImpl client, RaftProperties properties);
}
