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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;

public class DataStreamClientImpl implements DataStreamClient {

  private final ClientId clientId;
  private final RaftProperties properties;
  private final DataStreamClientRpc dataStreamClientRpc;
  private final DataStreamApi dataStreamApi = null; //TODO need an impl

  public DataStreamClientImpl(ClientId clientId,
                              RaftProperties properties,
                              DataStreamClientRpc dataStreamClientRpc){
    this.clientId = clientId;
    this.properties = properties;
    this.dataStreamClientRpc = dataStreamClientRpc;
  }

  @Override
  public ClientId getId() {
    return clientId;
  }

  @Override
  public DataStreamApi getDataStreamApi(){
    return dataStreamApi;
  }

}
