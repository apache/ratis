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

import org.apache.ratis.client.RaftClientStream;
import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.datastream.DataStreamApi;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.objects.DataStreamReply;
import org.apache.ratis.datastream.objects.DataStreamRequest;
import org.apache.ratis.protocol.ClientId;

import java.util.concurrent.CompletableFuture;

public class DataStreamClientImpl implements DataStreamClient {

  private ClientId clientId;
  private RaftProperties properties;
  private DataStreamApi streamApi;
  private RaftClientStream networkTransferApi;

  public DataStreamClientImpl(ClientId clientId,
                              RaftProperties properties,
                              RaftClientStream networkTransferApi){
    this.clientId = clientId;
    this.properties = properties;
    this.networkTransferApi = networkTransferApi;
  }

  @Override
  public ClientId getId() {
    return clientId;
  }

  @Override
  public DataStreamApi getDataStreamApi(){
    return streamApi;
  }

  @Override
  public RaftClientStream getNetworkTransferApi() {
    return networkTransferApi;
  }

  @Override
  public CompletableFuture<DataStreamReply> sendAsync(DataStreamRequest request){
    return new CompletableFuture<DataStreamReply>();
  }

}
