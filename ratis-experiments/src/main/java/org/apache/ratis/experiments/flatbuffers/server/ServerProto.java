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

package org.apache.ratis.experiments.flatbuffers.server;

import org.apache.ratis.thirdparty.io.grpc.BindableService;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerBuilder;

public final class ServerProto {
  private ServerProto(){

  }

  public static void main( String[] args ) throws Exception {
    BindableService service;
    service = (BindableService) new FileTransferProtobufs();
    Server svr = ServerBuilder.forPort(50051)
        .addService(service)
        .build();
    svr.start();
    System.out.println("Server started");
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Server shutting down");
        svr.shutdown();
      }
    });
    svr.awaitTermination();
  }
}
