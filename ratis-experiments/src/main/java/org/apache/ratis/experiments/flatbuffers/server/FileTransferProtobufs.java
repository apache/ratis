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

import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.ExperimentsProtos.TransferReplyProto;
import org.apache.ratis.proto.ExperimentsProtos.TransferMsgProto;
import org.apache.ratis.proto.FileTransferExampleServiceGrpc;

/**
 * Server code responding to messages of protobuffers based Client.
 */

public class FileTransferProtobufs extends FileTransferExampleServiceGrpc.FileTransferExampleServiceImplBase {
  @Override
  public StreamObserver<TransferMsgProto> sendData(final StreamObserver<TransferReplyProto> responseObserver) {
    return new StreamObserver<TransferMsgProto>() {
      private long rcvdDataSize = 0;

      public long getRcvdDataSize(){
        return rcvdDataSize;
      }

      @Override
      public void onNext(TransferMsgProto msg){
        rcvdDataSize += msg.getData().size();
        TransferReplyProto rep = TransferReplyProto.newBuilder().setPartId(msg.getPartId()).setMessage("OK").build();
        responseObserver.onNext(rep);
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        System.out.println(status);
        System.out.println("Finished streaming with errors");
      }

      @Override
      public void onCompleted(){
        responseObserver.onCompleted();
      }
    };
  }
}
