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

import org.apache.ratis.thirdparty.com.google.flatbuffers.FlatBufferBuilder;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.flatbufs.FileTransferGrpc;
import org.apache.ratis.flatbufs.TransferMsg;
import org.apache.ratis.flatbufs.TransferReply;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;

/**
 * Server code responding to messages of flatbuffer based Client.
 */

public class FileTransferFlatbufs extends FileTransferGrpc.FileTransferImplBase {
  @Override
  public StreamObserver<TransferMsg> sendData(final StreamObserver<TransferReply> responseObserver){
    return new StreamObserver<TransferMsg>(){
      private long rcvdDataSize = 0;

      public long getRcvdDataSize(){
        return rcvdDataSize;
      }

      @Override
      public void onNext(TransferMsg msg){
        rcvdDataSize += msg.dataLength();
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int off = TransferReply.createTransferReply(builder, msg.partId(), builder.createString("OK"));
        builder.finish(off);
        TransferReply rep = TransferReply.getRootAsTransferReply(builder.dataBuffer());
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
        System.out.println(rcvdDataSize);
        responseObserver.onCompleted();
      }
    };
  }
}
