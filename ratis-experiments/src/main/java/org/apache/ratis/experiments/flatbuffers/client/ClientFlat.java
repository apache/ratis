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

package org.apache.ratis.experiments.flatbuffers.client;

import org.apache.ratis.thirdparty.com.google.flatbuffers.FlatBufferBuilder;
import org.apache.ratis.thirdparty.io.grpc.Channel;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.flatbufs.FileTransferGrpc;
import org.apache.ratis.flatbufs.TransferMsg;
import org.apache.ratis.flatbufs.TransferReply;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Sets up a client to send flatbuffers messages over GRPC to Server.
 * Uses a semaphore to manage current outbound data
 * Uses a flatbuffer builder interface to create message.
 */

public class ClientFlat {

  private FileTransferGrpc.FileTransferStub asyncStubFlat;
  // semaphore to manage current outbound data
  private final Semaphore available = new Semaphore(3000, true);

  private final long[] recv = new long[1];
  private int partId = 0;

  public long[] getRecv() {
    return recv;
  }

  public int getPartId(){
    return partId;
  }

  public ClientFlat(Channel channel){
    asyncStubFlat = FileTransferGrpc.newStub(channel);
    recv[0] = 0;
  }

  public void execFlatClient(int reps) throws Exception{
    System.out.println("Starting streaming with Flatbuffers");
    StreamObserver<TransferMsg> requestObserver = asyncStubFlat.sendData(new StreamObserver<TransferReply>(){
      @Override
      public void onNext(TransferReply msg) {
        recv[0]++;
        available.release();
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        System.out.println(status);
        System.out.println("Finished streaming with errors");
      }

      @Override
      public void onCompleted() {
        System.out.println("Finished streaming");
      }
    });
    try{
      int i = 0;
      // allocate a byte buffer containing message data.
      ByteBuffer bf = ByteBuffer.allocateDirect(1024*1024);
      if(bf.hasArray()){
        Arrays.fill(bf.array(), (byte) 'a');
      }
      while(i < reps) {
        partId++;
        available.acquire();
        // start a builder and reset position of databuffer to 0.
        FlatBufferBuilder builder = new FlatBufferBuilder();
        bf.position(0).limit(bf.capacity());
        // create string of the message data
        // the datacopy happens here, significant CPU time spent.
        int dataOff = builder.createString(bf);
        // Use flatbuffers generated message builder.
        int off = TransferMsg.createTransferMsg(builder, partId, dataOff);
        builder.finish(off);
        TransferMsg msg = TransferMsg.getRootAsTransferMsg(builder.dataBuffer());
        requestObserver.onNext(msg);
        i++;
      }

    } catch (Exception e){
      System.out.println(e);
    }
    requestObserver.onCompleted();
    Thread.sleep(1000*100);
    if(recv[0] == partId){
      System.out.println("Transfer Successfull....");
    } else{
      System.out.println("Some error occurred...");
    }
  }

  public static void main(String[] args) throws Exception{
    int times = 100000;
    if(args.length != 0){
      times = Integer.parseInt(args[0]);
    }
    String target = "localhost:50051";
    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    ClientFlat c = new ClientFlat(channel);
    c.execFlatClient(times);
    channel.shutdown();
    channel.awaitTermination(1, TimeUnit.SECONDS);
  }
}