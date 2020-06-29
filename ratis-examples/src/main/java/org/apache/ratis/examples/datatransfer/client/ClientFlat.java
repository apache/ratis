package org.apache.ratis.examples.datatransfer.client;

import com.google.flatbuffers.FlatBufferBuilder;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.ratis.examples.datatransfer.flatbufs.FileTransferGrpc;
import org.apache.ratis.examples.datatransfer.flatbufs.TransferMsg;
import org.apache.ratis.examples.datatransfer.flatbufs.TransferReply;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;



public class ClientFlat {

    private FileTransferGrpc.FileTransferStub asyncStubFlat;

    final long[] recv = new long[1];
    int partId = 0;

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
            ByteBuffer bf = ByteBuffer.allocate(1024*1024);
            if(bf.hasArray()){
                Arrays.fill(bf.array(), (byte) 'a');
            }
            while(i < reps) {
                partId++;
                FlatBufferBuilder builder = new FlatBufferBuilder();
                int dataOff = TransferMsg.createDataVector(builder, bf);
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
        Thread.sleep(100*100);
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