package org.apache.ratis.examples.datatransfer.server;

import com.google.flatbuffers.FlatBufferBuilder;
import io.grpc.Status;
import org.apache.ratis.examples.datatransfer.flatbufs.FileTransferGrpc;
import org.apache.ratis.examples.datatransfer.flatbufs.TransferMsg;
import org.apache.ratis.examples.datatransfer.flatbufs.TransferReply;
import io.grpc.stub.StreamObserver;

public class FileTransferFlatbufs extends FileTransferGrpc.FileTransferImplBase {
    @Override
    public StreamObserver<TransferMsg> sendData(final StreamObserver<TransferReply> responseObserver){
        return new StreamObserver<TransferMsg>(){
            long rcvdDataSize = 0;
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
                responseObserver.onCompleted();
            }
        };
    }
}
