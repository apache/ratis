package org.apache.ratis.examples.datatransfer.server;

import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.ExamplesProtos.TransferReplyProto;
import org.apache.ratis.proto.ExamplesProtos.TransferMsgProto;
import org.apache.ratis.proto.FileTransferExampleServiceGrpc;

public class FileTransferProtobufs extends FileTransferExampleServiceGrpc.FileTransferExampleServiceImplBase {
    @Override
    public StreamObserver<TransferMsgProto> sendData(final StreamObserver<TransferReplyProto> responseObserver) {
        return new StreamObserver<TransferMsgProto>() {
            long x = 0;
            @Override
            public void onNext(TransferMsgProto msg){
                x += msg.getData().size();
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
