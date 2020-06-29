package org.apache.ratis.examples.datatransfer.server;

import org.apache.ratis.thirdparty.io.grpc.BindableService;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerBuilder;

public class ServerProto {
    public static void main( String[] args ) throws Exception
    {
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
