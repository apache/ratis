package org.apache.ratis.examples.datatransfer.server;

import io.grpc.BindableService;
import io.grpc.ServerBuilder;

public class ServerFlat {
    public static void main( String[] args ) throws Exception
    {
        BindableService service;
            service = (BindableService) new FileTransferFlatbufs();
        io.grpc.Server svr = ServerBuilder.forPort(50051)
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
