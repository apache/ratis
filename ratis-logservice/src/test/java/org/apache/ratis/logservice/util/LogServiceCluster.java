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


package org.apache.ratis.logservice.util;

import org.apache.ratis.BaseTest;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogServiceClient;
import org.apache.ratis.logservice.server.LogServer;
import org.apache.ratis.logservice.server.MetadataServer;
import org.apache.ratis.util.NetUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * MiniCluster for the LogService. Allows to create and manage master nodes as well as to create and manage worker nodes
 */

public class LogServiceCluster implements AutoCloseable {
    private List<MetadataServer> masters;
    private List<LogServer> workers = new ArrayList<>();
    private String baseTestDir = BaseTest.getRootTestDir().getAbsolutePath();

    /**
     * Create a number of worker nodes with random ports and start them
     * @param numWorkers number of Workers to create
     */
    public void createWorkers(int numWorkers) {
        String meta = getMetaIdentity();
        List<LogServer> newWorkers = IntStream.range(0, numWorkers).parallel().mapToObj(i ->
                LogServer.newBuilder()
                        .setHostName("localhost")
                        .setMetaQuorum(meta)
                        .setWorkingDir(baseTestDir + "/workers/" + i)
                        .build()).collect(Collectors.toList());
        newWorkers.parallelStream().forEach( worker -> {
            try {
                worker.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        workers.addAll(newWorkers);
    }

    /**
     *
     * @return the string that represent the meta quorum ID that can can be used to manually create a worker nodes
     */
    public String getMetaIdentity() {
        // Nb. Can only be called after the masters have been instantiated.
        return masters.stream().map(object -> object.getAddress()).collect(Collectors.joining(","));
    }

    /**
     * Create and start a LogService metadata quorum with N number of masters.
     * They are created with ports starting from 9000
     * @param numServers
     */

    public LogServiceCluster(int numServers) {
        // Have to construct the meta quorum by hand -- `getMetaIdentity()` requires
        // uses the masters to build the quorum (chicken and egg problem).
        List<InetSocketAddress> addresses = NetUtils.createLocalServerAddress(numServers);
        String metaQuorum = addresses.stream().map(address -> address.getHostString() + ':' + address.getPort())
            .collect(Collectors.joining(","));
        this.masters = addresses.stream().map(address ->
                MetadataServer.newBuilder()
                        .setHostName(address.getHostName())
                        .setPort(address.getPort())
                        .setWorkingDir(baseTestDir + "/masters/" + address.getPort())
                        .setMetaQuorum(metaQuorum)
                        .build())
                .collect(Collectors.toList());
        masters.parallelStream().forEach(master -> {
            try {
                master.cleanUp();
                master.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }


    /**
     * Create a new LOG with the given name
     * @param logName
     * @throws IOException
     */
    public LogStream createLog(LogName logName) throws Exception {
        try (LogServiceClient client = new LogServiceClient(getMetaIdentity())) {
            return client.createLog(logName);
        }
    }

    /**
     * @return the current set of the workers
     */
    public List<LogServer> getWorkers() {
        return workers;
    }

    /**
     *
     * @return the current set of the masters
     */
    public List<MetadataServer> getMasters() {
        return masters;
    }



    /**
     * Shutdown the cluster.
     */
    @Override
    public void close() {
        masters.stream().parallel().forEach(master -> {
            try {
                master.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        workers.stream().parallel().forEach ( worker -> {
            try {
                worker.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public LogStream getLog(LogName logName) throws Exception {
        try (LogServiceClient client = new LogServiceClient(getMetaIdentity())) {
            return client.getLog(logName);
        }
    }

    /**
     * Remove all temporary directories created by the mini cluster
     */
    public void cleanUp() {
//        FileUtils.deleteDirectory();
    }
}
