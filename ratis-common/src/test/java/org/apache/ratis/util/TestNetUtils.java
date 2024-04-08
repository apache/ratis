/*
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
package org.apache.ratis.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class TestNetUtils {

  @Test
  void createsUniqueAddresses() {
    for (int i = 0; i < 10; i++) {
      List<InetSocketAddress> addresses = NetUtils.createLocalServerAddress(100);
      Assertions.assertEquals(addresses.stream().distinct().collect(Collectors.toList()), addresses);
    }
  }

  @Test
  void returnsUniquePorts() {
    List<Integer> addresses = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      addresses.add(NetUtils.getFreePort());
    }
    Assertions.assertEquals(addresses.stream().distinct().collect(Collectors.toList()), addresses);
  }

  @Test
  void skipsUsedPort() throws IOException {
    int port = NetUtils.getFreePort();
    try (ServerSocket ignored = new ServerSocket(port + 1)) {
      int nextPort = NetUtils.getFreePort();
      Assertions.assertEquals(port + 2, nextPort);
    }
  }
}
