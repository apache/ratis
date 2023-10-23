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
package org.apache.ratis.examples.membership.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Interactive command line console.
 */
public class Console {
  public static final String USAGE_MSG =
    "Usage: java org.apache.ratis.examples.membership.server.Console [options]\n"
      + "Options:\n"
      + "\tupdate [new_peer_ports]   Update membership to C_new. Separate ports with comma. "
      + "e.g. update 5100,5101\n"
      + "\tadd [peer_port]           Add peer with peer_port to raft cluster. e.g. add 5103\n"
      + "\tremove  [peer_port]       Remove peer with peer_port from raft cluster. e.g. remove"
      + " 5100\n"
      + "\tshow                      Show all peers of raft cluster.\n"
      + "\tincr                      Increment the counter value.\n"
      + "\tquery                     Query the value of counter.\n"
      + "\tquit                      Quit.";

  private final Scanner sc = new Scanner(System.in, "UTF-8");
  private final RaftCluster cluster = new RaftCluster();

  private void init() {
    System.out.println("Raft Server Membership Example.");
    System.out.println("Type ports seperated by comma for initial peers. e.g. 5100,5101,5102");

    String[] portArguments = commandLineInput()[0].split(",");
    List<Integer> ports = new ArrayList<>();
    Arrays.stream(portArguments).map(Integer::parseInt).forEach(ports::add);
    try {
      cluster.init(ports);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    show();
    System.out.println(USAGE_MSG);
  }

  private void execute() {
    while (true) {
      try {
        String[] args = commandLineInput();
        String command = args[0];

        if (command.equalsIgnoreCase("show")) {
          show();
        } else if (command.equalsIgnoreCase("add")) {
          add(args, 1);
        } else if (command.equalsIgnoreCase("remove")) {
          remove(args, 1);
        } else if (command.equalsIgnoreCase("update")) {
          update(args, 1);
        } else if (command.equalsIgnoreCase("incr")) {
          cluster.counterIncrement();
        } else if (command.equalsIgnoreCase("query")) {
          cluster.queryCounter();
        } else if (command.equalsIgnoreCase("quit")) {
          break;
        } else {
          System.out.println(USAGE_MSG);
        }
      } catch (Exception e) {
        System.out.println("Get error " + e.getMessage());
      }
    }
    try {
      System.out.println("Closing cluster...");
      cluster.close();
      System.out.println("Cluster closed successfully.");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void show() {
    cluster.show();
  }

  private void add(String[] args, int index) throws IOException {
    int port = Integer.parseInt(args[index]);
    List<Integer> ports = new ArrayList();
    ports.add(port);
    ports.addAll(cluster.ports());
    cluster.update(ports);
  }

  private void remove(String[] args, int index) throws IOException {
    int port = Integer.parseInt(args[index]);
    List<Integer> ports = new ArrayList<>();
    ports.addAll(cluster.ports());
    if (ports.remove(Integer.valueOf(port))) {
      cluster.update(ports);
    } else {
      System.out.println("Invalid port " + port);
    }
  }

  private void update(String[] args, int index) throws IOException {
    String[] portStrArray = args[index].split(",");
    List<Integer> ports = new ArrayList<>();
    for (String portStr : portStrArray) {
      ports.add(Integer.parseInt(portStr));
    }
    cluster.update(ports);
  }

  private String[] commandLineInput() {
    System.out.print(">>> ");
    return sc.nextLine().split(" ");
  }

  public static void main(String[] args) throws IOException {
    Console console = new Console();
    console.init();
    console.execute();
  }
}
