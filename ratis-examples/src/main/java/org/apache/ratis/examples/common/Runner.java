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
package org.apache.ratis.examples.common;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.log4j.Level;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.examples.arithmetic.cli.Arithmetic;
import org.apache.ratis.examples.filestore.cli.FileStore;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.util.LogUtils;

import java.util.List;
import java.util.Optional;

/**
 * Standalone arithmetic raft server.
 */
public class Runner {

  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("No command type specified: ");
      return;
    }
    List<SubCommandBase> commands = initializeCommands(args[0]);
    Runner runner = new Runner();

    if (commands == null) {
      System.err.println("Wrong command type: " + args[0]);
      return;
    }
    String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 1, newArgs, 0, args.length - 1);

    JCommander.Builder builder = JCommander.newBuilder().addObject(runner);
    commands.forEach(command -> builder
        .addCommand(command.getClass().getSimpleName().toLowerCase(), command));
    JCommander jc = builder.build();
    try {
      jc.parse(newArgs);
      Optional<SubCommandBase> selectedCommand = commands.stream().filter(
          command -> command.getClass().getSimpleName().toLowerCase()
              .equals(jc.getParsedCommand())).findFirst();
      if (selectedCommand.isPresent()) {
        selectedCommand.get().run();
      } else {
        jc.usage();
      }
    } catch (ParameterException exception) {
      System.err.println("Wrong parameters: " + exception.getMessage());
      jc.usage();
    }

  }

  private static List<SubCommandBase> initializeCommands(String command) {
    if (command.equals(FileStore.class.getSimpleName().toLowerCase())) {
      return FileStore.getSubCommands();
    } else if (command.equals(Arithmetic.class.getSimpleName().toLowerCase())) {
      return Arithmetic.getSubCommands();
    }
    return null;
  }

}
