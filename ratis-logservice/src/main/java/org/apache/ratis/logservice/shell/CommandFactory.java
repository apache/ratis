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
package org.apache.ratis.logservice.shell;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.ratis.logservice.shell.commands.CreateLogCommand;
import org.apache.ratis.logservice.shell.commands.DeleteLogCommand;
import org.apache.ratis.logservice.shell.commands.ExitCommand;
import org.apache.ratis.logservice.shell.commands.HelpCommand;
import org.apache.ratis.logservice.shell.commands.ListLogsCommand;
import org.apache.ratis.logservice.shell.commands.PutToLogCommand;
import org.apache.ratis.logservice.shell.commands.ReadLogCommand;

public class CommandFactory {
  private static final Map<String,Command> KNOWN_COMMANDS = cacheCommands();

  private static Map<String,Command> cacheCommands() {
    Map<String,Command> commands = new HashMap<>();
    ExitCommand exitCommand = new ExitCommand();

    // Ensure all keys are lowercase -- see the same logic in #create(String).
    commands.put("create", new CreateLogCommand());
    commands.put("put", new PutToLogCommand());
    commands.put("read", new ReadLogCommand());
    commands.put("delete", new DeleteLogCommand());
    commands.put("exit", exitCommand);
    commands.put("quit", exitCommand);
    commands.put("help", new HelpCommand());
    commands.put("list", new ListLogsCommand());

    return Collections.unmodifiableMap(commands);
  }

  public static Map<String,Command> getCommands() {
    return KNOWN_COMMANDS;
  }

  private CommandFactory() {}

  /**
   * Returns an instance of the command to run given the name, else null.
   * Be aware that Command instances are singletons and do not retain state.
   */
  public static Command create(String commandName) {
    // Normalize the command name to lowercase
    return KNOWN_COMMANDS.get(Objects.requireNonNull(commandName).toLowerCase());
  }
}
