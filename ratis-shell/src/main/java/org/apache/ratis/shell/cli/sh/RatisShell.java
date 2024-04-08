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
package org.apache.ratis.shell.cli.sh;

import org.apache.ratis.shell.cli.AbstractShell;
import org.apache.ratis.shell.cli.Command;
import org.apache.ratis.shell.cli.sh.command.AbstractParentCommand;
import org.apache.ratis.shell.cli.sh.command.Context;
import org.apache.ratis.shell.cli.sh.command.ElectionCommand;
import org.apache.ratis.shell.cli.sh.command.GroupCommand;
import org.apache.ratis.shell.cli.sh.command.LocalCommand;
import org.apache.ratis.shell.cli.sh.command.PeerCommand;
import org.apache.ratis.shell.cli.sh.command.SnapshotCommand;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Shell for manage ratis group.
 */
public class RatisShell extends AbstractShell {
  static final List<Function<Context, AbstractParentCommand>> PARENT_COMMAND_CONSTRUCTORS
      = Collections.unmodifiableList(Arrays.asList(
          PeerCommand::new, GroupCommand::new, ElectionCommand::new, SnapshotCommand::new, LocalCommand::new));

  static List<AbstractParentCommand> allParentCommands(Context context) {
    return PARENT_COMMAND_CONSTRUCTORS.stream()
        .map(constructor -> constructor.apply(context))
        .collect(Collectors.toList());
  }

  /**
   * Manage ratis shell command.
   *
   * @param args array of arguments given by the user's input from the terminal
   */
  public static void main(String[] args) {
    final RatisShell shell = new RatisShell(System.out);
    System.exit(shell.run(args));
  }

  public RatisShell(PrintStream out) {
    super(new Context(out));
  }

  @Override
  protected String getShellName() {
    return "sh";
  }

  @Override
  protected Map<String, Command> loadCommands(Context context) {
    return allParentCommands(context).stream()
        .collect(Collectors.toMap(Command::getCommandName, Function.identity()));
  }
}
