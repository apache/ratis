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
package org.apache.ratis.shell.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.ratis.thirdparty.com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Abstract class for handling command line inputs.
 */
public abstract class AbstractShell implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractShell.class);

  private final Map<String, Command> mCommands;
  private final Closer closer;

  /**
   * Creates a new instance of {@link AbstractShell}.
   */
  public AbstractShell() {
    closer = Closer.create();
    mCommands = loadCommands();
    // Register all loaded commands under closer.
    mCommands.values().forEach(closer::register);
  }

  /**
   * Handles the specified shell command request, displaying usage if the command format is invalid.
   *
   * @param argv [] Array of arguments given by the user's input from the terminal
   * @return 0 if command is successful, -1 if an error occurred
   */
  public int run(String... argv) {
    if (argv.length == 0) {
      printUsage();
      return -1;
    }

    // Sanity check on the number of arguments
    String cmd = argv[0];
    Command command = mCommands.get(cmd);

    if (command == null) {
      // Unknown command (we didn't find the cmd in our dict)
      System.err.printf("%s is an unknown command.%n", cmd);
      printUsage();
      return -1;
    }

    // Find the inner-most command and its argument line.
    CommandLine cmdline;
    try {
      String[] currArgs = Arrays.copyOf(argv, argv.length);
      while (command.hasSubCommand()) {
        if (currArgs.length < 2) {
          throw new IllegalArgumentException("No sub-command is specified");
        }
        if (!command.getSubCommands().containsKey(currArgs[1])) {
          throw new IllegalArgumentException("Unknown sub-command: " + currArgs[1]);
        }
        command = command.getSubCommands().get(currArgs[1]);
        currArgs = Arrays.copyOfRange(currArgs, 1, currArgs.length);
      }
      currArgs = Arrays.copyOfRange(currArgs, 1, currArgs.length);

      cmdline = command.parseAndValidateArgs(currArgs);
    } catch (IllegalArgumentException e) {
      // It outputs a prompt message when passing wrong args to CLI
      System.out.println(e.getMessage());
      System.out.println("Usage: " + command.getUsage());
      System.out.println(command.getDescription());
      LOG.error("Invalid arguments for command {}:", command.getCommandName(), e);
      return -1;
    }

    // Handle the command
    try {
      return command.run(cmdline);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      LOG.error("Error running" + Arrays.stream(argv).reduce("", (a, b) -> a + " " + b), e);
      return -1;
    }
  }

  /**
   * @return all commands provided by this shell
   */
  public Collection<Command> getCommands() {
    return mCommands.values();
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }

  /**
   * @return name of the shell
   */
  protected abstract String getShellName();

  /**
   * Map structure: Command name => {@link Command} instance.
   *
   * @return a set of commands which can be executed under this shell
   */
  protected abstract Map<String, Command> loadCommands();

  protected Closer getCloser() {
    return closer;
  }

  /**
   * Prints usage for all commands.
   */
  protected void printUsage() {
    System.out.println("Usage: ratis " + getShellName() + " [generic options]");
    SortedSet<String> sortedCmds = new TreeSet<>(mCommands.keySet());
    for (String cmd : sortedCmds) {
      System.out.format("%-60s%n", "\t [" + mCommands.get(cmd).getUsage() + "]");
    }
  }
}
