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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.ratis.logservice.client.LogServiceClient;
import org.apache.ratis.logservice.shell.commands.ExitCommand;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultHighlighter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

/**
 * An interactive shell that can interact with a LogService instance.
 */
public class LogServiceShell {
  private static Logger LOG = LoggerFactory.getLogger(LogServiceShell.class);

  private static final String PROMPT = "logservice> ";

  private final Terminal terminal;
  private final LineReader lineReader;
  private final LogServiceClient client;

  public LogServiceShell(Terminal terminal, LineReader reader, LogServiceClient client) {
    this.terminal = Objects.requireNonNull(terminal);
    this.lineReader = Objects.requireNonNull(reader);
    this.client = Objects.requireNonNull(client);
  }

  public void run() {
    while (true) {
      // Read and sanitize the input
      String line;
      try {
        line = lineReader.readLine(PROMPT);
      } catch (UserInterruptException e) {
        continue;
      } catch (EndOfFileException e) {
        break;
      }
      if (line == null) {
        continue;
      }
      line = line.trim();

      // Construct the Command and args to pass to that command
      Entry<Command,String[]> pair = parseCommand(line);
      if (pair == null) {
        continue;
      }
      Command command = pair.getKey();
      String[] commandArgs = pair.getValue();

      // Our "exit" or "quit"
      if (command instanceof ExitCommand) {
        break;
      }

      // Run the command
      command.run(terminal, lineReader, client, commandArgs);

      // Flush a newline to the screen.
      terminal.writer().flush();
    }
    terminal.writer().println("Bye!");
    terminal.writer().flush();
  }

  Entry<Command,String[]> parseCommand(String line) {
    String[] words = line.split("\\s+");
    if (words.length == 0) {
      return null;
    }

    String commandWord = words[0];
    Command command = CommandFactory.create(commandWord);
    // If we have no command to run, return null.
    if (command == null) {
      return null;
    }

    // Pull off the "command" name
    String[] args = Arrays.copyOfRange(words, 1, words.length);

    return new AbstractMap.SimpleEntry<>(command, args);
  }

  public static void main(String[] args) throws Exception {
    final Terminal terminal = TerminalBuilder.builder()
        .system(true)
        .build();

    History defaultHistory = new DefaultHistory();
    // Register a shutdown-hook per JLine documentation to save history
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        defaultHistory.save();
      } catch (IOException e) {
        LOG.debug("Failed to save terminal history", e);
      }
    }));

    final LineReader lineReader = LineReaderBuilder.builder()
        .terminal(terminal)
        .highlighter(new DefaultHighlighter())
        .history(defaultHistory)
        .build();

    LogServiceShellOpts opts = new LogServiceShellOpts();
    JCommander.newBuilder()
        .addObject(opts)
        .build()
        .parse(args);

    try (LogServiceClient logServiceClient = new LogServiceClient(opts.metaQuorum)) {
      LogServiceShell client = new LogServiceShell(terminal, lineReader, logServiceClient);
      client.run();
    }
  }
}
