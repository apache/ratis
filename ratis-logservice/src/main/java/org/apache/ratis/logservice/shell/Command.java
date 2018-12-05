package org.apache.ratis.logservice.shell;

import org.apache.ratis.logservice.client.LogServiceClient;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

/**
 * Represents a single command in the shell. Each command must be stateless.
 */
public interface Command {

  String getHelpMessage();

  void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args);
}