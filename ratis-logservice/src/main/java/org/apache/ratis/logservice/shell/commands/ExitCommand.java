package org.apache.ratis.logservice.shell.commands;

import org.apache.ratis.logservice.client.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class ExitCommand implements Command {

  @Override public String getHelpMessage() {
    return "`exit` -- Exits this shell.";
  }

  @Override
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    throw new IllegalStateException("This command should be be invoked");
  }

}

