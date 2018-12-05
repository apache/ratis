package org.apache.ratis.logservice.shell.commands;

import org.apache.ratis.logservice.client.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.apache.ratis.logservice.shell.CommandFactory;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class HelpCommand implements Command {

  @Override public String getHelpMessage() {
    return "`help` - Prints this help message";
  }

  @Override
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    CommandFactory.getCommands()
        .values()
        .stream()
        .forEach((c) -> terminal.writer().println(c.getHelpMessage()));
  }

}
