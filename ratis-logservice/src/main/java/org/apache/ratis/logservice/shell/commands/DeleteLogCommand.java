package org.apache.ratis.logservice.shell.commands;

import java.io.IOException;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.client.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class DeleteLogCommand implements Command {

  @Override public String getHelpMessage() {
    return "`delete` - deletes the log with the given name.";
  }

  @Override
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    if (args.length != 1) {
      terminal.writer().println("ERROR - Usage: delete <name>");
      return;
    }
    String logName = args[0];
    try {
      client.deleteLog(LogName.of(logName));
      terminal.writer().println("Deleted log '" + logName + "'");
    } catch (IOException e) {
      terminal.writer().println("Error deleting log '" + logName + "'");
      e.printStackTrace(terminal.writer());
    }
  }
}
