package org.apache.ratis.logservice.shell.commands;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.client.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class CreateLogCommand implements Command {

  @Override public String getHelpMessage() {
    return "`create` - Creates a new log with the given name";
  }

  @Override
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    if (args.length != 1) {
      terminal.writer().println("ERROR - Usage: create <name>");
      return;
    }
    String logName = args[0];
    try (LogStream stream = client.createLog(LogName.of(logName))) {
      terminal.writer().println("Created log '" + logName + "'");
    } catch (Exception e) {
      terminal.writer().println("Error creating log '" + logName + "'");
      e.printStackTrace(terminal.writer());
    }
  }
}