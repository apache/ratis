package org.apache.ratis.logservice.shell.commands;

import java.io.IOException;
import java.util.List;

import org.apache.ratis.logservice.api.LogInfo;
import org.apache.ratis.logservice.client.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class ListLogsCommand implements Command {

  @Override
  public String getHelpMessage() {
    return "`list` - List the available logs";
  }

  @Override
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    if (args.length != 0) {
      terminal.writer().println("ERROR - Usage: list");
      return;
    }

    try {
      List<LogInfo> logs = client.listLogs();
      StringBuilder sb = new StringBuilder();
      for (LogInfo log : logs) {
        if (sb.length() > 0) {
          sb.append("\n");
        }
        sb.append(log.getLogName().getName());
      }
      terminal.writer().println(sb.toString());
    } catch (IOException e) {
      terminal.writer().println("Failed to list available logs");
      e.printStackTrace(terminal.writer());
    }
  }

}
