package org.apache.ratis.logservice.shell.commands;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.client.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class PutToLogCommand implements Command {

  @Override public String getHelpMessage() {
    return "`put` - Appends a value to a log";
  }

  @Override
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    if (args.length != 2) {
      terminal.writer().println("ERROR - Usage: put <name> <value>");
      return;
    }
    String name = args[0];
    String value = args[1];
    try (LogStream stream = client.getLog(LogName.of(name));
        LogWriter writer = stream.createWriter()) {
      writer.write(ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      terminal.writer().println("Error writing to log");
      e.printStackTrace(terminal.writer());
    }
  }
}
