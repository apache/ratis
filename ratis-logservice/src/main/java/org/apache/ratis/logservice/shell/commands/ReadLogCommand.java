package org.apache.ratis.logservice.shell.commands;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.client.LogServiceClient;
import org.apache.ratis.logservice.shell.Command;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

public class ReadLogCommand implements Command {

  @Override public String getHelpMessage() {
    return "`read` - Reads all values from the given log";
  }

  @Override
  public void run(Terminal terminal, LineReader lineReader, LogServiceClient client, String[] args) {
    if (args.length != 1) {
      terminal.writer().println("ERROR - Usage: read <name>");
      return;
    }
    String logName = args[0];
    try (LogStream stream = client.getLog(LogName.of(logName));
        LogReader reader = stream.createReader()) {
      long firstId = stream.getStartRecordId();
      long lastId = stream.getLastRecordId();
      StringBuilder sb = new StringBuilder();
      int i = 0;
      List<ByteBuffer> records = reader.readBulk((int) (lastId - firstId));
      for (ByteBuffer record : records) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append(i).append(":");
        if (record != null) {
          String strData = new String(record.array(), record.arrayOffset(), record.remaining(), StandardCharsets.UTF_8);
          sb.append(strData);
        }
      }
      sb.insert(0, "[");
      sb.append("]");
      terminal.writer().println(sb.toString());
    } catch (Exception e) {
      terminal.writer().println("Failed to read from log");
      e.printStackTrace(terminal.writer());
    }
  }
}
