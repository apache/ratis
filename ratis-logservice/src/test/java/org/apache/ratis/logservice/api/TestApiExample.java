package org.apache.ratis.logservice.api;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.ratis.logservice.dummy.DummyLogService;
import org.junit.Test;

/**
 * Example usage of the LogService API with dummy objects.
 */
public class TestApiExample {

  byte[] intToBytes(int i) {
    return Integer.toString(i).getBytes(StandardCharsets.UTF_8); 
  }

  @Test
  public void test() throws IOException, InterruptedException, ExecutionException {
    try (LogService svc = new DummyLogService();
        LogStream log1 = svc.createLog(LogName.of("log1"))) {
      // Write some data
      try (LogWriter writer = log1.createWriter()) {
        for (int i = 0; i < 5; i++) {
          writer.write(ByteBuffer.wrap(intToBytes(i)));
        }

        List<ByteBuffer> records = new ArrayList<>(5);
        for (int i = 5; i < 10; i++) {
          records.add(ByteBuffer.wrap(intToBytes(i)));
        }
        writer.writeMany(records);
      }

      // Read some data
      try (LogReader reader = log1.createReader()) {
        // Seek the reader
        reader.seek(0);
        List<ByteBuffer> records = reader.readBulk(10);
        assertEquals(10, records.size());
      }

      svc.deleteLog(log1.getName());
    }
  }
}
