package org.apache.ratis.logservice.dummy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.logservice.api.LogReader;

public class DummyLogReader implements LogReader {
  private static final ByteBuffer IMMUTABLE_BYTES = ByteBuffer.wrap(new byte[0]);

  @Override
  public void close() {}

  @Override
  public void seek(long recordId) throws IOException {
    return;
  }

  @Override
  public ByteBuffer readNext() throws IOException {
    return IMMUTABLE_BYTES;
  }

  @Override
  public List<ByteBuffer> readBulk(int numRecords) throws IOException {
    ArrayList<ByteBuffer> records = new ArrayList<>(numRecords);
    for (int i = 0; i < numRecords; i++) {
      records.add(IMMUTABLE_BYTES);
    }
    return records;
  }

}
