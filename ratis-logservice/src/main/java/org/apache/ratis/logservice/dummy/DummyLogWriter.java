package org.apache.ratis.logservice.dummy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ratis.logservice.api.LogWriter;

public class DummyLogWriter implements LogWriter {
  private final AtomicLong counter;

  public DummyLogWriter() {
    this.counter = new AtomicLong(-1);
  }

  @Override public void close() {}

  @Override
  public long write(ByteBuffer data) throws IOException {
    return counter.incrementAndGet();
  }

  @Override
  public long writeMany(List<ByteBuffer> records) throws IOException {
    return counter.addAndGet(records.size());
  }

  @Override
  public long sync() throws IOException {
    return counter.get();
  }

}
