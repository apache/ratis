package org.apache.ratis.logservice.dummy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ratis.logservice.api.AsyncLogWriter;

public class DummyAsyncLogWriter implements AsyncLogWriter {
  private final AtomicLong counter;

  public DummyAsyncLogWriter() {
    this.counter = new AtomicLong(-1);
  }

  @Override public void close() throws Exception {}

  @Override
  public CompletableFuture<Long> write(ByteBuffer data) throws IOException {
    return CompletableFuture.completedFuture(counter.incrementAndGet());
  }

  @Override
  public CompletableFuture<Long> writeMany(List<ByteBuffer> records) throws IOException {
    return CompletableFuture.completedFuture(counter.addAndGet(records.size()));
  }

  @Override
  public CompletableFuture<Long> sync() throws IOException {
    return CompletableFuture.completedFuture(counter.get());
  }

}
