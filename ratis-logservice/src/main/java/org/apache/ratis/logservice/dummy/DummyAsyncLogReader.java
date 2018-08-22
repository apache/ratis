package org.apache.ratis.logservice.dummy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.ratis.logservice.api.AsyncLogReader;

public class DummyAsyncLogReader implements AsyncLogReader {
  private static final ByteBuffer IMMUTABLE_BYTES = ByteBuffer.wrap(new byte[0]);

  public DummyAsyncLogReader() {}

  @Override public void close() throws Exception {}

  @Override
  public CompletableFuture<Void> seek(long recordId) throws IOException {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<ByteBuffer> readNext() throws IOException {
    return CompletableFuture.completedFuture(IMMUTABLE_BYTES);
  }

  @Override
  public CompletableFuture<List<ByteBuffer>> readBulk(int numRecords) throws IOException {
    return CompletableFuture.supplyAsync(() -> {
      ArrayList<ByteBuffer> records = new ArrayList<>(numRecords);
      for (int i = 0; i < numRecords; i++) {
        records.add(IMMUTABLE_BYTES);
      }
      return records;
    });
  }

}
