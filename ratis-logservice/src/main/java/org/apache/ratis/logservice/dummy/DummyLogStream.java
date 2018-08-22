package org.apache.ratis.logservice.dummy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ratis.logservice.api.AsyncLogReader;
import org.apache.ratis.logservice.api.AsyncLogWriter;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.api.RecordListener;

public class DummyLogStream implements LogStream {
  private final LogName name;
  private final AtomicLong start;
  private final AtomicLong archivePoint;
  private final List<RecordListener> recordListeners;

  public DummyLogStream(LogName name) {
    this.name = Objects.requireNonNull(name);
    this.start = new AtomicLong(0);
    this.archivePoint = new AtomicLong(0);
    this.recordListeners = Collections.synchronizedList(new ArrayList<>());
  }

  @Override
  public LogName getName() {
    return name;
  }

  @Override
  public long getSizeInBytes() {
    return 0;
  }

  @Override
  public long getSizeInRecords() {
    return 0;
  }

  @Override
  public AsyncLogReader createAsyncReader() {
    return new DummyAsyncLogReader();
  }

  @Override
  public AsyncLogWriter createAsyncWriter() {
    return new DummyAsyncLogWriter();
  }

  @Override
  public LogReader createReader() {
    return new DummyLogReader();
  }

  @Override
  public LogWriter createWriter() {
    return new DummyLogWriter();
  }

  @Override
  public CompletableFuture<Void> truncateBefore(long recordId) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Long> getFirstRecordId() {
    return CompletableFuture.completedFuture(start.get());
  }

  @Override
  public CompletableFuture<Void> archiveBefore(long recordId) {
    return CompletableFuture.supplyAsync(() -> {
      archivePoint.getAndUpdate((before) -> before > recordId ? before : recordId);
      return null;
    });
  }

  @Override
  public CompletableFuture<Long> getArchivalPoint() {
    return CompletableFuture.completedFuture(archivePoint.get());
  }

  @Override
  public void addRecordListener(RecordListener listener) {
    recordListeners.add(listener);
  }

  @Override
  public List<RecordListener> getRecordListeners() {
    return new ArrayList<>(recordListeners);
  }

  @Override
  public void removeRecordListener(RecordListener listener) {
    recordListeners.remove(listener);
  }

}
