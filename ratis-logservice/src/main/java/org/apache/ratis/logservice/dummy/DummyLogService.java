package org.apache.ratis.logservice.dummy;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogService;
import org.apache.ratis.logservice.api.LogStream;

public class DummyLogService implements LogService {

  @Override
  public CompletableFuture<LogStream> createLog(LogName name) {
    return CompletableFuture.completedFuture(new DummyLogStream(name));
  }

  @Override
  public CompletableFuture<LogStream> getLog(LogName name) {
    return CompletableFuture.completedFuture(new DummyLogStream(name));
  }

  @Override
  public CompletableFuture<Iterator<LogStream>> listLogs() {
    return CompletableFuture.completedFuture(Collections.<LogStream> emptyList().iterator());
  }

  @Override
  public CompletableFuture<Void> deleteLog(LogName name) {
    return CompletableFuture.completedFuture(null);
  }

}
