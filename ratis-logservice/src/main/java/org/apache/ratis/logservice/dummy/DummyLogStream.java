package org.apache.ratis.logservice.dummy;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStreamConfiguration;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.api.RecordListener;

public class DummyLogStream implements LogStream {
  private final LogName name;
  private final DummyLogService service;

  public DummyLogStream(DummyLogService service, LogName name) {
    this.service = Objects.requireNonNull(service);
    this.name = Objects.requireNonNull(name);
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
  public LogReader createReader() {
    return new DummyLogReader();
  }

  @Override
  public LogWriter createWriter() {
    return new DummyLogWriter();
  }

  @Override
  public Set<RecordListener> getRecordListeners() {
    Set<RecordListener> listeners = service.recordListeners.get(name);
    if (listeners == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(listeners);
  }

  @Override
  public State getState() {
    return State.OPEN;
  }

  @Override
  public long getLastRecordId() {
    return 0;
  }

  @Override
  public LogStreamConfiguration getConfiguration() {
    return null;
  }

  @Override public void close() throws IOException {}
}
