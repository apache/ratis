package org.apache.ratis.logservice.dummy;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogService;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStream.State;
import org.apache.ratis.logservice.api.LogStreamConfiguration;
import org.apache.ratis.logservice.api.RecordListener;

public class DummyLogService implements LogService {
  final ConcurrentHashMap<LogName,Set<RecordListener>> recordListeners = new ConcurrentHashMap<>();

  @Override
  public LogStream createLog(LogName name) {
    return new DummyLogStream(this, name);
  }

  @Override
  public LogStream createLog(LogName name, LogStreamConfiguration config) {
    return new DummyLogStream(this, name);
  }

  @Override
  public LogStream getLog(LogName name) {
    return new DummyLogStream(this, name);
  }

  @Override
  public Iterator<LogStream> listLogs() {
    return Collections.<LogStream> emptyList().iterator();
  }

  @Override public void closeLog(LogName name) {}

  @Override
  public State getState(LogName name) {
    return State.OPEN;
  }

  @Override public void archiveLog(LogName name) {}

  @Override public void deleteLog(LogName name) {}

  @Override public void updateConfiguration(LogName name, LogStreamConfiguration config) {}

  @Override public void addRecordListener(LogName name, RecordListener listener) {
    recordListeners.compute(name, (key, currentValue) -> {
      if (currentValue == null) {
        return new HashSet<RecordListener>(Collections.singleton(listener));
      }
      currentValue.add(listener);
      return currentValue;
    });
  }

  @Override public void removeRecordListener(LogName name, RecordListener listener) {
    recordListeners.compute(name, (key, currentValue) -> {
      if (currentValue == null) {
        return null;
      }
      currentValue.remove(listener);
      return currentValue;
    });
  }

  @Override public void close() throws IOException {}

}
