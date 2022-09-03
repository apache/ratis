package org.apache.ratis.util;

import javax.annotation.Nullable;

/**
 * An internal error will be recorded so it can be later retrieved for report or recovery.
 */
public interface ErrorRecorded {
  void setError(Throwable t);

  @Nullable
  Throwable getError();
}
