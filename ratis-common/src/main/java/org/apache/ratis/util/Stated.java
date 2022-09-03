package org.apache.ratis.util;

import javax.annotation.Nullable;

public interface Stated {
  void setError(Throwable t);

  @Nullable
  Throwable getError();
}
