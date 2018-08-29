package org.apache.ratis.logservice.api;

import java.nio.ByteBuffer;

/**
 * Interface that, when registered with a {@link LogStream}, will receive all records written
 * to that LogStream until it is removed.
 */
public interface RecordListener {

  /**
   * Processes the written record from the LogStream.
   *
   * @param record The record
   */
  void receiveRecord(ByteBuffer record);

}
