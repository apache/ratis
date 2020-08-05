package org.apache.ratis.server.impl;

import org.apache.ratis.datastream.StreamFactory;
import org.apache.ratis.server.RaftServer;

public interface ServerStreamFactory extends StreamFactory {
  /**
   * Server implementation for streaming in Raft group
   */
  DataStreamServer newServerStreamApi(RaftServer server);
}
