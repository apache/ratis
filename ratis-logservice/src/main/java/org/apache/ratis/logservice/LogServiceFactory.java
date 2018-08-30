package org.apache.ratis.logservice;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.logservice.api.LogService;

public class LogServiceFactory {
  private static final LogServiceFactory INSTANCE = new LogServiceFactory();

  private LogServiceFactory() {}

  /**
   * Creates an implementation of {@link LogService} using the given {@link RaftClient}.
   *
   * @param raftClient The client to a Raft quorum.
   */
  public LogService createLogService(RaftClient raftClient) {
    //TODO return new LogServiceImpl();
    return null;
  }

  /**
   * Returns an instance of the factory to create {@link LogService} instances.
   */
  public static LogServiceFactory getInstance() {
    return INSTANCE;
  }
}
