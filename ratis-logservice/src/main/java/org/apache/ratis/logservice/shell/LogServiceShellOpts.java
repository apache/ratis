package org.apache.ratis.logservice.shell;

import com.beust.jcommander.Parameter;

public class LogServiceShellOpts {
  @Parameter(names = {"--meta-quorum", "-q"})
  public String metaQuorum;
}