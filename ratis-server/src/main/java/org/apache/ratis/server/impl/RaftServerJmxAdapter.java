package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerMXBean;
import org.apache.ratis.util.JmxRegister;

import javax.management.JMException;
import javax.management.ObjectName;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RaftServerJmxAdapter implements RaftServerMXBean {
  private RaftServerImpl raftServer;
  private JmxRegister jmxRegister;

  public RaftServerJmxAdapter(RaftServerImpl raftServer, JmxRegister jmxRegister) {
    this.raftServer = raftServer;
    this.jmxRegister = jmxRegister;
  }

  @Override
  public String getId() {
    return raftServer.getState().getSelfId().toString();
  }

  @Override
  public String getLeaderId() {
    return raftServer.getState().getLeaderId().toString();
  }

  @Override
  public long getCurrentTerm() {
    return raftServer.getState().getCurrentTerm();
  }

  @Override
  public String getGroupId() {
    return raftServer.getGroupId().toString();
  }

  @Override
  public String getRole() {
    return raftServer.getRole().toString();
  }

  @Override
  public List<String> getFollowers() {
    return raftServer.getRole().getLeaderState().map(LeaderState::getFollowers).orElse(Collections.emptyList())
        .stream().map(RaftPeer::toString).collect(Collectors.toList());
  }

  public boolean registerMBean() {
    RaftPeerId id = raftServer.getState().getSelfId();
    final String prefix = "Ratis:service=RaftServer,group=" + raftServer.getGroupId() + ",id=";
    final String registered = jmxRegister.register(this, Arrays.asList(
        () -> prefix + id,
        () -> prefix + ObjectName.quote(id.toString())));
    return registered != null;
  }

  public void unregister() throws JMException {
    jmxRegister.unregister();
  }
}