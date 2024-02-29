package org.apache.ratis.protocol;

public class PeerInfoRequest  extends RaftClientRequest{

  public PeerInfoRequest(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId) {
    super(clientId, serverId, groupId, callId, readRequestType());
  }

}
