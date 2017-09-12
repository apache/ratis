package org.apache.ratis.protocol;

/**
 * Client sends this request to a server to request for the information about
 * the server itself.
 */
public class ServerInformatonRequest extends RaftClientRequest {
  public ServerInformatonRequest(ClientId clientId, RaftPeerId serverId,
      RaftGroupId groupId, long callId) {
    super(clientId, serverId, groupId, callId, null);
  }
}
