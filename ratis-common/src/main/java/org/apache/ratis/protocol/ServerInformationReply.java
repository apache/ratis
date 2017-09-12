package org.apache.ratis.protocol;

/**
 * The response of server information request. Sent from server to client.
 *
 * TODO : currently, only information returned is the info of the group the
 * server belongs to.
 */
public class ServerInformationReply extends RaftClientReply {
  RaftGroup group;

  public ServerInformationReply(RaftClientRequest request, Message message,
      RaftGroup group) {
    super(request, message);
    this.group = group;
  }

  public ServerInformationReply(RaftClientRequest request,
      RaftException ex) {
    super(request, ex);
  }

  public RaftGroup getGroup() {
    return group;
  }

  public ServerInformationReply(ClientId clientId, RaftPeerId serverId,
      RaftGroupId groupId, long callId, boolean success, Message message,
      RaftException exception, RaftGroup group) {
    super(clientId, serverId, groupId, callId, success, message, exception);
    this.group = group;
  }
}
