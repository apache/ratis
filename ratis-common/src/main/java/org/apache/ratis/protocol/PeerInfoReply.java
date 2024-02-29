package org.apache.ratis.protocol;

import org.apache.ratis.proto.RaftProtos;

import java.util.Collection;
import java.util.List;

public class PeerInfoReply extends RaftClientReply {

  private RaftGroup group;
  private RaftProtos.RoleInfoProto roleInfoProto;
  public long currentTerm;
  public long lastCommitIndex;
  public long lastAppliedIndex;
  public List<Long> followerNextIdx;
  public long lastSnapshotIndex;

  public PeerInfoReply(RaftClientRequest request,
                       RaftGroup group,
                       RaftProtos.RoleInfoProto roleInfoProto,
                       Collection<RaftProtos.CommitInfoProto> commitInfos,
                       long currentTerm,
                       long lastCommitIndex,
                       long lastAppliedIndex,
                       List<Long> followerNextIdx,
                       long lastSnapshotIndex
  ) {
    this(request.getClientId(),
        request.getServerId(),
        request.getRaftGroupId(),
        request.getCallId(),
        commitInfos,
        group,
        roleInfoProto,
        currentTerm,
        lastCommitIndex,
        lastAppliedIndex,
        followerNextIdx,
        lastSnapshotIndex);

  }

  public PeerInfoReply(ClientId clientId,
                       RaftPeerId serverId,
                       RaftGroupId groupId,
                       long callId,
                       Collection<RaftProtos.CommitInfoProto> commitInfos,
                       RaftGroup group,
                       RaftProtos.RoleInfoProto roleInfoProto,
                       long currentTerm,
                       long lastCommitIndex,
                       long lastAppliedIndex,
                       List<Long> followerNextIdx,
                       long lastSnapshotIndex) {
    super(clientId, serverId, groupId, callId, true, null, null, 0L, commitInfos);
    this.group = group;
    this.roleInfoProto = roleInfoProto;
    this.currentTerm = currentTerm;
    this.lastCommitIndex = lastCommitIndex;
    this.lastAppliedIndex = lastAppliedIndex;
    this.followerNextIdx = followerNextIdx;
    this.lastSnapshotIndex = lastSnapshotIndex;
  }

  public RaftProtos.RoleInfoProto getRoleInfoProto() {
    return roleInfoProto;
  }

  public RaftGroup getGroup() {
    return group;
  }

  public long getCurrentTerm() {
    return currentTerm;
  }

  public long getLastCommitIndex() {
    return lastCommitIndex;
  }

  public long getLastAppliedIndex() {
    return lastAppliedIndex;
  }

  public List<Long> getFollowerNextIdx() {
    return followerNextIdx;
  }

  public long getLastSnapshotIndex() {
    return lastSnapshotIndex;
  }

}
