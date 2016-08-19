package org.apache.raft.hadoopRpc.server;

import org.apache.raft.protocol.NotLeaderException;
import org.apache.raft.protocol.RaftClientProtocol;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.protocol.AppendEntriesReply;
import org.apache.raft.server.protocol.AppendEntriesRequest;
import org.apache.raft.server.protocol.InstallSnapshotReply;
import org.apache.raft.server.protocol.InstallSnapshotRequest;
import org.apache.raft.server.protocol.RaftServerProtocol;
import org.apache.raft.server.protocol.RequestVoteReply;
import org.apache.raft.server.protocol.RequestVoteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RaftServerRpcService implements RaftClientProtocol, RaftServerProtocol {
  static final Logger LOG = LoggerFactory.getLogger(RaftServerRpcService.class);
  private final RaftServer server;

  public RaftServerRpcService(RaftServer server) {
    this.server = server;
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    CompletableFuture<RaftClientReply> future = server.submitClientRequest(request);
    return waitForReply(request, future);
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    CompletableFuture<RaftClientReply> future = server.setConfiguration(request);
    return waitForReply(request, future);
  }

  private RaftClientReply waitForReply(RaftClientRequest request,
      CompletableFuture<RaftClientReply> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      LOG.info("Interrupted when waiting for reply", e);
      throw new InterruptedIOException("Interrupted when waiting for reply");
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof NotLeaderException) {
        return new RaftClientReply(request, false, (NotLeaderException) cause);
      } else if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw cause != null ? new IOException(cause) : new IOException(e);
      }
    }
  }

  @Override
  public RequestVoteReply requestVote(RequestVoteRequest request)
      throws IOException {
    return server.requestVote(request);
  }

  @Override
  public AppendEntriesReply appendEntries(AppendEntriesRequest request)
      throws IOException {
    return server.appendEntries(request);
  }

  @Override
  public InstallSnapshotReply installSnapshot(InstallSnapshotRequest request)
      throws IOException {
    return server.installSnapshot(request);
  }

  public String getId() {
    return server.getId();
  }
}
