package org.apache.raft.statemachine;

import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.FileInfo;

import java.util.List;

public class SnapshotInfoImpl implements SnapshotInfo {

  protected final RaftConfiguration raftConfiguration;
  protected final List<FileInfo> files;
  protected final TermIndex termIndex;

  public SnapshotInfoImpl(RaftConfiguration raftConfiguration,
                          List<FileInfo> files, long term, long index) {
    this.raftConfiguration = raftConfiguration;
    this.files = files;
    this.termIndex = new TermIndex(term, index);
  }

  @Override
  public TermIndex getTermIndex() {
    return termIndex;
  }

  @Override
  public long getTerm() {
    return termIndex.getTerm();
  }

  @Override
  public long getIndex() {
    return termIndex.getIndex();
  }

  @Override
  public List<FileInfo> getFiles() {
    return files;
  }

  @Override
  public RaftConfiguration getRaftConfiguration() {
    return raftConfiguration;
  }

  @Override
  public String toString() {
    return raftConfiguration + "." + files + "." + termIndex;
  }
}
