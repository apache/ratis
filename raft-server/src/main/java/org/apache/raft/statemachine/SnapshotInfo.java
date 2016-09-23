package org.apache.raft.statemachine;

import org.apache.raft.server.RaftConfiguration;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.FileInfo;

import java.util.List;

/**
 * SnapshotInfo represents a durable state by the state machine. The state machine implementation is
 * responsible for the layout of the snapshot files as well as making the data durable. Latest term,
 * latest index, and the raft configuration must be saved together with any data files in the
 * snapshot.
 */
public interface SnapshotInfo {

  /**
   * Returns the term and index corresponding to this snapshot.
   * @return The term and index corresponding to this snapshot.
   */
  TermIndex getTermIndex();

  /**
   * Returns the term corresponding to this snapshot.
   * @return The term corresponding to this snapshot.
   */
  long getTerm();

  /**
   * Returns the index corresponding to this snapshot.
   * @return The index corresponding to this snapshot.
   */
  long getIndex();

  /**
   * Returns a list of files corresponding to this snapshot. This list should include all
   * the files that the state machine keeps in its data directory. This list of files will be
   * copied as to other replicas in install snapshot RPCs.
   * @return a list of Files corresponding to the this snapshot.
   */
  List<FileInfo> getFiles();

  /**
   * Returns the RaftConfiguration corresponding to this snapshot.
   * @return the RaftConfiguration corresponding to this snapshot.
   */
  RaftConfiguration getRaftConfiguration();
}
