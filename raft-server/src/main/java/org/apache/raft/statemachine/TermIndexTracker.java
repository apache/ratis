package org.apache.raft.statemachine;

import org.apache.raft.server.RaftServerConstants;
import org.apache.raft.server.protocol.TermIndex;

import static org.apache.raft.server.RaftServerConstants.INVALID_LOG_INDEX;

/**
 * Tracks the term index that is applied to the StateMachine for simple state machines with
 * no concurrent snapshoting capabilities.
 */
public class TermIndexTracker {

  private long latestTerm = INVALID_LOG_INDEX;
  private long latestIndex = INVALID_LOG_INDEX;

  //TODO: developer note: everything is synchronized for now for convenience.

  /**
   * Initialize the tracker with a term index (likely from a snapshot).
   */
  public synchronized void init(TermIndex termIndex) {
    this.latestTerm = termIndex.getTerm();
    this.latestIndex = termIndex.getIndex();
  }

  public synchronized void reset() {
    init(new TermIndex(INVALID_LOG_INDEX, INVALID_LOG_INDEX));
  }

  /**
   * Update the tracker with a new TermIndex. It means that the StateMachine has this index
   * in memory.
   */
  public synchronized void update(TermIndex termIndex) {
    update(termIndex.getTerm(), termIndex.getIndex());
  }

  /**
   * Update the tracker with a new term and index. It means that the StateMachine has this index
   * in memory.
   */
  public synchronized void update(long term, long index) {
    this.latestTerm = Math.max(this.latestTerm, term);
    this.latestIndex = Math.max(this.latestIndex, index);
  }

  /**
   * Return latest term and index that is inserted to this tracker as an atomic entity.
   */
  public synchronized TermIndex getLatestTermIndex() {
    return new TermIndex(this.latestTerm, this.latestIndex);
  }

}
