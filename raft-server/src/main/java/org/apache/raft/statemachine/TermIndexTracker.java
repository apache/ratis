package org.apache.raft.statemachine;

import com.google.common.base.Preconditions;
import org.apache.raft.server.protocol.TermIndex;

import static org.apache.raft.server.RaftServerConstants.INVALID_LOG_INDEX;

/**
 * Tracks the term index that is applied to the StateMachine for simple state machines with
 * no concurrent snapshoting capabilities.
 */
public class TermIndexTracker {
  static final TermIndex INIT_TERMINDEX =
      new TermIndex(INVALID_LOG_INDEX, INVALID_LOG_INDEX);

  private TermIndex current = INIT_TERMINDEX;

  //TODO: developer note: everything is synchronized for now for convenience.

  /**
   * Initialize the tracker with a term index (likely from a snapshot).
   */
  public synchronized void init(TermIndex termIndex) {
    this.current = termIndex;
  }

  public synchronized void reset() {
    init(INIT_TERMINDEX);
  }

  /**
   * Update the tracker with a new TermIndex. It means that the StateMachine has
   * this index in memory.
   */
  public synchronized void update(TermIndex termIndex) {
    Preconditions.checkArgument(termIndex != null &&
        termIndex.compareTo(current) >= 0);
    this.current = termIndex;
  }

  /**
   * Return latest term and index that is inserted to this tracker as an atomic
   * entity.
   */
  public synchronized TermIndex getLatestTermIndex() {
    return current;
  }

}
