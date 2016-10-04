package org.apache.raft.statemachine;

import org.apache.raft.proto.RaftProtos;
import org.apache.raft.protocol.RaftClientRequest;

import java.util.Optional;

/**
 * Context for a transaction. The transaction might have originated from a client request, or it
 * maybe coming from another replica of the state machine through the RAFT log.
 */
public class TrxContext {

  /** Original request from the client */
  protected final Optional<RaftClientRequest> clientRequest;

  /** Exception from the StateMachine */
  protected final Optional<Exception> exception;

  /** Data from the StateMachine */
  protected final Optional<RaftProtos.SMLogEntryProto> smLogEntryProto;

  /** Context specific to the State machine. The StateMachine can use this object to carry state
   * between startTransaction() and applyLogEntries() */
  protected final Optional<Object> stateMachineContext;

  /**
   * Committed LogEntry.
   */
  protected Optional<RaftProtos.LogEntryProto> logEntry = Optional.empty();

  public TrxContext(RaftClientRequest clientRequest, RaftProtos.SMLogEntryProto smLogEntryProto) {
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.of(smLogEntryProto);
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.empty();
  }

  public TrxContext(RaftClientRequest clientRequest, RaftProtos.SMLogEntryProto smLogEntryProto,
                    Object stateMachineContext) {
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.of(smLogEntryProto);
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.of(stateMachineContext);
  }

  public TrxContext(RaftClientRequest clientRequest, Exception exception) {
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.empty();
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.empty();
  }

  public TrxContext(RaftClientRequest clientRequest, Exception exception,
                    Object stateMachineContext) {
    this.clientRequest = Optional.of(clientRequest);
    this.smLogEntryProto = Optional.empty();
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.of(stateMachineContext);
  }

  /**
   * Construct a TrxContext from a LogEntry. Used by followers for applying committed entries to the
   * state machine
   * @param logEntry
   */
  public TrxContext(RaftProtos.LogEntryProto logEntry) {
    this.clientRequest = Optional.empty();
    this.smLogEntryProto = Optional.of(logEntry.getSmLogEntry());
    this.exception = Optional.empty();
    this.stateMachineContext = Optional.empty();
    this.logEntry = Optional.of(logEntry);
  }

  public Optional<RaftClientRequest> getClientRequest() {
    return this.clientRequest;
  }

  public Optional<RaftProtos.SMLogEntryProto> getSMLogEntry() {
    return this.smLogEntryProto;
  }

  public Optional<Exception> getException() {
    return this.exception;
  }

  public Optional<Object> getStateMachineContext() {
    return stateMachineContext;
  }

  public void setLogEntry(Optional<RaftProtos.LogEntryProto> logEntry) {
    this.logEntry = logEntry;
  }

  public Optional<RaftProtos.LogEntryProto> getLogEntry() {
    return logEntry;
  }
}
