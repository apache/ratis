package org.apache.hadoop.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.raft.RaftProtocol.Response;
import org.apache.hadoop.util.Time;

class LeaderElection {
  public static final Log LOG = LogFactory.getLog(RaftServer.class);

  static String string(List<Response> responses, List<Exception> exceptions) {
    return "received " + responses.size() + " response(s) and "
        + exceptions.size() + " exception(s); "
        + responses + "; " + exceptions;
  }

  enum Result {ELECTED, REJECTED, TIMEOUT, NEWTERM}

  private final RaftServer candidate;
  private final long electionTerm;

  LeaderElection(RaftServer candidate, long electionTerm) {
    this.candidate = candidate;
    this.electionTerm = electionTerm;
  }

  Result begin() throws RaftException, InterruptedException {
    final int size = candidate.getEnsemable().getOtherServers().size();
    final ExecutorService executor = Executors.newFixedThreadPool(size);
    try {
      return election(executor);
    } finally {
      executor.shutdownNow();
    }
  }

  /** submit requestVote to all servers */
  int submitRequestVoteTasks(ExecutorCompletionService<Response> completion)
      throws InterruptedException, RaftException {
    int submitted = 0;
    for(final RaftServer s : candidate.getEnsemable().getOtherServers()) {
      submitted++;
      completion.submit(new Callable<Response>() {
        @Override
        public Response call() throws RaftServerException {
          return candidate.sendRequestVote(electionTerm, s);
        }
      });
    }
    return submitted;
  }

  /** Begin an election and try to become a leader. */
  Result election(ExecutorService executor)
      throws InterruptedException, RaftException {
    final long startTime = Time.monotonicNow();
    final long timeout = startTime + RaftConstants.getRandomElectionWaitTime();

    final ExecutorCompletionService<Response> completion
        = new ExecutorCompletionService<>(executor);
    final int submitted = submitRequestVoteTasks(completion);

    // wait for responses
    final List<Response> responses = new ArrayList<>();
    final List<Exception> exceptions = new ArrayList<>();
    int granted = 1;
    for(;;) {
      final long waitTime = timeout - Time.monotonicNow();
      if (waitTime <= 0) {
        LOG.info("Election timeout: " + string(responses, exceptions));
        return Result.TIMEOUT;
      }
      try {
        final Response r = completion.poll(waitTime, TimeUnit.MILLISECONDS).get();
        if (r.term  > electionTerm) {
          return Result.NEWTERM;
        }

        responses.add(r);
        if (r.success) {
          granted++;
          if (granted > candidate.getEnsemable().size()/2) {
            LOG.info("Election passed: " + string(responses, exceptions));
            return Result.ELECTED;
          }
        }
      } catch(ExecutionException e) {
        LOG.warn("", e);
        exceptions.add(e);
      }

      if (responses.size() + exceptions.size() == submitted) {
        // received all the responses
        LOG.info("Election rejected: " + string(responses, exceptions));
        return Result.REJECTED;
      }
    }
  }
}