/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.util;

import org.apache.ratis.protocol.AlreadyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongFunction;

/**
 * A single-client-to-multiple-server sliding window.
 * The client only talks to a server at any time.
 * When the current server fails, the client fails over to another server.
 */
public interface SlidingWindow {
  Logger LOG = LoggerFactory.getLogger(SlidingWindow.class);

  interface Request<REPLY> {
    long getSeqNum();

    void setReply(REPLY reply);

    boolean hasReply();

    default void fail(Exception e) {
    }
  }

  /** A seqNum-to-request map, sorted by seqNum. */
  class RequestMap<REQUEST extends Request<REPLY>, REPLY> implements Iterable<REQUEST> {
    static boolean LOG_REPEATEDLY = false;
    private final Object name;
    /** Request map: seqNum -> request */
    private final SortedMap<Long, REQUEST> requests = new TreeMap<>();

    RequestMap(Object name) {
      this.name = name;
      if (LOG_REPEATEDLY && LOG.isDebugEnabled()) {
        JavaUtils.runRepeatedly(() -> log(), 5, 10, TimeUnit.SECONDS);
      }
    }

    Object getName() {
      return name;
    }

    boolean isEmpty() {
      return requests.isEmpty();
    }

    /**
     * If the request with the given seqNum is non-replied, return it.
     * Otherwise, return null.
     *
     * A request is non-replied if
     * (1) it is in the request map, and
     * (2) it does not has reply.
     */
    REQUEST getNonRepliedRequest(long seqNum, String op) {
      final REQUEST request = requests.get(seqNum);
      if (request == null) {
        LOG.debug("{}: {}, seq={} not found in {}", getName(), op, seqNum, this);
        return null;
      }
      if (request.hasReply()) {
        LOG.debug("{}: {}, seq={} already has replied in {}", getName(), op, seqNum, this);
        return null;
      }
      return request;
    }

    long firstSeqNum() {
      return requests.firstKey();
    }

    long lastSeqNum() {
      return requests.lastKey();
    }

    /** Iterate the requests in the order of seqNum. */
    @Override
    public Iterator<REQUEST> iterator() {
      return requests.values().iterator();
    }

    void putNewRequest(REQUEST request) {
      final long seqNum = request.getSeqNum();
      CollectionUtils.putNew(seqNum, request, requests, () -> getName() + ":requests");
    }

    /**
     * Set reply for the request with the given seqNum if it is non-replied.
     * Otherwise, do nothing.
     *
     * @return true iff this method does set the reply for the request.
     */
    boolean setReply(long seqNum, REPLY reply, String op) {
      final REQUEST request = getNonRepliedRequest(seqNum, op);
      if (request == null) {
        LOG.debug("{}: DUPLICATED reply {} for seq={} in {}", getName(), reply, seqNum, this);
        return false;
      }

      LOG.debug("{}: set reply {} for seq={} in {}", getName(), reply, seqNum, this);
      request.setReply(reply);
      return true;
    }

    synchronized void clear() {
      LOG.debug("close {}", this);
      requests.clear();
    }

    synchronized void log() {
      LOG.debug(this.toString());
      for(REQUEST r : requests.values()) {
        LOG.debug("  {}: hasReply? {}", r.getSeqNum(), r.hasReply());
      }
    }

    @Override
    public String toString() {
      return getName() + ": requests" + asString(requests);
    }

    private static String asString(SortedMap<Long, ?> map) {
      return map.isEmpty()? "[]": "[" + map.firstKey() + ".." + map.lastKey() + "]";
    }
  }

  /**
   * Client side sliding window.
   * A client may
   * (1) allocate seqNum for new requests;
   * (2) send requests/retries to the server;
   * (3) receive replies/exceptions from the server;
   * (4) return the replies/exceptions to client.
   *
   * Depend on the replies/exceptions, the client may retry the requests
   * to the same or a different server.
   */
  class Client<REQUEST extends Request<REPLY>, REPLY> {
    /** The requests in the sliding window. */
    private final RequestMap<REQUEST, REPLY> requests;
    /** Delayed requests. */
    private final SortedMap<Long, Long> delayedRequests = new TreeMap<>();

    /** The seqNum for the next new request. */
    private long nextSeqNum = 0;
    /** The seqNum of the first request. */
    private long firstSeqNum = -1;
    /** Is the first request replied? */
    private boolean firstReplied;
    /** The exception, if there is any. */
    private Exception exception;

    public Client(Object name) {
      this.requests = new RequestMap<REQUEST, REPLY>(name) {
        @Override
        synchronized void log() {
          LOG.debug(toString());
          for (REQUEST r : requests) {
            LOG.debug("  {}: {}", r.getSeqNum(), r.hasReply() ? "replied"
                : delayedRequests.containsKey(r.getSeqNum()) ? "delayed" : "submitted");
          }
        }
      };
    }

    @Override
    public synchronized String toString() {
      return requests + ", nextSeqNum=" + nextSeqNum
          + ", firstSubmitted=" + firstSeqNum + ", replied? " + firstReplied
          + ", delayed=" + delayedRequests.keySet();
    }

    /**
     * A new request arrives, create it with {@link #nextSeqNum}
     * and then try sending it to the server.
     *
     * @param requestConstructor use seqNum to create a new request.
     * @return the new request.
     */
    public synchronized REQUEST submitNewRequest(
        LongFunction<REQUEST> requestConstructor, Consumer<REQUEST> sendMethod) {
      if (!requests.isEmpty()) {
        Preconditions.assertTrue(nextSeqNum == requests.lastSeqNum() + 1,
            () -> "nextSeqNum=" + nextSeqNum + " but " + this);
      }

      final long seqNum = nextSeqNum++;
      final REQUEST r = requestConstructor.apply(seqNum);

      if (exception != null) {
        alreadyClosed(r, exception);
        return r;
      }

      requests.putNewRequest(r);

      final boolean submitted = sendOrDelayRequest(r, sendMethod);
      LOG.debug("{}: submitting a new request {} in {}? {}",
          requests.getName(), r, this, submitted? "submitted": "delayed");
      return r;
    }

    private boolean sendOrDelayRequest(REQUEST request, Consumer<REQUEST> sendMethod) {
      final long seqNum = request.getSeqNum();
      Preconditions.assertTrue(requests.getNonRepliedRequest(seqNum, "sendOrDelayRequest") == request);

      if (firstReplied) {
        // already received the reply for the first request, submit any request.
        sendMethod.accept(request);
        return true;
      }

      if (firstSeqNum == -1 && seqNum == requests.firstSeqNum()) {
        // first request is not yet submitted and this is the first request, submit it.
        LOG.debug("{}: detect firstSubmitted {} in {}", requests.getName(), request, this);
        firstSeqNum = seqNum;
        sendMethod.accept(request);
        return true;
      }

      // delay other requests
      CollectionUtils.putNew(seqNum, seqNum, delayedRequests, () -> requests.getName() + ":delayedRequests");
      return false;
    }

    /** Receive a retry from an existing request (may out-of-order). */
    public synchronized void retry(REQUEST request, Consumer<REQUEST> sendMethod) {
      if (requests.getNonRepliedRequest(request.getSeqNum(), "retry") != request) {
        // out-dated or invalid retry
        LOG.debug("{}: Ignore retry {} in {}", requests.getName(), request, this);
        return;
      }
      final boolean submitted = sendOrDelayRequest(request, sendMethod);
      LOG.debug("{}: submitting a retry {} in {}? {}",
          requests.getName(), request, this, submitted? "submitted": "delayed");
    }

    private void removeRepliedFromHead() {
      for (final Iterator<REQUEST> i = requests.iterator(); i.hasNext(); i.remove()) {
        final REQUEST r = i.next();
        if (!r.hasReply()) {
          return;
        }
      }
    }

    /**
     * Receive a reply with the given seqNum (may out-of-order).
     * It may trigger the client to send delayed requests.
     */
    public synchronized void receiveReply(
        long seqNum, REPLY reply, Consumer<REQUEST> sendMethod) {
      if (!requests.setReply(seqNum, reply, "receiveReply")) {
        return; // request already replied
      }
      if (seqNum == firstSeqNum) {
        firstReplied = true; // received the reply for the first submitted request
      }
      removeRepliedFromHead();
      trySendDelayed(sendMethod);
    }

    private void trySendDelayed(Consumer<REQUEST> sendMethod) {
      if (firstReplied) {
        // after first received, all other requests can be submitted (out-of-order)
        if (!delayedRequests.isEmpty()) {
          for (Long seqNum : delayedRequests.keySet()) {
            sendMethod.accept(requests.getNonRepliedRequest(seqNum, "trySendDelayed"));
          }
          delayedRequests.clear();
        }
      } else {
        // Otherwise, submit the first only if it is a delayed request
        final Iterator<REQUEST> i = requests.iterator();
        if (i.hasNext()) {
          final REQUEST r = i.next();
          final Long delayed = delayedRequests.remove(r.getSeqNum());
          if (delayed != null) {
            sendOrDelayRequest(r, sendMethod);
          }
        }
      }
    }

    /** Reset the {@link #firstSeqNum} The stream has an error. */
    public synchronized void resetFirstSeqNum() {
      firstSeqNum = -1;
      firstReplied = false;
      LOG.debug("After resetFirstSeqNum: {}", this);
    }

    /** Fail all requests starting from the given seqNum. */
    public synchronized void fail(final long startingSeqNum, Exception e) {
      exception = e;

      boolean handled = false;
      for(long i = startingSeqNum; i <= requests.lastSeqNum(); i++) {
        final REQUEST request = requests.getNonRepliedRequest(i, "fail");
        if (request != null) {
          if (request.getSeqNum() == startingSeqNum) {
            request.fail(e);
          } else {
            alreadyClosed(request, e);
          }
          handled = true;
        }
      }

      if (handled) {
        removeRepliedFromHead();
      }
    }

    private void alreadyClosed(REQUEST request, Exception e) {
      request.fail(new AlreadyClosedException(SlidingWindow.class.getSimpleName() + "$" + getClass().getSimpleName()
          + " " + requests.getName() + " is closed.", e));
    }
  }

  /**
   * Server side sliding window.
   * A server may
   * (1) receive requests from client;
   * (2) submit the requests for processing;
   * (3) receive replies from the processing unit;
   * (4) send replies to the client.
   */
  class Server<REQUEST extends Request<REPLY>, REPLY> implements Closeable {
    /** The requests in the sliding window. */
    private final RequestMap<REQUEST, REPLY> requests;
    /** The end of requests */
    private final REQUEST end;

    private long nextToProcess = -1;

    public Server(Object name, REQUEST end) {
      this.requests = new RequestMap<>(name);
      this.end = end;
      Preconditions.assertTrue(end.getSeqNum() == Long.MAX_VALUE);
    }

    @Override
    public synchronized String toString() {
      return requests + ", nextToProcess=" + nextToProcess;
    }

    /** A request (or a retry) arrives (may be out-of-order except for the first request). */
    public synchronized void receivedRequest(REQUEST request, Consumer<REQUEST> processingMethod) {
      final long seqNum = request.getSeqNum();
      if (nextToProcess == -1) {
        nextToProcess = seqNum;
        LOG.debug("{}: got seq={} (first request), set nextToProcess in {}", requests.getName(), seqNum, this);
      } else {
        LOG.debug("{}: got seq={} in {}", requests.getName(), seqNum, this);
      }
      requests.putNewRequest(request);
      processRequestsFromHead(processingMethod);
    }

    private void processRequestsFromHead(Consumer<REQUEST> processingMethod) {
      for(REQUEST r : requests) {
        if (r.getSeqNum() != nextToProcess) {
          return;
        }
        processingMethod.accept(r);
        nextToProcess++;
      }
    }

    /**
     * Receives a reply for the given seqNum (may out-of-order) from the processor.
     * It may trigger sending replies to client or processing more requests.
     */
    public synchronized void receiveReply(
        long seqNum, REPLY reply, Consumer<REQUEST> replyMethod, Consumer<REQUEST> processingMethod) {
      if (!requests.setReply(seqNum, reply, "receiveReply")) {
        return; // request already replied
      }
      sendRepliesFromHead(replyMethod);
      processRequestsFromHead(processingMethod);
    }

    private void sendRepliesFromHead(
        Consumer<REQUEST> replyMethod
    ) {
      for(final Iterator<REQUEST> i = requests.iterator(); i.hasNext(); i.remove()) {
        final REQUEST r = i.next();
        if (!r.hasReply()) {
          return;
        }
        replyMethod.accept(r);
        if (r == end) {
          return;
        }
      }
    }

    /**
     * Signal the end of requests.
     * @return true if no more outstanding requests.
     */
    public synchronized boolean endOfRequests() {
      if (requests.isEmpty()) {
        return true;
      } else {
        LOG.debug("{}: put end-of-request in {}", requests.getName(), this);
        requests.putNewRequest(end);
        return false;
      }
    }

    @Override
    public void close() {
      requests.clear();
    }
  }
}