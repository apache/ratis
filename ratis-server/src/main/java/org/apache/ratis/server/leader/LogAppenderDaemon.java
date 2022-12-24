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
package org.apache.ratis.server.leader;

import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;

import java.io.InterruptedIOException;
import java.util.function.UnaryOperator;

import org.apache.ratis.util.LifeCycle.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ratis.util.LifeCycle.State.CLOSED;
import static org.apache.ratis.util.LifeCycle.State.CLOSING;
import static org.apache.ratis.util.LifeCycle.State.EXCEPTION;
import static org.apache.ratis.util.LifeCycle.State.NEW;
import static org.apache.ratis.util.LifeCycle.State.RUNNING;
import static org.apache.ratis.util.LifeCycle.State.STARTING;

class LogAppenderDaemon {
  public static final Logger LOG = LoggerFactory.getLogger(LogAppenderDaemon.class);

  private final String name;
  private final LifeCycle lifeCycle;
  private final Daemon daemon;

  private final LogAppenderBase logAppender;

  LogAppenderDaemon(LogAppenderBase logAppender) {
    this.logAppender = logAppender;
    this.name = logAppender + "-" + JavaUtils.getClassSimpleName(getClass());
    this.lifeCycle = new LifeCycle(name);
    this.daemon = Daemon.newBuilder().setName(name).setRunnable(this::run)
        .setThreadGroup(logAppender.getServer().getThreadGroup()).build();
  }

  public boolean isWorking() {
    return !LifeCycle.States.CLOSING_OR_CLOSED_OR_EXCEPTION.contains(lifeCycle.getCurrentState());
  }

  public void tryToStart() {
    if (lifeCycle.compareAndTransition(NEW, STARTING)) {
      daemon.start();
    }
  }

  static final UnaryOperator<State> TRY_TO_RUN = current -> {
    if (current == STARTING) {
      return RUNNING;
    } else if (LifeCycle.States.CLOSING_OR_CLOSED_OR_EXCEPTION.contains(current)) {
      return current;
    }
    // Other states are illegal.
    throw new IllegalArgumentException("Cannot to tryToRun from " + current);
  };

  private void run() {
    try {
      if (lifeCycle.transition(TRY_TO_RUN) == RUNNING) {
        logAppender.run();
      }
      lifeCycle.compareAndTransition(RUNNING, CLOSING);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info(this + " was interrupted: " + e);
    } catch (InterruptedIOException e) {
      LOG.info(this + " I/O was interrupted: " + e);
    } catch (Throwable e) {
      LOG.error(this + " failed", e);
      lifeCycle.transitionIfValid(EXCEPTION);
    } finally {
      if (lifeCycle.transitionAndGet(TRANSITION_FINALLY) == EXCEPTION) {
        logAppender.restart();
      }
    }
  }

  static final UnaryOperator<State> TRANSITION_FINALLY = current -> {
    if (State.isValid(current, CLOSED)) {
      return CLOSED;
    } else if (State.isValid(current, EXCEPTION)) {
      return EXCEPTION;
    } else {
      return current;
    }
  };

  public void tryToClose() {
    if (lifeCycle.transition(TRY_TO_CLOSE) == CLOSING) {
      daemon.interrupt();
    }
  }

  static final UnaryOperator<State> TRY_TO_CLOSE = current -> {
    if (current == NEW) {
      return CLOSED;
    } else if (current.isClosingOrClosed()) {
      return current;
    } else if (State.isValid(current, CLOSING)) {
      return CLOSING;
    }
    // Other states are illegal.
    throw new IllegalArgumentException("Cannot to tryToClose from " + current);
  };

  @Override
  public String toString() {
    return name;
  }
}
