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

import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * A queue with execution order guarantee such that
 * each task is submitted for execution only if it becomes the head of the queue.
 * Tasks are executed sequentially without any overlap.
 *
 * By the definition of a queue, a task can become the head iff
 * (1) the queue is empty when offering it, or
 * (2) it is the next to the head and the head is polled out from the queue.
 *
 * A typically use case is to submit concurrent tasks
 * with in-order guarantee for some of the tasks.
 *
 * One example use case is to submit tasks to write multiple files:
 * - A file may requires multiple write tasks.
 * - Multiple files are written at the same time.
 * A solution is to create a {@link TaskQueue} for each file
 * and then submit the write tasks to the corresponding queue.
 * The files will be written concurrently and the writes to each file are in-order.
 */
public class TaskQueue {
  public static final Logger LOG = LoggerFactory.getLogger(TaskQueue.class);

  private final String name;
  private final Queue<Runnable> q = new LinkedList<>();

  public TaskQueue(String name) {
    this.name = name;
  }

  /**
   * Poll the current head from this queue
   * and then submit the next head, if there is any.
   */
  private synchronized Runnable pollAndSubmit(ExecutorService executor) {
    final Runnable head = q.poll();
    final Runnable next = q.peek();
    if (next != null) {
      executor.submit(next);
    }
    return head;
  }

  /**
   * Offer the given task to this queue.
   * If it is the first task, submit it.
   */
  private synchronized void offerAndSubmit(Runnable task, ExecutorService executor) {
    q.offer(task);
    if (q.size() == 1) {
      executor.submit(task);
    }
  }

  /**
   * The same as submit(task, executor, Function.identity());
   */
  public <OUTPUT, THROWABLE extends Throwable> CompletableFuture<OUTPUT> submit(
      CheckedSupplier<OUTPUT, THROWABLE> task, ExecutorService executor) {
    return submit(task, executor, Function.identity());
  }

  /**
   * Offer the given task to this queue
   * and then submit the tasks one by one in the queue order for execution.
   *
   * @param task the task to be submitted.
   * @param executor to execute tasks.
   * @param newThrowable When the task throws a throwable, create a new Throwable
   *                     in order to include more error message.
   * @param <OUTPUT> the output type of the task.
   * @param <THROWABLE> the throwable type of the task.
   * @return a future of the output.
   */
  public <OUTPUT, THROWABLE extends Throwable> CompletableFuture<OUTPUT> submit(
      CheckedSupplier<OUTPUT, THROWABLE> task, ExecutorService executor,
      Function<Throwable, Throwable> newThrowable) {
    final CompletableFuture<OUTPUT> f = new CompletableFuture<>();
    final Runnable runnable = LogUtils.newRunnable(LOG, () -> {
      LOG.trace("{}: running {}", this, task);
      try {
        f.complete(task.get());
      } catch (Throwable t) {
        f.completeExceptionally(newThrowable.apply(t));
      }

      pollAndSubmit(executor);
    }, task::toString);

    offerAndSubmit(runnable, executor);
    return f;
  }

  @Override
  public String toString() {
    return name + "-" + JavaUtils.getClassSimpleName(getClass());
  }
}
