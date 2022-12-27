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

package org.apache.ratis;

import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import org.apache.log4j.Level;
import org.apache.ratis.client.impl.OrderedAsync;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.conf.ReconfigurationBase;
import org.apache.ratis.conf.ReconfigurationException;
import org.apache.ratis.conf.ReconfigurationTaskStatus;
import org.apache.ratis.conf.ReconfigurationUtil;
import org.apache.ratis.conf.ReconfigurationUtil.PropertyChange;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.thirdparty.com.google.common.collect.Lists;
import org.apache.ratis.util.Log4jUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

public abstract class TestReConfigProperty<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    Log4jUtils.setLogLevel(OrderedAsync.LOG, Level.DEBUG);
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
  }

  private RaftProperties conf1;
  private RaftProperties conf2;

  private static final String PROP1 = "test.prop.one";
  private static final String PROP2 = "test.prop.two";
  private static final String PROP3 = "test.prop.three";
  private static final String PROP4 = "test.prop.four";
  private static final String PROP5 = "test.prop.five";

  private static final String VAL1 = "val1";
  private static final String VAL2 = "val2";


  @Before
  public void setup () {
    conf1 = new RaftProperties();
    conf2 = new RaftProperties();

    // set some test properties
    conf1.set(PROP1, VAL1);
    conf1.set(PROP2, VAL1);
    conf1.set(PROP3, VAL1);

    conf2.set(PROP1, VAL1); // same as conf1
    conf2.set(PROP2, VAL2); // different value as conf1
    // PROP3 not set in conf2
    conf2.set(PROP4, VAL1); // not set in conf1

  }

  @Test
  public void testGetChangedProperty() {
    Collection<ReconfigurationUtil.PropertyChange> changes
        = ReconfigurationUtil.getChangedProperties(conf2, conf1);

    Assert.assertTrue("expected 3 changed properties but got " + changes.size(),
        changes.size() == 3);

    boolean changeFound = false;
    boolean unsetFound = false;
    boolean setFound = false;

    for (ReconfigurationUtil.PropertyChange c: changes) {
      if (c.prop.equals(PROP2) && c.oldVal != null && c.oldVal.equals(VAL1) &&
          c.newVal != null && c.newVal.equals(VAL2)) {
        changeFound = true;
      } else if (c.prop.equals(PROP3) && c.oldVal != null && c.oldVal.equals(VAL1) &&
          c.newVal == null) {
        unsetFound = true;
      } else if (c.prop.equals(PROP4) && c.oldVal == null &&
          c.newVal != null && c.newVal.equals(VAL1)) {
        setFound = true;
      }
    }
    Assert.assertTrue("not all changes have been applied",
        changeFound && unsetFound && setFound);
  }

  /**
   * a simple reconfigurable class
   */
  public static class ReconfigurableDummy extends ReconfigurationBase
      implements Runnable {
    public volatile boolean running = true;

    public ReconfigurableDummy(RaftProperties prop) {
      super(prop);
    }

    @Override
    protected RaftProperties getNewConf() {
      return new RaftProperties();
    }

    @Override
    public Collection<String> getReconfigurableProperties() {
      return Arrays.asList(PROP1, PROP2, PROP4);
    }

    @Override
    public synchronized String reconfigurePropertyImpl(
        String property, String newVal) throws ReconfigurationException {
      // do nothing
      return newVal;
    }

    /**
     * Run until PROP1 is no longer VAL1.
     */
    @Override
    public void run() {
      while (running && getConf().get(PROP1).equals(VAL1)) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException ignore) {
          // do nothing
        }
      }
    }

  }

  /**
   * Test reconfiguring a Reconfigurable.
   */
  @Test
  public void testReconfigure() {
    ReconfigurableDummy dummy = new ReconfigurableDummy(conf1);

    Assert.assertTrue(PROP1 + " set to wrong value ",
        dummy.getConf().get(PROP1).equals(VAL1));
    Assert.assertTrue(PROP2 + " set to wrong value ",
        dummy.getConf().get(PROP2).equals(VAL1));
    Assert.assertTrue(PROP3 + " set to wrong value ",
        dummy.getConf().get(PROP3).equals(VAL1));
    Assert.assertTrue(PROP4 + " set to wrong value ",
        dummy.getConf().get(PROP4) == null);
    Assert.assertTrue(PROP5 + " set to wrong value ",
        dummy.getConf().get(PROP5) == null);

    Assert.assertTrue(PROP1 + " should be reconfigurable ",
        dummy.isPropertyReconfigurable(PROP1));
    Assert.assertTrue(PROP2 + " should be reconfigurable ",
        dummy.isPropertyReconfigurable(PROP2));
    Assert.assertFalse(PROP3 + " should not be reconfigurable ",
        dummy.isPropertyReconfigurable(PROP3));
    Assert.assertTrue(PROP4 + " should be reconfigurable ",
        dummy.isPropertyReconfigurable(PROP4));
    Assert.assertFalse(PROP5 + " should not be reconfigurable ",
        dummy.isPropertyReconfigurable(PROP5));

    // change something to the same value as before
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP1, VAL1);
        Assert.assertTrue(PROP1 + " set to wrong value ",
            dummy.getConf().get(PROP1).equals(VAL1));
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      Assert.assertFalse("received unexpected exception",
          exceptionCaught);
    }

    // change something to null
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP1, null);
        Assert.assertTrue(PROP1 + "set to wrong value ",
            dummy.getConf().get(PROP1) == null);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      Assert.assertFalse("received unexpected exception",
          exceptionCaught);
    }

    // change something to a different value than before
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP1, VAL2);
        Assert.assertTrue(PROP1 + "set to wrong value ",
            dummy.getConf().get(PROP1).equals(VAL2));
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      Assert.assertFalse("received unexpected exception",
          exceptionCaught);
    }

    // set unset property to null
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP4, null);
        Assert.assertTrue(PROP4 + "set to wrong value ",
            dummy.getConf().get(PROP4) == null);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      Assert.assertFalse("received unexpected exception",
          exceptionCaught);
    }

    // set unset property
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP4, VAL1);
        Assert.assertTrue(PROP4 + "set to wrong value ",
            dummy.getConf().get(PROP4).equals(VAL1));
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      Assert.assertFalse("received unexpected exception",
          exceptionCaught);
    }

    // try to set unset property to null (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP5, null);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      Assert.assertTrue("did not receive expected exception",
          exceptionCaught);
    }

    // try to set unset property to value (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP5, VAL1);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      Assert.assertTrue("did not receive expected exception",
          exceptionCaught);
    }

    // try to change property to value (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP3, VAL2);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      Assert.assertTrue("did not receive expected exception",
          exceptionCaught);
    }

    // try to change property to null (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP3, null);
      } catch (ReconfigurationException e) {
        exceptionCaught = true;
      }
      Assert.assertTrue("did not receive expected exception",
          exceptionCaught);
    }
  }

  /**
   * Test whether configuration changes are visible in another thread.
   */
  @Test
  public void testThread() throws ReconfigurationException {
    ReconfigurableDummy dummy = new ReconfigurableDummy(conf1);
    Assert.assertTrue(dummy.getConf().get(PROP1).equals(VAL1));
    Thread dummyThread = new Thread(dummy);
    dummyThread.start();
    try {
      Thread.sleep(500);
    } catch (InterruptedException ignore) {
      // do nothing
    }
    dummy.reconfigureProperty(PROP1, VAL2);

    long endWait = System.currentTimeMillis() + 2000;
    while (dummyThread.isAlive() && System.currentTimeMillis() < endWait) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException ignore) {
        // do nothing
      }
    }

    Assert.assertFalse("dummy thread should not be alive",
        dummyThread.isAlive());
    dummy.running = false;
    try {
      dummyThread.join();
    } catch (InterruptedException ignore) {
      // do nothing
    }
    Assert.assertTrue(PROP1 + " is set to wrong value",
        dummy.getConf().get(PROP1).equals(VAL2));

  }

  private static class AsyncReconfigurableDummy extends ReconfigurationBase {

    AsyncReconfigurableDummy(RaftProperties prop) {
      super(prop);
    }

    @Override
    protected RaftProperties getNewConf() {
      return new RaftProperties();
    }

    final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public Collection<String> getReconfigurableProperties() {
      return Arrays.asList(PROP1, PROP2, PROP4);
    }

    @Override
    public synchronized String reconfigurePropertyImpl(String property,
        String newVal) throws ReconfigurationException {
      try {
        latch.await();
      } catch (InterruptedException e) {
        // Ignore
      }
      return newVal;
    }
  }

  private static void waitAsyncReconfigureTaskFinish(ReconfigurationBase rb)
      throws InterruptedException {
    ReconfigurationTaskStatus status = null;
    int count = 20;
    while (count > 0) {
      status = rb.getReconfigurationTaskStatus();
      if (status.stopped()) {
        break;
      }
      count--;
      Thread.sleep(500);
    }
    assert(status.stopped());
  }

  @Test
  public void testAsyncReconfigure()
      throws ReconfigurationException, IOException, InterruptedException {
    AsyncReconfigurableDummy dummy = spy(new AsyncReconfigurableDummy(conf1));

    List<PropertyChange> changes = Lists.newArrayList();
    changes.add(new PropertyChange("name1", "new1", "old1"));
    changes.add(new PropertyChange("name2", "new2", "old2"));
    changes.add(new PropertyChange("name3", "new3", "old3"));
    doReturn(changes).when(dummy).getChangedProperties(
        any(RaftProperties.class), any(RaftProperties.class));

    doReturn(true).when(dummy).isPropertyReconfigurable(eq("name1"));
    doReturn(false).when(dummy).isPropertyReconfigurable(eq("name2"));
    doReturn(true).when(dummy).isPropertyReconfigurable(eq("name3"));

    doReturn("dummy").when(dummy)
        .reconfigurePropertyImpl(eq("name1"), anyString());
    doReturn("dummy").when(dummy)
        .reconfigurePropertyImpl(eq("name2"), anyString());
    doThrow(new ReconfigurationException("NAME3", "NEW3", "OLD3",
        new IOException("io exception")))
        .when(dummy).reconfigurePropertyImpl(eq("name3"), anyString());

    dummy.startReconfigurationTask();

    waitAsyncReconfigureTaskFinish(dummy);
    ReconfigurationTaskStatus status = dummy.getReconfigurationTaskStatus();
    Assert.assertEquals(2, status.getStatus().size());
    for (Map.Entry<PropertyChange, Optional<String>> result :
        status.getStatus().entrySet()) {
      PropertyChange change = result.getKey();
      if (change.prop.equals("name1")) {
        Assert.assertFalse(result.getValue().isPresent());
      } else if (change.prop.equals("name2")) {
        Assert.assertThat(result.getValue().get(),
            containsString("Property name2 is not reconfigurable"));
      } else if (change.prop.equals("name3")) {
        Assert.assertThat(result.getValue().get(), containsString("io exception"));
      } else {
        fail("Unknown property: " + change.prop);
      }
    }
  }

  @Test(timeout=30000)
  public void testStartReconfigurationFailureDueToExistingRunningTask()
      throws InterruptedException, IOException {
    AsyncReconfigurableDummy dummy = spy(new AsyncReconfigurableDummy(conf1));
    List<PropertyChange> changes = Lists.newArrayList(
        new PropertyChange(PROP1, "new1", "old1")
    );
    doReturn(changes).when(dummy).getChangedProperties(
        any(RaftProperties.class), any(RaftProperties.class));

    ReconfigurationTaskStatus status = dummy.getReconfigurationTaskStatus();
    Assert.assertFalse(status.hasTask());

    dummy.startReconfigurationTask();
    status = dummy.getReconfigurationTaskStatus();
    Assert.assertTrue(status.hasTask());
    Assert.assertFalse(status.stopped());

    // An active reconfiguration task is running.
    try {
      dummy.startReconfigurationTask();
      fail("Expect to throw IOException.");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Another reconfiguration task is running"));
    }
    status = dummy.getReconfigurationTaskStatus();
    Assert.assertTrue(status.hasTask());
    Assert.assertFalse(status.stopped());

    dummy.latch.countDown();
    waitAsyncReconfigureTaskFinish(dummy);
    status = dummy.getReconfigurationTaskStatus();
    Assert.assertTrue(status.hasTask());
    Assert.assertTrue(status.stopped());

    // The first task has finished.
    dummy.startReconfigurationTask();
    waitAsyncReconfigureTaskFinish(dummy);
    ReconfigurationTaskStatus status2 = dummy.getReconfigurationTaskStatus();
    Assert.assertTrue(status2.getStartTime() >= status.getEndTime());

    dummy.shutdownReconfigurationTask();
    try {
      dummy.startReconfigurationTask();
      fail("Expect to throw IOException");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("The server is stopped"));
    }
  }

  /**
   * Ensure that {@link ReconfigurationBase#reconfigureProperty} updates the
   * parent's cached configuration on success.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testConfIsUpdatedOnSuccess() throws ReconfigurationException {
    final String property = "FOO";
    final String value1 = "value1";
    final String value2 = "value2";

    final RaftProperties conf = new RaftProperties();
    conf.set(property, value1);
    final RaftProperties newConf = new RaftProperties();
    newConf.set(property, value2);

    final ReconfigurationBase reconfigurable = makeReconfigurable(
        conf, newConf, Arrays.asList(property));

    reconfigurable.reconfigureProperty(property, value2);
    Assert.assertEquals(value2, reconfigurable.getConf().get(property));
  }

  /**
   * Ensure that {@link ReconfigurationBase#startReconfigurationTask} updates
   * its parent's cached configuration on success.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testConfIsUpdatedOnSuccessAsync()
      throws InterruptedException, IOException, TimeoutException {
    final String property = "FOO";
    final String value1 = "value1";
    final String value2 = "value2";

    final RaftProperties conf = new RaftProperties();
    conf.set(property, value1);
    final RaftProperties newConf = new RaftProperties();
    newConf.set(property, value2);

    final ReconfigurationBase reconfigurable = makeReconfigurable(
        conf, newConf, Arrays.asList(property));

    // Kick off a reconfiguration task and wait until it completes.
    reconfigurable.startReconfigurationTask();

    RaftTestUtil.waitFor(() -> reconfigurable.getReconfigurationTaskStatus().stopped(), 100, 60000);
    Assert.assertEquals(value2, reconfigurable.getConf().get(property));
  }

  /**
   * Ensure that {@link ReconfigurationBase#reconfigureProperty} unsets the
   * property in its parent's configuration when the new value is null.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testConfIsUnset() throws ReconfigurationException {
    final String property = "FOO";
    final String value1 = "value1";

    final RaftProperties conf = new RaftProperties();
    conf.set(property, value1);
    final RaftProperties newConf = new RaftProperties();

    final ReconfigurationBase reconfigurable = makeReconfigurable(
        conf, newConf, Arrays.asList(property));

    reconfigurable.reconfigureProperty(property, null);
    Assert.assertNull(reconfigurable.getConf().get(property));
  }

  /**
   * Ensure that {@link ReconfigurationBase#startReconfigurationTask} unsets the
   * property in its parent's configuration when the new value is null.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testConfIsUnsetAsync() throws ReconfigurationException,
      IOException, TimeoutException, InterruptedException {
    final String property = "FOO";
    final String value1 = "value1";

    final RaftProperties conf = new RaftProperties();
    conf.set(property, value1);
    final RaftProperties newConf = new RaftProperties();

    final ReconfigurationBase reconfigurable = makeReconfigurable(
        conf, newConf, Arrays.asList(property));

    // Kick off a reconfiguration task and wait until it completes.
    reconfigurable.startReconfigurationTask();
    RaftTestUtil.waitFor(() -> reconfigurable.getReconfigurationTaskStatus().stopped(), 100, 60000);
    Assert.assertNull(reconfigurable.getConf().get(property));
  }

  private ReconfigurationBase makeReconfigurable(
      final RaftProperties oldConf, final RaftProperties newConf,
      final Collection<String> reconfigurableProperties) {
    final RaftProperties prop;

    return new ReconfigurationBase(oldConf) {
      @Override
      protected RaftProperties getNewConf() {
        return newConf;
      }

      @Override
      public Collection<String> getReconfigurableProperties() {
        return reconfigurableProperties;
      }

      @Override
      protected String reconfigurePropertyImpl(
          String property, String newVal) throws ReconfigurationException {
        return newVal;
      }
    };
  }
}
