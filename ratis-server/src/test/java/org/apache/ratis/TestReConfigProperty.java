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

import org.apache.ratis.client.impl.OrderedAsync;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.conf.ReconfigurationBase;
import org.apache.ratis.conf.ReconfigurationException;
import org.apache.ratis.conf.ReconfigurationStatus.PropertyChange;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.util.Slf4jUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

public abstract class TestReConfigProperty<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    Slf4jUtils.setLogLevel(OrderedAsync.LOG, Level.DEBUG);
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
  private static final String DEFAULT = "default";

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
    Collection<PropertyChange> changes
        = ReconfigurationBase.getChangedProperties(conf2, conf1);

    Assert.assertTrue("expected 3 changed properties but got " + changes.size(),
        changes.size() == 3);

    boolean changeFound = false;
    boolean unsetFound = false;
    boolean setFound = false;

    for (PropertyChange c: changes) {
      if (c.getProperty().equals(PROP2) && c.getOldValue() != null && c.getOldValue().equals(VAL1) &&
          c.getNewValue() != null && c.getNewValue().equals(VAL2)) {
        changeFound = true;
      } else if (c.getProperty().equals(PROP3) && c.getOldValue() != null && c.getOldValue().equals(VAL1) &&
          c.getNewValue() == null) {
        unsetFound = true;
      } else if (c.getProperty().equals(PROP4) && c.getOldValue() == null &&
          c.getNewValue() != null && c.getNewValue().equals(VAL1)) {
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
    private RaftProperties newProp;

    public ReconfigurableDummy(RaftProperties prop) {
      super("reConfigDummy", prop);
    }

    @Override
    protected RaftProperties getNewProperties() {
      return newProp;
    }

    @Override
    public synchronized String reconfigureProperty(String property, String newValue)
        throws ReconfigurationException {
      newProp = new RaftProperties();
      newProp.set(property, newValue != null ? newValue : DEFAULT);
      return newValue;
    }

    @Override
    public Collection<String> getReconfigurableProperties() {
      return Arrays.asList(PROP1, PROP2, PROP4);
    }

    /**
     * Run until PROP1 is no longer VAL1.
     */
    @Override
    public void run() {
      while (running && getProperties().get(PROP1).equals(VAL1)) {
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

    Assert.assertEquals(PROP1 + " set to wrong value ", VAL1, dummy.getProperties().get(PROP1));
    Assert.assertEquals(PROP2 + " set to wrong value ", VAL1, dummy.getProperties().get(PROP2));
    Assert.assertEquals(PROP3 + " set to wrong value ", VAL1, dummy.getProperties().get(PROP3));
    Assert.assertNull(PROP4 + " set to wrong value ", dummy.getProperties().get(PROP4));
    Assert.assertNull(PROP5 + " set to wrong value ", dummy.getProperties().get(PROP5));

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
        dummy.startReconfiguration();
        RaftTestUtil.waitFor(() -> dummy.getReconfigurationStatus().ended(), 100, 60000);
        Assert.assertEquals(PROP1 + " set to wrong value ", VAL1, dummy.getProperties().get(PROP1));
      } catch (ReconfigurationException | IOException | TimeoutException | InterruptedException e) {
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
        dummy.startReconfiguration();
        RaftTestUtil.waitFor(() -> dummy.getReconfigurationStatus().ended(), 100, 60000);
        Assert.assertEquals(PROP1 + "set to wrong value ", DEFAULT,
            dummy.getProperties().get(PROP1));
      } catch (ReconfigurationException | IOException | InterruptedException | TimeoutException e) {
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
        dummy.startReconfiguration();
        RaftTestUtil.waitFor(() -> dummy.getReconfigurationStatus().ended(), 100, 60000);
        Assert.assertEquals(PROP1 + "set to wrong value ", VAL2, dummy.getProperties().get(PROP1));
      } catch (ReconfigurationException | IOException | InterruptedException | TimeoutException e) {
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
        dummy.startReconfiguration();
        RaftTestUtil.waitFor(() -> dummy.getReconfigurationStatus().ended(), 100, 60000);
        Assert.assertSame(PROP4 + "set to wrong value ", DEFAULT, dummy.getProperties().get(PROP4));
      } catch (ReconfigurationException | IOException | InterruptedException | TimeoutException e) {
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
        dummy.startReconfiguration();
        RaftTestUtil.waitFor(() -> dummy.getReconfigurationStatus().ended(), 100, 60000);
        Assert.assertEquals(PROP4 + "set to wrong value ", VAL1, dummy.getProperties().get(PROP4));
      } catch (ReconfigurationException | IOException | InterruptedException | TimeoutException e) {
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
        dummy.startReconfiguration();
        RaftTestUtil.waitFor(() -> dummy.getReconfigurationStatus().ended(), 100, 60000);
      } catch (ReconfigurationException | IOException | InterruptedException | TimeoutException e) {
        exceptionCaught = true;
      }
      Assert.assertTrue("did not receive expected exception",
          dummy.getReconfigurationStatus().getChanges()
              .get(new PropertyChange(PROP5, DEFAULT, null))
              .getMessage().contains("Property is not reconfigurable.") && !exceptionCaught);
    }

    // try to set unset property to value (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP5, VAL1);
        dummy.startReconfiguration();
        RaftTestUtil.waitFor(() -> dummy.getReconfigurationStatus().ended(), 100, 60000);
      } catch (ReconfigurationException | IOException | InterruptedException | TimeoutException e) {
        exceptionCaught = true;
      }
      Assert.assertTrue("did not receive expected exception",
          dummy.getReconfigurationStatus().getChanges()
              .get(new PropertyChange(PROP5, VAL1, null))
              .getMessage().contains("Property is not reconfigurable.") && !exceptionCaught);
    }

    // try to change property to value (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP3, VAL2);
        dummy.startReconfiguration();
        RaftTestUtil.waitFor(() -> dummy.getReconfigurationStatus().ended(), 100, 60000);
      } catch (ReconfigurationException | IOException | InterruptedException | TimeoutException e) {
        exceptionCaught = true;
      }
      Assert.assertTrue("did not receive expected exception",
          dummy.getReconfigurationStatus().getChanges()
              .get(new PropertyChange(PROP3, VAL2, VAL1))
              .getMessage().contains("Property is not reconfigurable.") && !exceptionCaught);
    }

    // try to change property to null (not reconfigurable)
    {
      boolean exceptionCaught = false;
      try {
        dummy.reconfigureProperty(PROP3, null);
        dummy.startReconfiguration();
        RaftTestUtil.waitFor(() -> dummy.getReconfigurationStatus().ended(), 100, 60000);
      } catch (ReconfigurationException | IOException | InterruptedException | TimeoutException e) {
        exceptionCaught = true;
      }
      Assert.assertTrue("did not receive expected exception",
          dummy.getReconfigurationStatus().getChanges()
              .get(new PropertyChange(PROP3, DEFAULT, VAL1))
              .getMessage().contains("Property is not reconfigurable.") && !exceptionCaught);
    }
  }

  /**
   * Test whether configuration changes are visible in another thread.
   */
  @Test
  public void testThread() throws ReconfigurationException, IOException {
    ReconfigurableDummy dummy = new ReconfigurableDummy(conf1);
    Assert.assertEquals(VAL1, dummy.getProperties().get(PROP1));
    Thread dummyThread = new Thread(dummy);
    dummyThread.start();
    try {
      Thread.sleep(500);
    } catch (InterruptedException ignore) {
      // do nothing
    }
    dummy.reconfigureProperty(PROP1, VAL2);
    dummy.startReconfiguration();

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
        dummy.getProperties().get(PROP1).equals(VAL2));

  }

  /**
   * Ensure that {@link ReconfigurationBase#reconfigureProperty} updates the
   * parent's cached configuration on success.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testConfIsUpdatedOnSuccess()
      throws ReconfigurationException, IOException, InterruptedException, TimeoutException {
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
    reconfigurable.startReconfiguration();
    RaftTestUtil.waitFor(() -> reconfigurable.getReconfigurationStatus().ended(), 100, 60000);
    Assert.assertEquals(value2, reconfigurable.getProperties().get(property));
  }

  /**
   * Ensure that {@link ReconfigurationBase#startReconfiguration} updates
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
    reconfigurable.startReconfiguration();

    RaftTestUtil.waitFor(() -> reconfigurable.getReconfigurationStatus().ended(), 100, 60000);
    Assert.assertEquals(value2, reconfigurable.getProperties().get(property));
  }

  /**
   * Ensure that {@link ReconfigurationBase#reconfigureProperty} unsets the
   * property in its parent's configuration when the new value is null.
   * @throws IOException
   */
  @Test (timeout=300000)
  public void testConfIsUnset()
      throws InterruptedException, TimeoutException, IOException {
    final String property = "FOO";
    final String value1 = "value1";

    final RaftProperties conf = new RaftProperties();
    conf.set(property, value1);
    final RaftProperties newConf = new RaftProperties();

    final ReconfigurationBase reconfigurable = makeReconfigurable(
        conf, newConf, Arrays.asList(property));

    reconfigurable.startReconfiguration();
    RaftTestUtil.waitFor(() -> reconfigurable.getReconfigurationStatus().ended(), 100, 60000);
    Assert.assertNull(reconfigurable.getProperties().get(property));
  }

  /**
   * Ensure that {@link ReconfigurationBase#startReconfiguration} unsets the
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
    reconfigurable.startReconfiguration();
    RaftTestUtil.waitFor(() -> reconfigurable.getReconfigurationStatus().ended(), 100, 60000);
    Assert.assertNull(reconfigurable.getProperties().get(property));
  }

  private ReconfigurationBase makeReconfigurable(
      final RaftProperties oldProperties, final RaftProperties newProperties,
      final Collection<String> reconfigurableProperties) {

    return new ReconfigurationBase("tempReConfigDummy", oldProperties) {
      @Override
      protected RaftProperties getNewProperties() {
        return newProperties;
      }

      @Override
      public String reconfigureProperty(String property, String newValue) {
        return newValue;
      }

      @Override
      public Collection<String> getReconfigurableProperties() {
        return reconfigurableProperties;
      }
    };
  }
}
