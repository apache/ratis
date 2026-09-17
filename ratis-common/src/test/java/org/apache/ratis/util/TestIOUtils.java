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

import org.apache.ratis.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.beans.EventHandler;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.URL;
import java.rmi.server.RemoteObject;
import java.rmi.server.RemoteObjectInvocationHandler;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import javax.management.BadAttributeValueExpException;
import javax.naming.InitialContext;
import javax.xml.transform.Templates;

/** Test methods of {@link IOUtils}. */
public class TestIOUtils extends BaseTest {

  @Test
  public void testReadObjectAllowsThrowable() {
    final IOException original = new IOException("outer", new RuntimeException("inner"));
    final byte[] bytes = IOUtils.object2Bytes(original);
    final Throwable roundTrip = IOUtils.bytes2Object(bytes, Throwable.class);
    Assertions.assertEquals(IOException.class, roundTrip.getClass());
    Assertions.assertEquals("outer", roundTrip.getMessage());
    Assertions.assertNotNull(roundTrip.getCause());
    Assertions.assertEquals("inner", roundTrip.getCause().getMessage());
  }

  @Test
  public void testReadObjectAllowsStackTraceElements() {
    final StackTraceElement[] original = new Throwable().getStackTrace();
    final byte[] bytes = IOUtils.object2Bytes(original);
    final StackTraceElement[] roundTrip = IOUtils.bytes2Object(bytes, StackTraceElement[].class);
    Assertions.assertArrayEquals(original, roundTrip);
  }

  @Test
  public void testAllowedList() throws Exception {
    // BigDecimal is in java.math which is in neither the allow-list nor the disallow-list
    assertDisallowed(new BigDecimal("3.14"), false);
  }

  /**
   * Assert some of the JRE classes are in the disallow-list
   * since these classes are subjected to Java deserialization vulnerabilities.
   * When an object cannot be easily created or the class does not exist in all JRE implementations,
   * test the full class name.
   */
  @Test
  public void testDisallowedList() throws Exception {
    // Kick-off & Entry Points (Sources)
    assertDisallowed(new HashMap<>());
    assertDisallowed(new HashSet<>());
    assertDisallowed(new LinkedHashSet<>());
    assertDisallowed(new PriorityQueue<>());
    assertDisallowed(new BadAttributeValueExpException(null));
    assertDisallowed(new EventHandler("object", "test", "event", "listener"));
    assertDisallowed(new ProcessBuilder());
    assertDisallowed(Runtime.getRuntime());
    assertIsDisallowedForObjectInputStream("java.security.SignedObject"); //hard to create
    assertIsDisallowedForObjectInputStream("sun.reflect.annotation.AnnotationInvocationHandler"); //package private

    // Intermediate & Proxy Gadgets
    assertDisallowed(new TreeMap<>());
    assertDisallowed(new TreeSet<>());
    assertDisallowed(Proxy.newProxyInstance(
        Proxy.class.getClassLoader(),
        new Class<?>[] { Consumer.class },
        (proxy, method, args) -> null
    ));

    // Execution & Class Loading Sinks
    assertIsDisallowedForObjectInputStream(Templates.class.getName()); //interface
    assertIsDisallowedForObjectInputStream("com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl"); //not exist
    assertIsDisallowedForObjectInputStream("sun.rmi.server.MarshalInputStream"); //class not exist

    // JNDI & Remote Triggers
    assertDisallowed(new InitialContext());
    assertIsDisallowedForObjectInputStream(RemoteObject.class.getName()); //hard to create or MarshalException
    assertIsDisallowedForObjectInputStream(RemoteObjectInvocationHandler.class.getName()); //hard to create
    assertIsDisallowedForObjectInputStream("java.rmi.server.UnicastRef"); //class not exist
    assertIsDisallowedForObjectInputStream("com.sun.rowset.JdbcRowSetImpl"); //class not exist

    // Network Reconnaissance & OOB Triggers
    assertDisallowed(new URL("http://localhost"));
  }

  static void assertDisallowed(Object object) throws Exception {
    assertDisallowed(object, true);
  }

  static void assertDisallowed(Object object, boolean isInDisallowedList) throws Exception {
    final Class<?> clazz = object.getClass();
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      oout.writeObject(object);
    } catch (NotSerializableException e) {
      return;
    }

    if (isInDisallowedList) {
      assertIsDisallowedForObjectInputStream(clazz.getName());
    }
    final byte[] bytes = bout.toByteArray();
    final IllegalStateException ise = Assertions.assertThrows(IllegalStateException.class,
        () -> IOUtils.bytes2Object(bytes, clazz));
    // The cause should be an InvalidClassException raised by the filter.
    Throwable cause = ise.getCause();
    Assertions.assertNotNull(cause, "expected a cause on " + ise);
    Assertions.assertEquals(InvalidClassException.class, cause.getClass(),
        "unexpected cause: " + cause);
  }

  static void assertIsDisallowedForObjectInputStream(String classname) {
    Assertions.assertTrue(IOUtils.isDisallowedForObjectInputStream(classname), classname);
  }
}
