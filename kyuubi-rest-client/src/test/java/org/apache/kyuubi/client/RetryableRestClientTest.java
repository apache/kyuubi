/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.client;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class RetryableRestClientTest {

  private RetryableRestClient getHandler(IRestClient proxy) {
    return (RetryableRestClient) Proxy.getInvocationHandler(proxy);
  }

  private String getCurrentUri(RetryableRestClient client) throws Exception {
    Field f = RetryableRestClient.class.getDeclaredField("currentUri");
    f.setAccessible(true);
    return (String) f.get(client);
  }

  private List<String> getUris(RetryableRestClient client) throws Exception {
    Field f = RetryableRestClient.class.getDeclaredField("uris");
    f.setAccessible(true);
    return (List<String>) f.get(client);
  }

  @Test
  public void testUpdateUrisKeepsCurrentServer() throws Exception {
    List<String> uris =
        Arrays.asList("http://host1:10099", "http://host2:10099", "http://host3:10099");
    RestClientConf conf = new RestClientConf();
    IRestClient proxy = RetryableRestClient.getRestClient(uris, conf);
    RetryableRestClient handler = getHandler(proxy);

    String currentUri = getCurrentUri(handler);
    assertTrue(uris.contains(currentUri));

    // update with a new list that still contains the current server
    List<String> newUris = Arrays.asList("http://host4:10099", currentUri, "http://host5:10099");
    handler.updateUris(newUris);

    assertEquals(currentUri, getCurrentUri(handler));
    assertEquals(newUris, getUris(handler));

    proxy.close();
  }

  @Test
  public void testUpdateUrisSwitchesWhenCurrentGone() throws Exception {
    List<String> uris = Arrays.asList("http://host1:10099", "http://host2:10099");
    RestClientConf conf = new RestClientConf();
    IRestClient proxy = RetryableRestClient.getRestClient(uris, conf);
    RetryableRestClient handler = getHandler(proxy);

    // Force current to host1
    // (we can't control random, so we just verify the invariant:
    //  after updateUris with list not containing current, current is in new list)
    String currentBefore = getCurrentUri(handler);

    List<String> newUris = Collections.singletonList("http://host3:10099");
    handler.updateUris(newUris);

    String currentAfter = getCurrentUri(handler);
    assertEquals("http://host3:10099", currentAfter);
    assertNotEquals(currentBefore, currentAfter);

    proxy.close();
  }

  @Test
  public void testUpdateUrisWithEmptyRemoval() throws Exception {
    List<String> uris = Arrays.asList("http://host1:10099", "http://host2:10099");
    RestClientConf conf = new RestClientConf();
    IRestClient proxy = RetryableRestClient.getRestClient(uris, conf);
    RetryableRestClient handler = getHandler(proxy);

    // Remove the current server from the list
    String current = getCurrentUri(handler);
    String other =
        current.equals("http://host1:10099") ? "http://host2:10099" : "http://host1:10099";

    handler.updateUris(Collections.singletonList(other));
    assertEquals(other, getCurrentUri(handler));

    proxy.close();
  }

  @Test
  public void testRotateToNextUri() throws Exception {
    List<String> uris =
        Arrays.asList("http://host1:10099", "http://host2:10099", "http://host3:10099");
    RestClientConf conf = new RestClientConf();
    IRestClient proxy = RetryableRestClient.getRestClient(uris, conf);
    RetryableRestClient handler = getHandler(proxy);

    String current = getCurrentUri(handler);
    int idx = uris.indexOf(current);
    String expected = uris.get((idx + 1) % uris.size());

    // Use reflection to call rotateToNextUri
    java.lang.reflect.Method rotateMethod =
        RetryableRestClient.class.getDeclaredMethod("rotateToNextUri");
    rotateMethod.setAccessible(true);
    rotateMethod.invoke(handler);

    assertEquals(expected, getCurrentUri(handler));

    proxy.close();
  }
}
