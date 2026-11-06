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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.kyuubi.client.exception.RetryableKyuubiRestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A retryable rest client that catches the {@link RetryableKyuubiRestException} which is thrown by
 * underlying rest client and use a new server uri to the next attempt.
 */
public class RetryableRestClient implements InvocationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RetryableRestClient.class);

  private final RestClientConf conf;
  private final List<String> uris;
  private int currentUriIndex;
  private volatile IRestClient restClient;

  private RetryableRestClient(List<String> uris, RestClientConf conf) {
    this.conf = conf;
    this.uris = uris;
    this.currentUriIndex = ThreadLocalRandom.current().nextInt(uris.size());
    newRestClient();
  }

  @SuppressWarnings("rawtypes")
  public static IRestClient getRestClient(List<String> uris, RestClientConf conf) {
    RetryableRestClient client = new RetryableRestClient(uris, conf);
    return (IRestClient)
        Proxy.newProxyInstance(
            Thread.currentThread().getContextClassLoader(),
            new Class[] {IRestClient.class},
            client);
  }

  private void newRestClient() {
    if (restClient != null) {
      try {
        restClient.close();
        restClient = null;
      } catch (Exception e) {
        LOG.warn("Failed to close rest client", e);
      }
    }

    CloseableHttpClient httpclient = HttpClientFactory.createHttpClient(conf);
    assert currentUriIndex < uris.size();
    this.restClient = new RestClient(uris.get(currentUriIndex), httpclient);
    LOG.info("Current connect server uri {}", uris.get(currentUriIndex));
  }

  @Override
  public synchronized Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    int retryTimes = 0;
    while (true) {
      try {
        return method.invoke(restClient, args);
      } catch (IllegalAccessException ignored) {
      } catch (InvocationTargetException e) {
        if (e.getCause() == null) {
          throw e;
        } else if (e.getCause() instanceof RetryableKyuubiRestException) {
          // the remote server has some issues or the client machine has some issues
          retryTimes++;
          if (retryTimes <= conf.getMaxAttempts()) {
            Thread.sleep(conf.getAttemptWaitTime());
          } else {
            LOG.error("Attempt over {} times", conf.getMaxAttempts(), e.getCause());
            throw e.getCause();
          }
          if (currentUriIndex == uris.size() - 1) {
            currentUriIndex = 0;
          } else {
            currentUriIndex++;
          }
          newRestClient();
        } else {
          throw e.getCause();
        }
      }
    }
  }
}
