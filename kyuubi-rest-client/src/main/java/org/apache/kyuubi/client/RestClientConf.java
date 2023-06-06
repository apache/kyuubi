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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class RestClientConf {
  private int maxAttempts;
  private int attemptWaitTime;
  private int socketTimeout;
  private int connectTimeout;

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public void setMaxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
  }

  public int getAttemptWaitTime() {
    return attemptWaitTime;
  }

  public void setAttemptWaitTime(int attemptWaitTime) {
    this.attemptWaitTime = attemptWaitTime;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public void setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  /**
   * [for CodeQL testing only] Deserialize the object from the input stream.
   *
   * @param is the input stream
   * @return the object
   * @throws IOException if an I/O error occurs while reading stream header
   * @throws ClassNotFoundException Class of a serialized object cannot be found.
   */
  public static Object unSafeDeserialize(InputStream is)
      throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(is);
    return ois.readObject();
  }
}
