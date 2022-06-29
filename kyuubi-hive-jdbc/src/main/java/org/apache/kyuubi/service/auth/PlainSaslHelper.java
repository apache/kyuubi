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

package org.apache.kyuubi.service.auth;

import java.io.IOException;
import java.security.Security;
import java.util.HashMap;
import javax.security.auth.callback.*;
import javax.security.sasl.SaslException;
import org.apache.kyuubi.service.auth.PlainSaslServer.SaslPlainProvider;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;

public final class PlainSaslHelper {

  // Register Plain SASL server provider
  static {
    Security.addProvider(new SaslPlainProvider());
  }

  public static TTransport getPlainTransport(
      String username, String password, TTransport underlyingTransport) throws SaslException {
    return new TSaslClientTransport(
        "PLAIN",
        null,
        null,
        null,
        new HashMap<String, String>(),
        new PlainCallbackHandler(username, password),
        underlyingTransport);
  }

  private PlainSaslHelper() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  public static class PlainCallbackHandler implements CallbackHandler {

    private final String username;
    private final String password;

    public PlainCallbackHandler(String username, String password) {
      this.username = username;
      this.password = password;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nameCallback = (NameCallback) callback;
          nameCallback.setName(username);
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback passCallback = (PasswordCallback) callback;
          passCallback.setPassword(password.toCharArray());
        } else {
          throw new UnsupportedCallbackException(callback);
        }
      }
    }
  }
}
