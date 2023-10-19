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

package org.apache.kyuubi.client.auth;

import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import javax.security.auth.Subject;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.apache.kyuubi.util.reflect.DynFields;
import org.apache.kyuubi.util.reflect.DynMethods;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpnegoAuthHeaderGenerator implements AuthHeaderGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(SpnegoAuthHeaderGenerator.class);
  private static final String UGI_CLASS = "org.apache.hadoop.security.UserGroupInformation";
  private final String spnegoHost;

  public SpnegoAuthHeaderGenerator(String spnegoHost) {
    this.spnegoHost = spnegoHost;
  }

  @Override
  public String generateAuthHeader() {
    try {
      return "NEGOTIATE " + generateToken(spnegoHost);
    } catch (KyuubiRestException rethrow) {
      throw rethrow;
    } catch (Exception e) {
      throw new KyuubiRestException("Failed to generate spnego auth header for " + spnegoHost, e);
    }
  }

  /**
   * Generate SPNEGO challenge request token. Copied from Apache Hadoop
   * YarnClientUtils::generateToken
   *
   * @param server - hostname to contact
   * @throws Exception
   */
  private String generateToken(String server) throws Exception {
    Subject subject;
    try {
      Object ugiCurrentUser =
          DynMethods.builder("getCurrentUser")
              .hiddenImpl(Class.forName(UGI_CLASS))
              .buildStaticChecked()
              .invoke();
      LOG.debug("The user credential is {}", ugiCurrentUser);
      subject =
          DynFields.builder()
              .hiddenImpl(ugiCurrentUser.getClass(), "subject")
              .<Subject>buildChecked(ugiCurrentUser)
              .get();
    } catch (ClassNotFoundException e) {
      // TODO do kerberos authentication using JDK class directly
      LOG.error("Hadoop UGI class {} is required for SPNEGO authentication.", UGI_CLASS);
      throw e;
    }
    return Subject.doAs(subject, (PrivilegedExceptionAction<String>) () -> doGenerateToken(server));
  }

  private String doGenerateToken(String server) throws GSSException {
    GSSManager manager = GSSManager.getInstance();
    // GSS name for server
    GSSName serverName = manager.createName("HTTP@" + server, GSSName.NT_HOSTBASED_SERVICE);
    // Create a GSSContext for authentication with the service.
    // We're passing client credentials as null since we want them to
    // be read from the Subject.
    // We're passing Oid as null to use the default.
    GSSName peer = serverName.canonicalize(null);
    GSSContext gssContext = manager.createContext(peer, null, null, GSSContext.DEFAULT_LIFETIME);
    gssContext.requestMutualAuth(true);
    gssContext.requestCredDeleg(true);
    // Establish context
    byte[] inToken = new byte[0];
    byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.length);
    gssContext.dispose();
    // Base64 encoded and stringified token for server
    LOG.debug("Got valid challenge for host {}", serverName);
    return new String(Base64.getEncoder().encode(outToken), StandardCharsets.US_ASCII);
  }
}
