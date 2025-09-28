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

package org.apache.kyuubi.jdbc.hive.auth;

import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import javax.security.auth.Subject;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/** Utility functions for HTTP mode authentication. */
public final class HttpAuthUtils {
  public static final String AUTHORIZATION = "Authorization";
  public static final String NEGOTIATE = "Negotiate";
  public static final String BEARER = "Bearer";

  /**
   * @return Stringified Base64 encoded kerberosAuthHeader on success
   */
  public static String getKerberosServiceTicket(
      String serverPrinciple, String host, Subject loggedInSubject) throws Exception {
    String spn = KerberosUtils.canonicalPrincipal(serverPrinciple, host);
    return Subject.doAs(loggedInSubject, new HttpKerberosClientAction(spn));
  }

  private HttpAuthUtils() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  /**
   * We'll create an instance of this class within a doAs block so that the client's TGT credentials
   * can be read from the Subject
   */
  public static class HttpKerberosClientAction implements PrivilegedExceptionAction<String> {
    private final String serverPrincipal;

    public HttpKerberosClientAction(String serverPrincipal) {
      this.serverPrincipal = serverPrincipal;
    }

    @Override
    public String run() throws Exception {
      // This Oid for Kerberos GSS-API mechanism.
      Oid mechOid = new Oid("1.2.840.113554.1.2.2");
      // Oid for kerberos principal name
      Oid krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1");
      GSSManager manager = GSSManager.getInstance();
      // GSS name for server
      GSSName serverName = manager.createName(serverPrincipal, krb5PrincipalOid);
      // Create a GSSContext for authentication with the service.
      // We're passing client credentials as null since we want them to be read from the Subject.
      GSSContext gssContext =
          manager.createContext(serverName, mechOid, null, GSSContext.DEFAULT_LIFETIME);
      gssContext.requestMutualAuth(false);
      // Establish context
      byte[] inToken = new byte[0];
      byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.length);
      gssContext.dispose();
      // Base64 encoded and stringified token for server
      return Base64.getEncoder().encodeToString(outToken);
    }
  }
}
