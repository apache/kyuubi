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

import java.util.Map;
import javax.security.auth.Subject;
import javax.security.sasl.SaslException;
import org.apache.kyuubi.shaded.thrift.transport.TSaslClientTransport;
import org.apache.kyuubi.shaded.thrift.transport.TTransport;
import org.apache.kyuubi.shaded.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KerberosSaslHelper {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosSaslHelper.class);

  public static TTransport createSubjectAssumedTransport(
      Subject subject,
      String serverPrincipal,
      String host,
      TTransport underlyingTransport,
      Map<String, String> saslProps)
      throws SaslException, TTransportException {
    String resolvedPrincipal = KerberosUtils.canonicalPrincipal(serverPrincipal, host);
    String[] names = KerberosUtils.splitPrincipal(resolvedPrincipal);
    TTransport saslTransport =
        new TSaslClientTransport(
            "GSSAPI", null, names[0], names[1], saslProps, null, underlyingTransport);
    return new TSubjectTransport(saslTransport, subject);
  }

  private KerberosSaslHelper() {
    throw new UnsupportedOperationException("Can't initialize class");
  }
}
