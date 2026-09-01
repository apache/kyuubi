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

package org.apache.kyuubi.server.flight

import java.io.File
import java.security.{PrivilegedActionException, PrivilegedExceptionAction}
import java.util.Base64
import javax.security.auth.Subject
import javax.security.auth.kerberos.{KerberosPrincipal, KeyTab}
import javax.security.sasl.AuthenticationException

import org.apache.hadoop.security.authentication.util.KerberosName
import org.apache.hadoop.security.authentication.util.KerberosUtil._
import org.ietf.jgss.{GSSContext, GSSCredential, GSSManager, Oid}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{SERVER_SPNEGO_KEYTAB, SERVER_SPNEGO_PRINCIPAL}
import org.apache.kyuubi.util.KyuubiHadoopUtils

/**
 * Validates SPNEGO tokens for the Arrow Flight SQL gRPC frontend.
 * Reuses the same GSS-API pattern as KerberosAuthenticationHandler adapted for Flight.
 */
class FlightSqlKerberosValidator(conf: KyuubiConf) extends Logging {

  private val keytab = conf.get(SERVER_SPNEGO_KEYTAB).get
  private val principal = KyuubiHadoopUtils.getServerPrincipal(
    conf.get(SERVER_SPNEGO_PRINCIPAL).get)

  private val serverSubject: Subject = {
    val subject = new Subject()
    subject.getPrivateCredentials.add(KeyTab.getInstance(new File(keytab)))
    subject.getPrincipals.add(new KerberosPrincipal(principal))
    subject
  }

  private val gssManager: GSSManager = Subject.doAs(
    serverSubject,
    new PrivilegedExceptionAction[GSSManager] {
      override def run(): GSSManager = GSSManager.getInstance()
    })

  if (!KerberosName.hasRulesBeenSet) {
    KerberosName.setRules("DEFAULT")
  }

  info(s"FlightSqlKerberosValidator initialized with principal $principal, keytab $keytab")

  /**
   * Validates a SPNEGO token from the Authorization: Negotiate header.
   *
   * @param base64Token the base64-encoded SPNEGO token (Negotiate prefix already stripped)
   * @return the authenticated short username
   */
  def validate(base64Token: String): String = {
    val clientToken = Base64.getDecoder.decode(base64Token)
    try {
      Subject.doAs(
        serverSubject,
        new PrivilegedExceptionAction[String] {
          override def run(): String = validateToken(clientToken)
        })
    } catch {
      case e: PrivilegedActionException =>
        throw new AuthenticationException("SPNEGO authentication failed", e.getException)
      case e: Exception =>
        throw new AuthenticationException("SPNEGO authentication failed", e)
    }
  }

  private def validateToken(clientToken: Array[Byte]): String = {
    val serverPrincipalName = getTokenServerName(clientToken)
    var gssContext: GSSContext = null
    var gssCreds: GSSCredential = null
    try {
      gssCreds = gssManager.createCredential(
        gssManager.createName(serverPrincipalName, NT_GSS_KRB5_PRINCIPAL_OID),
        GSSCredential.INDEFINITE_LIFETIME,
        Array[Oid](GSS_SPNEGO_MECH_OID, GSS_KRB5_MECH_OID),
        GSSCredential.ACCEPT_ONLY)
      gssContext = gssManager.createContext(gssCreds)
      gssContext.acceptSecContext(clientToken, 0, clientToken.length)
      if (!gssContext.isEstablished) {
        throw new AuthenticationException("SPNEGO context wasn't fully established")
      }
      val clientPrincipal = gssContext.getSrcName.toString
      new KerberosName(clientPrincipal).getShortName
    } finally {
      if (gssContext != null) gssContext.dispose()
      if (gssCreds != null) gssCreds.dispose()
    }
  }
}
