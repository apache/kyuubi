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

package org.apache.kyuubi.service.authentication.ldap

import javax.naming.{AuthenticationException => JndiAuthenticationException, NamingException}
import javax.security.sasl.AuthenticationException

import com.unboundid.ldap.sdk.{LDAPException, ResultCode}

/**
 * Classifies an LDAP authentication failure into one of three operational categories so
 * that dashboards and alerts can distinguish credential rejection from misconfiguration
 * from infrastructure outage.
 *
 * Categories:
 *  - `invalid_credentials`: the LDAP server explicitly rejected the bind with
 *    [[ResultCode.INVALID_CREDENTIALS]] (LDAP error 49). Expected and high-baseline.
 *  - `invalid_input`: the request itself was malformed -- blank user/password, or the
 *    user format produced no candidate principals against the configured DN patterns.
 *    Almost always client-side; baseline near zero in healthy production.
 *  - `infrastructure`: anything else -- connection failures, timeouts, search errors,
 *    server-internal errors. Baseline zero; spikes warrant paging.
 *
 * Classification first inspects the cause chain (most reliable) and falls back to message
 * prefixes for the cases where our own code threw [[AuthenticationException]] with no
 * cause attached (notably the blank-input checks in [[LdapAuthenticationProviderImpl]]).
 */
// Visibility is `private[kyuubi]` rather than `private[authentication]` because the
// classification strings are used as metric-name suffixes by kyuubi-server (see
// org.apache.kyuubi.server.KyuubiServer.installLdapAuthMetricsRecorder).
private[kyuubi] object LdapAuthFailureClassifier {

  val INVALID_CREDENTIALS: String = "invalid_credentials"
  val INVALID_INPUT: String = "invalid_input"
  val INFRASTRUCTURE: String = "infrastructure"

  // Message prefixes thrown by our own code for the invalid_input category. Kept here
  // (not duplicated from the throw sites) so a future change to those messages will need
  // to update this list -- the test suite asserts the mapping holds.
  private val InvalidInputPrefixes: Seq[String] = Seq(
    "Error validating LDAP user, user is null",
    "Error validating LDAP user, password is null",
    "Error validating LDAP user, invalid filter",
    "No candidate principals for")

  def classify(t: Throwable): String = t match {
    case ae: AuthenticationException =>
      classifyByCause(ae.getCause).getOrElse(classifyByMessage(ae.getMessage))
    case _: NamingException => INFRASTRUCTURE
    case _ => INFRASTRUCTURE
  }

  private def classifyByCause(cause: Throwable): Option[String] = cause match {
    case le: LDAPException if le.getResultCode == ResultCode.INVALID_CREDENTIALS =>
      Some(INVALID_CREDENTIALS)
    case _: LDAPException => Some(INFRASTRUCTURE)
    // Legacy JNDI path: a failed bind surfaces as javax.naming.AuthenticationException,
    // the JNDI equivalent of LDAP result code 49. It is a NamingException subtype, so this
    // case must precede the generic NamingException case below; otherwise wrong-password
    // attempts on the pool-disabled path would be bucketed as `infrastructure` and page
    // operators as an outage.
    case _: JndiAuthenticationException => Some(INVALID_CREDENTIALS)
    case _: NamingException => Some(INFRASTRUCTURE)
    case _ => None
  }

  private def classifyByMessage(message: String): String = {
    val msg = Option(message).getOrElse("")
    if (InvalidInputPrefixes.exists(msg.startsWith)) INVALID_INPUT
    else INFRASTRUCTURE
  }
}
