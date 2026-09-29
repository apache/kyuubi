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

import javax.naming.{AuthenticationException => JndiAuthenticationException, CommunicationException, NamingException}
import javax.security.sasl.AuthenticationException

import com.unboundid.ldap.sdk.{LDAPException, ResultCode}

import org.apache.kyuubi.KyuubiFunSuite

class LdapAuthFailureClassifierSuite extends KyuubiFunSuite {

  import LdapAuthFailureClassifier._

  test("INVALID_CREDENTIALS via cause chain -> invalid_credentials") {
    val ldapEx = new LDAPException(ResultCode.INVALID_CREDENTIALS, "bad password")
    val authEx = new AuthenticationException("Error validating LDAP user: alice", ldapEx)
    assert(classify(authEx) === INVALID_CREDENTIALS)
  }

  test("non-INVALID_CREDENTIALS LDAPException cause -> infrastructure") {
    val ldapEx = new LDAPException(ResultCode.SERVER_DOWN, "no route")
    val authEx = new AuthenticationException("LDAP bind failed", ldapEx)
    assert(classify(authEx) === INFRASTRUCTURE)
  }

  test("blank user message -> invalid_input") {
    val ex = new AuthenticationException(
      "Error validating LDAP user, user is null or contains blank space")
    assert(classify(ex) === INVALID_INPUT)
  }

  test("blank password message -> invalid_input") {
    val ex = new AuthenticationException(
      "Error validating LDAP user, password is null or contains blank space")
    assert(classify(ex) === INVALID_INPUT)
  }

  test("no candidate principals message -> invalid_input") {
    val ex = new AuthenticationException("No candidate principals for alice was found.")
    assert(classify(ex) === INVALID_INPUT)
  }

  test("Unable to find the user message (no cause) -> infrastructure") {
    // This is the message LdapAuthenticationProviderImpl emits when it catches a
    // NamingException; the cause is intentionally not chained in production code.
    val ex = new AuthenticationException(
      "Unable to find the user in the LDAP tree. LDAP connect failed: timeout")
    assert(classify(ex) === INFRASTRUCTURE)
  }

  test("AuthenticationException with null message and null cause -> infrastructure") {
    val ex = new AuthenticationException()
    assert(classify(ex) === INFRASTRUCTURE)
  }

  test("plain NamingException -> infrastructure") {
    assert(classify(new NamingException("bad query")) === INFRASTRUCTURE)
  }

  test("CommunicationException (NamingException subtype) -> infrastructure") {
    assert(classify(new CommunicationException("connection refused")) === INFRASTRUCTURE)
  }

  test("RuntimeException -> infrastructure") {
    assert(classify(new RuntimeException("bug")) === INFRASTRUCTURE)
  }

  test("javax.naming.AuthenticationException cause -> invalid_credentials (JNDI path)") {
    // Legacy JNDI path: LdapSearchFactory wraps a failed bind (wrong password) as a
    // sasl.AuthenticationException whose cause is a javax.naming.AuthenticationException.
    // This must classify as invalid_credentials, not infrastructure.
    val jndiEx = new JndiAuthenticationException("[LDAP: error code 49 - Invalid Credentials]")
    val authEx = new AuthenticationException("Error validating LDAP user: alice", jndiEx)
    assert(classify(authEx) === INVALID_CREDENTIALS)
  }

  test("NamingException cause inside AuthenticationException -> infrastructure") {
    val authEx = new AuthenticationException("LDAP bind failed", new NamingException("boom"))
    assert(classify(authEx) === INFRASTRUCTURE)
  }
}
