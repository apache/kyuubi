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

import org.apache.kyuubi.KyuubiFunSuite

/**
 * Tests for [[LdapUrlParser]], which delegates to [[com.unboundid.ldap.sdk.LDAPURL]]
 * for RFC 4516-compliant URL parsing (default ports, case-insensitive scheme, etc.).
 */
class LdapUrlParserSuite extends KyuubiFunSuite {

  test("plain ldap with explicit port") {
    val Seq(url) = LdapUrlParser.parseAll("ldap://ldap.example.com:389")
    assert(url.getHost === "ldap.example.com")
    assert(url.getPort === 389)
    assert(url.getScheme.equalsIgnoreCase("ldap"))
  }

  test("ldaps with explicit port") {
    val Seq(url) = LdapUrlParser.parseAll("ldaps://ldap.example.com:636")
    assert(url.getHost === "ldap.example.com")
    assert(url.getPort === 636)
    assert(url.getScheme.equalsIgnoreCase("ldaps"))
  }

  test("ldap without port defaults to 389") {
    val Seq(url) = LdapUrlParser.parseAll("ldap://ldap.example.com")
    assert(url.getPort === 389)
    assert(url.getScheme.equalsIgnoreCase("ldap"))
  }

  test("ldaps without port defaults to 636") {
    val Seq(url) = LdapUrlParser.parseAll("ldaps://ldap.example.com")
    assert(url.getPort === 636)
    assert(url.getScheme.equalsIgnoreCase("ldaps"))
  }

  test("scheme matching is case-insensitive") {
    val Seq(url) = LdapUrlParser.parseAll("LDAPS://ldap.example.com:636")
    assert(url.getScheme.equalsIgnoreCase("ldaps"))
    assert(url.getHost === "ldap.example.com")
  }

  test("URL with base DN is accepted") {
    val Seq(url) = LdapUrlParser.parseAll("ldap://ldap.example.com:389/dc=example,dc=com")
    assert(url.getHost === "ldap.example.com")
    assert(url.getPort === 389)
  }

  test("parseAll splits on whitespace and returns all URLs") {
    val urls = LdapUrlParser.parseAll(
      "ldaps://server1.example.com:636 ldaps://server2.example.com:636 ldap://server3.example.com")
    assert(urls.size === 3)
    assert(urls(0).getHost === "server1.example.com")
    assert(urls(1).getHost === "server2.example.com")
    assert(urls(2).getHost === "server3.example.com")
    assert(urls(0).getScheme.equalsIgnoreCase("ldaps"))
    assert(urls(1).getScheme.equalsIgnoreCase("ldaps"))
    assert(urls(2).getScheme.equalsIgnoreCase("ldap"))
  }

  test("parseAll handles extra whitespace between URLs") {
    val urls = LdapUrlParser.parseAll("  ldap://a.com   ldap://b.com  ")
    assert(urls.size === 2)
    assert(urls(0).getHost === "a.com")
    assert(urls(1).getHost === "b.com")
  }

  test("parseAll returns empty sequence for blank input") {
    assert(LdapUrlParser.parseAll("   ").isEmpty)
    assert(LdapUrlParser.parseAll("").isEmpty)
  }

  test("invalid URL throws IllegalArgumentException") {
    val ex = intercept[IllegalArgumentException] {
      LdapUrlParser.parseAll("not-a-url")
    }
    assert(ex.getMessage.contains("Invalid LDAP URL"))
  }
}
