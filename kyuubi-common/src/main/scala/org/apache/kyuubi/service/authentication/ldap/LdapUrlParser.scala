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

import com.unboundid.ldap.sdk.{LDAPException, LDAPURL}

/**
 * Thin helper for parsing the whitespace-separated LDAP URL list stored in
 * [[org.apache.kyuubi.config.KyuubiConf.AUTHENTICATION_LDAP_URL]].
 *
 * Delegates all URL parsing to [[LDAPURL]], which is RFC 4516-compliant and handles
 * default ports (389 for ldap, 636 for ldaps) without custom regex logic.
 */
private[ldap] object LdapUrlParser {

  /**
   * Splits `urlString` on whitespace and parses each token as an [[LDAPURL]].
   *
   * @throws IllegalArgumentException if any token is not a valid LDAP URL.
   */
  def parseAll(urlString: String): Seq[LDAPURL] =
    urlString.trim.split("\\s+").iterator
      .filter(_.nonEmpty)
      .map { raw =>
        try new LDAPURL(raw)
        catch {
          case e: LDAPException =>
            throw new IllegalArgumentException(s"Invalid LDAP URL: $raw", e)
        }
      }
      .toSeq
}
