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

import scala.collection.mutable.ArrayBuffer

import com.unboundid.ldap.sdk.{DN, LDAPException}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{KyuubiConf, OptionalConfigEntry}
import org.apache.kyuubi.service.ServiceUtils

/**
 * Static utility methods related to LDAP authentication module.
 */
object LdapUtils extends Logging {

  /**
   * Extracts a base DN from the provided distinguished name.
   * <br>
   * <b>Example:</b>
   * <br>
   * "ou=CORP,dc=mycompany,dc=com" is the base DN for "cn=user1,ou=CORP,dc=mycompany,dc=com"
   *
   * Uses [[DN]] for correct parsing (handles escaped commas and multi-valued RDNs). Falls
   * back to a simple comma-split for pattern strings containing "%s" placeholders, which are
   * not valid DNs but are also passed through this method during filter construction.
   *
   * @param dn distinguished name or DN pattern
   * @return base DN, or null if the input has no parent (single-RDN or unparseable)
   */
  def extractBaseDn(dn: String): String =
    try {
      Option(new DN(dn).getParent).map(_.toString).orNull
    } catch {
      case _: LDAPException =>
        val idx = dn.indexOf(",")
        if (idx > -1) dn.substring(idx + 1) else null
    }

  /**
   * Extracts the first Relative Distinguished Name (RDN).
   * <br>
   * <b>Example:</b>
   * <br>
   * For DN "cn=user1,ou=CORP,dc=mycompany,dc=com" this method will return "cn=user1"
   *
   * Uses [[DN]] for correct parsing (handles escaped commas and multi-valued RDNs). Falls
   * back to a simple comma-split for pattern strings containing "%s" placeholders.
   *
   * @param dn distinguished name or DN pattern
   * @return first RDN string
   */
  def extractFirstRdn(dn: String): String =
    try {
      new DN(dn).getRDNString
    } catch {
      case _: LDAPException =>
        val idx = dn.indexOf(",")
        if (idx > -1) dn.substring(0, idx) else dn
    }

  /**
   * Extracts username from user DN.
   * <br>
   * <b>Examples:</b>
   * <pre>
   * LdapUtils.extractUserName("UserName")                        = "UserName"
   * LdapUtils.extractUserName("UserName@mycorp.com")             = "UserName"
   * LdapUtils.extractUserName("cn=UserName,dc=mycompany,dc=com") = "UserName"
   * </pre>
   */
  def extractUserName(userDn: String): String = {
    if (!isDn(userDn) && !hasDomain(userDn)) {
      return userDn
    }
    val domainIdx: Int = ServiceUtils.indexOfDomainMatch(userDn)
    if (domainIdx > 0) {
      return userDn.substring(0, domainIdx)
    }
    if (userDn.contains("=")) {
      return userDn.substring(userDn.indexOf("=") + 1, userDn.indexOf(","))
    }
    userDn
  }

  /**
   * Gets value part of the first attribute in the provided RDN.
   * <br>
   * <b>Example:</b>
   * <br>
   * For RDN "cn=user1,ou=CORP" this method will return "user1"
   *
   * @param rdn Relative Distinguished Name
   * @return value part of the first attribute
   */
  def getShortName(rdn: String): String = rdn.split(",")(0).split("=")(1)

  /**
   * Check for a domain part in the provided username.
   * <br>
   * <b>Example:</b>
   * <br>
   * <pre>
   * LdapUtils.hasDomain("user1@mycorp.com") = true
   * LdapUtils.hasDomain("user1")            = false
   * </pre>
   *
   * @param userName username
   * @return true if `userName` contains `@<domain>` part
   */
  def hasDomain(userName: String): Boolean = {
    ServiceUtils.indexOfDomainMatch(userName) > 0
  }

  /**
   * Get the username part in the provided user.
   * <br>
   * <b>Example:</b>
   * <br>
   * For user "user1@mycorp.com" this method will return "user1"
   *
   * @param user user
   * @return the username part in the provided user
   */
  def getUserName(user: String): String =
    if (LdapUtils.hasDomain(user)) LdapUtils.extractUserName(user) else user

  /**
   * Detects DN names.
   * <br>
   * <b>Example:</b>
   * <br>
   * <pre>
   * LdapUtils.isDn("cn=UserName,dc=mycompany,dc=com") = true
   * LdapUtils.isDn("user1")                           = false
   * LdapUtils.isDn(null)                              = false
   * </pre>
   *
   * Uses [[DN.isValidDN]] for RFC 4514-compliant validation (handles multi-valued RDNs,
   * escaped special characters, etc.) instead of a naive `contains("=")` check that would
   * produce false positives for arbitrary "key=value" strings.
   *
   * The empty string is explicitly rejected even though it is the (valid) "null DN" per
   * RFC 4514, because for our use case (deciding whether a value is a DN-typed login or a
   * uid-style login) an empty value is never a DN.
   *
   * @param name name to be checked
   * @return true if the provided name is a non-empty, valid distinguished name
   */
  def isDn(name: String): Boolean =
    name != null && name.nonEmpty && DN.isValidDN(name)

  /**
   * Reads and parses DN patterns from Kyuubi configuration.
   * <br>
   * If no patterns are provided in the configuration, then the base DN will be used.
   *
   * @param conf Kyuubi configuration
   * @param confKey  configuration key to be read
   * @return a list of DN patterns
   * @see [[KyuubiConf.AUTHENTICATION_LDAP_BASE_DN]]
   * @see [[KyuubiConf.AUTHENTICATION_LDAP_GUID_KEY]]
   * @see [[KyuubiConf.AUTHENTICATION_LDAP_GROUP_DN_PATTERN]]
   * @see [[KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN]]
   */
  def parseDnPatterns(conf: KyuubiConf, confKey: OptionalConfigEntry[String]): Array[String] = {
    val result = new ArrayBuffer[String]
    conf.get(confKey).map { patternsString =>
      patternsString.split(":").foreach { pattern =>
        if (pattern.contains(",") && pattern.contains("=")) {
          result += pattern
        } else {
          warn(s"Unexpected format for $confKey, ignoring $pattern")
        }
      }
    }.getOrElse {
      val guidAttr = conf.get(KyuubiConf.AUTHENTICATION_LDAP_GUID_KEY)
      conf.get(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN).foreach { defaultBaseDn =>
        result += s"$guidAttr=%s,$defaultBaseDn"
      }
    }
    result.toArray
  }

  private def patternToBaseDn(pattern: String): String =
    if (pattern.contains("=%s")) pattern.split(",", 2)(1) else pattern

  /**
   * Converts a collection of Distinguished Name patterns to a collection of base DNs.
   *
   * @param patterns Distinguished Name patterns
   * @return a list of base DNs
   * @see [[KyuubiConf.AUTHENTICATION_LDAP_GROUP_DN_PATTERN]]
   * @see [[KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN]]
   */
  def patternsToBaseDns(patterns: Array[String]): Array[String] = {
    patterns.map(patternToBaseDn)
  }

  /**
   * Creates a list of principals to be used for user authentication.
   *
   * @param conf Kyuubi configuration
   * @param user username
   * @return a list of user's principals
   */
  def createCandidatePrincipals(conf: KyuubiConf, user: String): Array[String] = {
    if (hasDomain(user) || isDn(user)) {
      return Array(user)
    }
    conf.get(KyuubiConf.AUTHENTICATION_LDAP_DOMAIN).map { ldapDomain =>
      Array(user + "@" + ldapDomain)
    }.getOrElse {
      val userPatterns = parseDnPatterns(conf, KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN)
      if (userPatterns.isEmpty) {
        return Array(user)
      }
      // Use replace (literal) not replaceAll (regex): a user containing '$' or '\' is
      // otherwise interpreted as a regex replacement back-reference/escape.
      userPatterns.map(_.replace("%s", user))
    }
  }
}
