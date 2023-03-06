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
   * @param dn distinguished name
   * @return base DN
   */
  def extractBaseDn(dn: String): String = {
    val indexOfFirstDelimiter = dn.indexOf(",")
    if (indexOfFirstDelimiter > -1) {
      return dn.substring(indexOfFirstDelimiter + 1)
    }
    null
  }

  /**
   * Extracts the first Relative Distinguished Name (RDN).
   * <br>
   * <b>Example:</b>
   * <br>
   * For DN "cn=user1,ou=CORP,dc=mycompany,dc=com" this method will return "cn=user1"
   *
   * @param dn distinguished name
   * @return first RDN
   */
  def extractFirstRdn(dn: String): String = dn.substring(0, dn.indexOf(","))

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
   * @return true if `userName`` contains `@<domain>` part
   */
  def hasDomain(userName: String): Boolean = {
    ServiceUtils.indexOfDomainMatch(userName) > 0
  }

  /**
   * Detects DN names.
   * <br>
   * <b>Example:</b>
   * <br>
   * <pre>
   * LdapUtils.isDn("cn=UserName,dc=mycompany,dc=com") = true
   * LdapUtils.isDn("user1")                           = false
   * </pre>
   *
   * @param name name to be checked
   * @return true if the provided name is a distinguished name
   */
  def isDn(name: String): Boolean = {
    name.contains("=")
  }

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
      userPatterns.map(_.replaceAll("%s", user))
    }
  }
}
