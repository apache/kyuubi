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

import javax.naming.{NameNotFoundException, NamingEnumeration, NamingException}
import javax.naming.directory.{DirContext, SearchControls, SearchResult}

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

/**
 * Implements search for LDAP.
 * @param conf Kyuubi configuration
 * @param ctx  Directory service that will be used for the queries.
 */
class LdapSearch(conf: KyuubiConf, ctx: DirContext) extends DirSearch with Logging {

  final private val baseDn = conf.get(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN).orNull
  final private val groupBases: Array[String] =
    LdapUtils.patternsToBaseDns(
      LdapUtils.parseDnPatterns(conf, KyuubiConf.AUTHENTICATION_LDAP_GROUP_DN_PATTERN))
  final private val userPatterns: Array[String] =
    LdapUtils.parseDnPatterns(conf, KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN)
  final private val userBases: Array[String] = LdapUtils.patternsToBaseDns(userPatterns)
  final private val queries: QueryFactory = new QueryFactory(conf)

  /**
   * Closes this search object and releases any system resources associated
   * with it. If the search object is already closed then invoking this
   * method has no effect.
   */
  override def close(): Unit = {
    try ctx.close()
    catch {
      case e: NamingException =>
        warn("Exception when closing LDAP context:", e)
    }
  }

  @throws[NamingException]
  override def findUserDn(user: String): String = {
    var allLdapNames: Array[String] = null
    if (LdapUtils.isDn(user)) {
      val userBaseDn: String = LdapUtils.extractBaseDn(user)
      val userRdn: String = LdapUtils.extractFirstRdn(user)
      allLdapNames = execute(Array(userBaseDn), queries.findUserDnByRdn(userRdn)).getAllLdapNames
    } else {
      allLdapNames = findDnByPattern(userPatterns, user)
      if (allLdapNames.isEmpty) {
        allLdapNames = execute(userBases, queries.findUserDnByName(user)).getAllLdapNames
      }
    }
    if (allLdapNames.length == 1) allLdapNames.head
    else {
      info(s"Expected exactly one user result for the user: $user, " +
        s"but got ${allLdapNames.length}. Returning null")
      debug("Matched users: $allLdapNames")
      null
    }
  }

  @throws[NamingException]
  private def findDnByPattern(patterns: Seq[String], name: String): Array[String] = {
    for (pattern <- patterns) {
      val baseDnFromPattern: String = LdapUtils.extractBaseDn(pattern)
      // Use replace (literal) not replaceAll (regex): a name containing '$' or '\' is
      // otherwise interpreted as a regex replacement back-reference/escape, corrupting the
      // RDN or throwing IndexOutOfBoundsException.
      val rdn = LdapUtils.extractFirstRdn(pattern).replace("%s", name)
      val names = execute(Array(baseDnFromPattern), queries.findDnByPattern(rdn)).getAllLdapNames
      if (!names.isEmpty) return names
    }
    Array.empty
  }

  @throws[NamingException]
  override def findGroupDn(group: String): String =
    execute(groupBases, queries.findGroupDnById(group)).getSingleLdapName

  @throws[NamingException]
  override def isUserMemberOfGroup(user: String, groupDn: String): Boolean = {
    val userId = LdapUtils.extractUserName(user)
    execute(userBases, queries.isUserMemberOfGroup(userId, groupDn)).hasSingleResult
  }

  @throws[NamingException]
  override def findGroupsForUser(userDn: String): Array[String] = {
    val userName = LdapUtils.extractUserName(userDn)
    execute(groupBases, queries.findGroupsForUser(userName, userDn)).getAllLdapNames
  }

  @throws[NamingException]
  override def executeCustomQuery(query: String): Array[String] =
    execute(Array(baseDn), queries.customQuery(query)).getAllLdapNamesAndAttributes

  private def execute(baseDns: Array[String], query: Query): SearchResultHandler = {
    // Reconstruct JNDI SearchControls from the Query fields. Query no longer holds a
    // SearchControls directly; the UnboundID path uses query.filter (Filter object) and
    // query.attributes / query.sizeLimit natively, while this JNDI path builds its own.
    val controls = new SearchControls()
    controls.setSearchScope(SearchControls.SUBTREE_SCOPE)
    // Always set returning attributes. An empty array means "return no attributes" (DN only)
    // -- the pre-UnboundID default -- so DN-only lookups do not fetch every attribute; the
    // SearchControls default of null would instead mean "return all". customQuery is the only
    // query that populates attributes, so it still receives its requested attribute.
    controls.setReturningAttributes(query.attributes.toArray)
    if (query.sizeLimit > 0) controls.setCountLimit(query.sizeLimit)

    val searchResults = new ArrayBuffer[NamingEnumeration[SearchResult]]
    debug(s"Executing a query: '${query.filterString}' with base DNs ${baseDns.mkString(",")}")
    baseDns.foreach { baseDn =>
      try {
        val searchResult = ctx.search(baseDn, query.filterString, controls)
        if (searchResult != null) searchResults += searchResult
      } catch {
        case _: NameNotFoundException =>
          // Base DN does not exist in the directory -- not an error, try the next base.
          // This is the JNDI equivalent of UnboundIdDirSearch swallowing NO_SUCH_OBJECT.
          debug(s"Base DN '$baseDn' not found for query '${query.filterString}', skipping.")
        case ex: NamingException =>
          // Any other JNDI failure (CommunicationException, ServiceUnavailableException,
          // AuthenticationException, etc.) means the LDAP server is unreachable or the
          // request was rejected at the protocol level. Propagate so callers can
          // distinguish "user not found" from "LDAP is down" -- the original broad catch
          // silently turned infrastructure failures into empty results, masking outages.
          throw ex
      }
    }
    new SearchResultHandler(searchResults.toArray)
  }
}
