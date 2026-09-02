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

import com.unboundid.ldap.sdk.{Filter => LdapFilter, LDAPException}
import org.stringtemplate.v4.{ST, STGroup}

/**
 * The object that encompasses all components of a Directory Service search query.
 *
 * @see [[LdapSearch]]
 * @see [[UnboundIdDirSearch]]
 */
object Query {

  /**
   * Creates Query Builder.
   *
   * @return query builder.
   */
  def builder: Query.QueryBuilder = new Query.QueryBuilder

  /**
   * A builder of the [[Query]].
   */
  final class QueryBuilder {
    private var filterTemplate: ST = _
    private val attributes: ArrayBuffer[String] = ArrayBuffer.empty
    private var sizeLimit: Int = 0

    /**
     * Sets search filter template.
     *
     * @param filterTemplate search filter template
     * @return the current instance of the builder
     */
    def filter(filterTemplate: String): Query.QueryBuilder = {
      // Use a dedicated STGroup per filter so LDAP filter compile/render is not concurrent on the
      // same group. Sharwell (ST4 maintainer) advises against using one STGroup from multiple
      // threads at once; options are one STGroup per thread, a pool, or external synchronization.
      // See https://github.com/antlr/stringtemplate4/issues/61#issuecomment-43803970
      val group = new STGroup()
      this.filterTemplate = new ST(group, filterTemplate)
      this
    }

    /**
     * Sets mapping between names in the search filter template and actual values.
     *
     * @param key   marker in the search filter template.
     * @param value actual value
     * @return the current instance of the builder
     */
    def map(key: String, value: String): Query.QueryBuilder = {
      filterTemplate.add(key, value)
      this
    }

    /**
     * Sets mapping between names in the search filter template and an RFC 4515-escaped
     * attribute value. Use this for any user-supplied or externally-sourced string that
     * ends up in an attribute value assertion position (e.g. `(uid=<userName>)`).
     *
     * Escaping converts the five special characters (`*`, `(`, `)`, `\`, NUL) to their
     * `\xx` hex forms per RFC 4515 section 3. This prevents filter injection: a username
     * like `jsmith)(uid=admin` is rendered as `jsmith\29\28uid\3dadmin` and treated as a
     * literal string rather than injected filter syntax.
     *
     * Do NOT use this for parameters in filter-fragment positions (e.g. `(<userRdn>)`) or
     * attribute-name positions -- only attribute value literals should be escaped.
     *
     * @param key   marker in the search filter template.
     * @param value user-controlled attribute value to escape before substitution
     * @return the current instance of the builder
     */
    def mapEscaped(key: String, value: String): Query.QueryBuilder = {
      filterTemplate.add(key, LdapFilter.encodeValue(value))
      this
    }

    /**
     * Sets mapping between names in the search filter template and actual values.
     *
     * @param key    marker in the search filter template.
     * @param values array of values
     * @return the current instance of the builder
     */
    def map(key: String, values: Array[String]): Query.QueryBuilder = {
      filterTemplate.add(key, values)
      this
    }

    /**
     * Sets attribute that should be returned in results for the query.
     *
     * @param attributeName attribute name
     * @return the current instance of the builder
     */
    def returnAttribute(attributeName: String): Query.QueryBuilder = {
      attributes += attributeName
      this
    }

    /**
     * Sets the maximum number of entries to be returned as a result of the search.
     * <br>
     * 0 indicates no limit: all entries will be returned.
     *
     * @param limit The maximum number of entries that will be returned.
     * @return the current instance of the builder
     */
    def limit(limit: Int): Query.QueryBuilder = {
      sizeLimit = limit
      this
    }

    private def validate(): Unit = {
      require(filterTemplate != null, "filter is required for LDAP search query")
    }

    /**
     * Builds an instance of [[Query]].
     *
     * @return configured directory service query
     * @throws IllegalStateException if the rendered filter string is not valid RFC 4515
     */
    def build: Query = {
      validate()
      val filterStr: String = filterTemplate.render()
      // Unload template cache after render to avoid CompiledST/STToken retention
      Option(filterTemplate.groupThatCreatedThisInstance).foreach(_.unload())
      val filter: LdapFilter =
        try LdapFilter.create(filterStr)
        catch {
          case e: LDAPException =>
            throw new IllegalStateException(
              s"Rendered LDAP filter is not valid RFC 4515: '$filterStr'",
              e)
        }
      Query(filterStr, filter, attributes.toSeq, sizeLimit)
    }
  }
}

/**
 * An immutable LDAP search query.
 *
 * @param filterString The RFC 4515 filter string as rendered from the template. Used by the
 *                     JNDI path ([[LdapSearch]]) which passes a raw string to
 *                     [[javax.naming.directory.DirContext#search]].
 * @param filter       The parsed [[LdapFilter]] object. Used by the UnboundID path
 *                     ([[UnboundIdDirSearch]]) which passes it directly to [[SearchRequest]],
 *                     eliminating a redundant parse step inside the SDK.
 * @param attributes   Attribute names to return in search results. Empty means return no
 *                     attributes (DN only); see [[UnboundIdDirSearch]] and [[LdapSearch]].
 * @param sizeLimit    Maximum entries to return per base DN; 0 means no limit.
 */
case class Query(
    filterString: String,
    filter: LdapFilter,
    attributes: Seq[String],
    sizeLimit: Int)
