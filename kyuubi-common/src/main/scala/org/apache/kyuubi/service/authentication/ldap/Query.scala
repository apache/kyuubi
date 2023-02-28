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

import java.util
import javax.naming.directory.SearchControls

import org.stringtemplate.v4.ST

/**
 * The object that encompasses all components of a Directory Service search query.
 *
 * @see [[LdapSearch]]
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
    private val controls: SearchControls = {
      val _controls = new SearchControls
      _controls.setSearchScope(SearchControls.SUBTREE_SCOPE)
      _controls.setReturningAttributes(new Array[String](0))
      _controls
    }
    private val returningAttributes: util.List[String] = new util.ArrayList[String]

    /**
     * Sets search filter template.
     *
     * @param filterTemplate search filter template
     * @return the current instance of the builder
     */
    def filter(filterTemplate: String): Query.QueryBuilder = {
      this.filterTemplate = new ST(filterTemplate)
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
      returningAttributes.add(attributeName)
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
      controls.setCountLimit(limit)
      this
    }

    private def validate(): Unit = {
      require(filterTemplate != null, "filter is required for LDAP search query")
    }

    private def createFilter: String = filterTemplate.render

    private def updateControls(): Unit = {
      if (!returningAttributes.isEmpty) controls.setReturningAttributes(
        returningAttributes.toArray(new Array[String](returningAttributes.size)))
    }

    /**
     * Builds an instance of [[Query]].
     *
     * @return configured directory service query
     */
    def build: Query = {
      validate()
      val filter: String = createFilter
      updateControls()
      new Query(filter, controls)
    }
  }
}

case class Query(filter: String, controls: SearchControls)
