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

import javax.naming.{NamingEnumeration, NamingException}
import javax.naming.directory.SearchResult

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.Logging

/**
 * The object that handles Directory Service search results.
 * In most cases it converts search results into a list of names in the namespace.
 */
object SearchResultHandler {

  /**
   * An interface used by [[SearchResultHandler]] for processing records of
   * a [[SearchResult]] on a per-record basis.
   * <br>
   * Implementations of this interface perform the actual work of processing each record,
   * but don't need to worry about exception handling, closing underlying data structures,
   * and combining results from several search requests.
   *
   * @see SearchResultHandler
   */
  trait RecordProcessor extends (SearchResult => Boolean) {

    /**
     * Implementations must implement this method to process each record in [[SearchResult]].
     *
     * @param record the [[SearchResult]] to precess
     * @return true to continue processing, false to stop iterating
     *         over search results
     */
    @throws[NamingException]
    override def apply(record: SearchResult): Boolean
  }
}

/**
 * Constructs a search result handler object for the provided search results.
 *
 * @param searchResults directory service search results
 */
class SearchResultHandler(val searchResults: Array[NamingEnumeration[SearchResult]])
  extends Logging {

  /**
   * Returns all entries from the search result.
   *
   * @return a list of names in the namespace
   */
  @throws[NamingException]
  def getAllLdapNames: Array[String] = {
    val result = new ArrayBuffer[String]
    handle { record => result += record.getNameInNamespace; true }
    result.toArray
  }

  /**
   * Checks whether search result contains exactly one entry.
   *
   * @return true if the search result contains a single entry.
   */
  @throws[NamingException]
  def hasSingleResult: Boolean = {
    val allResults = getAllLdapNames
    allResults != null && allResults.length == 1
  }

  /**
   * Returns a single entry from the search result.
   * Throws [[NamingException]] if the search result doesn't contain exactly one entry.
   *
   * @return name in the namespace
   */
  @throws[NamingException]
  def getSingleLdapName: String = {
    val allLdapNames = getAllLdapNames
    if (allLdapNames.length == 1) return allLdapNames.head
    throw new NamingException("Single result was expected")
  }

  /**
   * Returns all entries and all attributes for these entries.
   *
   * @return a list that includes all entries and all attributes from these entries.
   */
  @throws[NamingException]
  def getAllLdapNamesAndAttributes: Array[String] = {
    val result = new ArrayBuffer[String]

    @throws[NamingException]
    def addAllAttributeValuesToResult(values: NamingEnumeration[_]): Unit = {
      while (values.hasMore) result += String.valueOf(values.next)
    }
    handle { record =>
      result += record.getNameInNamespace
      val allAttributes = record.getAttributes.getAll
      while (allAttributes.hasMore) {
        val attribute = allAttributes.next
        addAllAttributeValuesToResult(attribute.getAll)
      }
      true
    }
    result.toArray
  }

  /**
   * Allows for custom processing of the search results.
   *
   * @param processor [[SearchResultHandler.RecordProcessor]] implementation
   */
  @throws[NamingException]
  def handle(processor: SearchResultHandler.RecordProcessor): Unit = {
    try {
      searchResults.foreach { searchResult =>
        while (searchResult.hasMore) if (!processor.apply(searchResult.next)) return
      }
    } finally {
      searchResults.foreach { searchResult =>
        try {
          searchResult.close()
        } catch {
          case ex: NamingException =>
            warn("Failed to close LDAP search result", ex)
        }
      }
    }
  }
}
