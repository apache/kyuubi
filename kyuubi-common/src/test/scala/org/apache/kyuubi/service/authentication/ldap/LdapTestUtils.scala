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
import javax.naming.directory._

import org.mockito.Mockito.when
import org.mockito.stubbing.OngoingStubbing
import org.scalatestplus.mockito.MockitoSugar

case class NameValues(name: String, values: String*)
case class Credentials(user: String, password: String)

case class User(dn: String, id: String, password: String) {

  def credentialsWithDn: Credentials = Credentials(dn, password)

  def credentialsWithId: Credentials = Credentials(id, password)
}

object User {
  def useIdForPassword(dn: String, id: String): User = User(dn, id, id)
}

object LdapTestUtils extends MockitoSugar {
  @throws[NamingException]
  def mockEmptyNamingEnumeration: NamingEnumeration[SearchResult] =
    mockNamingEnumeration(new Array[SearchResult](0))

  @throws[NamingException]
  def mockNamingEnumeration(dns: String*): NamingEnumeration[SearchResult] =
    mockNamingEnumeration(mockSearchResults(dns.toArray))

  @throws[NamingException]
  def mockNamingEnumeration(searchResults: Array[SearchResult]): NamingEnumeration[SearchResult] = {
    val ne = mock[NamingEnumeration[SearchResult]]
    mockHasMoreMethod(ne, searchResults.length)
    if (searchResults.nonEmpty) {
      val mockedResults = Array(searchResults: _*)
      mockNextMethod(ne, mockedResults)
    }
    ne
  }

  @throws[NamingException]
  def mockHasMoreMethod(ne: NamingEnumeration[SearchResult], length: Int): Unit = {
    var hasMoreStub: OngoingStubbing[Boolean] = when(ne.hasMore)
    (0 until length).foreach(_ => hasMoreStub = hasMoreStub.thenReturn(true))
    hasMoreStub.thenReturn(false)
  }

  @throws[NamingException]
  def mockNextMethod(
      ne: NamingEnumeration[SearchResult],
      searchResults: Array[SearchResult]): Unit = {
    var nextStub: OngoingStubbing[SearchResult] = when(ne.next)
    searchResults.foreach { searchResult =>
      nextStub = nextStub.thenReturn(searchResult)
    }
  }

  def mockSearchResults(dns: Array[String]): Array[SearchResult] = {
    dns.map(mockSearchResult(_, null))
  }

  def mockSearchResult(dn: String, attributes: Attributes): SearchResult = {
    val searchResult = mock[SearchResult]
    when(searchResult.getNameInNamespace).thenReturn(dn)
    when(searchResult.getAttributes).thenReturn(attributes)
    searchResult
  }

  @throws[NamingException]
  def mockEmptyAttributes(): Attributes = mockAttributes()

  @throws[NamingException]
  def mockAttributes(name: String, value: String): Attributes =
    mockAttributes(NameValues(name, value))

  @throws[NamingException]
  def mockAttributes(name1: String, value1: String, name2: String, value2: String): Attributes =
    if (name1 == name2) {
      mockAttributes(NameValues(name1, value1, value2))
    } else {
      mockAttributes(
        NameValues(name1, value1),
        NameValues(name2, value2))
    }

  @throws[NamingException]
  private def mockAttributes(namedValues: NameValues*): Attributes = {
    val attributes = new BasicAttributes
    namedValues.foreach { namedValue =>
      val attr = new BasicAttribute(namedValue.name)
      namedValue.values.foreach(attr.add)
      attributes.put(attr)
    }
    attributes
  }
}
