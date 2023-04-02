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
import javax.naming.{NamingEnumeration, NamingException}
import javax.naming.directory.SearchResult

import scala.collection.mutable.ArrayBuffer

import org.mockito.Mockito.{atLeastOnce, doThrow, verify}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.service.authentication.ldap.LdapTestUtils._

class SearchResultHandlerSuite extends KyuubiFunSuite {
  private var handler: SearchResultHandler = _

  test("handle") {
    val resultCollection = new MockResultCollectionBuilder()
      .addSearchResultWithDns("1")
      .addSearchResultWithDns("2", "3")
      .build
    handler = new SearchResultHandler(resultCollection)
    val expected: util.List[String] = util.Arrays.asList("1", "2")
    val actual: util.List[String] = new util.ArrayList[String]
    handler.handle { record =>
      actual.add(record.getNameInNamespace)
      actual.size < 2
    }
    assert(expected === actual)
    assertAllNamingEnumerationsClosed(resultCollection)
  }

  test("GetAllLdapNamesNoRecords") {
    val resultCollection = new MockResultCollectionBuilder()
      .addEmptySearchResult()
      .build
    handler = new SearchResultHandler(resultCollection)
    val actual = handler.getAllLdapNames
    assert(actual.isEmpty, "ResultSet size")
    assertAllNamingEnumerationsClosed(resultCollection)
  }

  test("GetAllLdapNamesWithExceptionInNamingEnumerationClose") {
    val resultCollection = new MockResultCollectionBuilder()
      .addSearchResultWithDns("1")
      .addSearchResultWithDns("2")
      .build
    doThrow(classOf[NamingException]).when(resultCollection.iterator.next).close()
    handler = new SearchResultHandler(resultCollection)
    val actual = handler.getAllLdapNames
    assert(actual.length === 2, "ResultSet size")
    assertAllNamingEnumerationsClosed(resultCollection)
  }

  test("GetAllLdapNames") {
    val objectDn1: String = "cn=a1,dc=b,dc=c"
    val objectDn2: String = "cn=a2,dc=b,dc=c"
    val objectDn3: String = "cn=a3,dc=b,dc=c"
    val resultCollection = new MockResultCollectionBuilder()
      .addSearchResultWithDns(objectDn1)
      .addSearchResultWithDns(objectDn2, objectDn3)
      .build
    handler = new SearchResultHandler(resultCollection)
    val expected = Array(objectDn1, objectDn2, objectDn3)
    val actual = handler.getAllLdapNames
    assert(expected.sorted === actual.sorted)
    assertAllNamingEnumerationsClosed(resultCollection)
  }

  test("GetAllLdapNamesAndAttributes") {
    val searchResult1 = mockSearchResult(
      "cn=a1,dc=b,dc=c",
      mockAttributes("attr1", "attr1value1"))
    val searchResult2 = mockSearchResult(
      "cn=a2,dc=b,dc=c",
      mockAttributes("attr1", "attr1value2", "attr2", "attr2value1"))
    val searchResult3 = mockSearchResult(
      "cn=a3,dc=b,dc=c",
      mockAttributes("attr1", "attr1value3", "attr1", "attr1value4"))
    val searchResult4 = mockSearchResult(
      "cn=a4,dc=b,dc=c",
      mockEmptyAttributes())
    val resultCollection = new MockResultCollectionBuilder()
      .addSearchResults(searchResult1)
      .addSearchResults(searchResult2, searchResult3)
      .addSearchResults(searchResult4)
      .build
    handler = new SearchResultHandler(resultCollection)
    val expected = Array(
      "cn=a1,dc=b,dc=c",
      "attr1value1",
      "cn=a2,dc=b,dc=c",
      "attr1value2",
      "attr2value1",
      "cn=a3,dc=b,dc=c",
      "attr1value3",
      "attr1value4",
      "cn=a4,dc=b,dc=c")
    val actual = handler.getAllLdapNamesAndAttributes
    assert(expected.sorted === actual.sorted)
    assertAllNamingEnumerationsClosed(resultCollection)
  }

  test("HasSingleResultNoRecords") {
    val resultCollection = new MockResultCollectionBuilder()
      .addEmptySearchResult()
      .build
    handler = new SearchResultHandler(resultCollection)
    assert(!handler.hasSingleResult)
    assertAllNamingEnumerationsClosed(resultCollection)
  }

  test("HasSingleResult") {
    val resultCollection = new MockResultCollectionBuilder()
      .addSearchResultWithDns("1")
      .build
    handler = new SearchResultHandler(resultCollection)
    assert(handler.hasSingleResult)
    assertAllNamingEnumerationsClosed(resultCollection)
  }

  test("HasSingleResultManyRecords") {
    val resultCollection = new MockResultCollectionBuilder()
      .addSearchResultWithDns("1")
      .addSearchResultWithDns("2")
      .build
    handler = new SearchResultHandler(resultCollection)
    assert(!handler.hasSingleResult)
    assertAllNamingEnumerationsClosed(resultCollection)
  }

  test("GetSingleLdapNameNoRecords") {
    intercept[NamingException] {
      val resultCollection = new MockResultCollectionBuilder()
        .addEmptySearchResult()
        .build
      handler = new SearchResultHandler(resultCollection)
      try handler.getSingleLdapName
      finally {
        assertAllNamingEnumerationsClosed(resultCollection)
      }
    }
  }

  test("GetSingleLdapName") {
    val objectDn: String = "cn=a,dc=b,dc=c"
    val resultCollection = new MockResultCollectionBuilder()
      .addEmptySearchResult()
      .addSearchResultWithDns(objectDn)
      .build
    handler = new SearchResultHandler(resultCollection)
    val expected: String = objectDn
    val actual: String = handler.getSingleLdapName
    assert(expected === actual)
    assertAllNamingEnumerationsClosed(resultCollection)
  }

  private def assertAllNamingEnumerationsClosed(
      resultCollection: Array[NamingEnumeration[SearchResult]]): Unit = {
    for (namingEnumeration <- resultCollection) {
      verify(namingEnumeration, atLeastOnce).close()
    }
  }
}

class MockResultCollectionBuilder {

  val results = new ArrayBuffer[NamingEnumeration[SearchResult]]

  def addSearchResultWithDns(dns: String*): MockResultCollectionBuilder = {
    results += mockNamingEnumeration(dns: _*)
    this
  }

  def addSearchResults(dns: SearchResult*): MockResultCollectionBuilder = {
    results += mockNamingEnumeration(dns.toArray)
    this
  }

  def addEmptySearchResult(): MockResultCollectionBuilder = {
    addSearchResults()
    this
  }

  def build: Array[NamingEnumeration[SearchResult]] = results.toArray
}
