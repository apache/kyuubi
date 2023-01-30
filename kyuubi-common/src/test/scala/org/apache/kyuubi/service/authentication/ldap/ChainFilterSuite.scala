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

import javax.security.sasl.AuthenticationException

import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{doThrow, times, verify, when}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class ChainFilterSuite extends KyuubiFunSuite {
  private var conf: KyuubiConf = _
  private var filter1: Filter = _
  private var filter2: Filter = _
  private var filter3: Filter = _
  private var factory1: FilterFactory = _
  private var factory2: FilterFactory = _
  private var factory3: FilterFactory = _
  private var factory: FilterFactory = _
  private var search: DirSearch = _

  override def beforeEach(): Unit = {
    conf = new KyuubiConf()
    filter1 = mock[Filter]
    filter2 = mock[Filter]
    filter3 = mock[Filter]
    factory1 = mock[FilterFactory]
    factory2 = mock[FilterFactory]
    factory3 = mock[FilterFactory]
    factory = new ChainFilterFactory(factory1, factory2, factory3)
    search = mock[DirSearch]
    super.beforeEach()
  }

  test("FactoryAllNull") {
    when(factory1.getInstance(any(classOf[KyuubiConf]))).thenReturn(None)
    when(factory2.getInstance(any(classOf[KyuubiConf]))).thenReturn(None)
    when(factory3.getInstance(any(classOf[KyuubiConf]))).thenReturn(None)
    assert(factory.getInstance(conf).isEmpty)
  }

  test("FactoryAllEmpty") {
    val emptyFactory = new ChainFilterFactory()
    assert(emptyFactory.getInstance(conf).isEmpty)
  }

  test("Factory") {
    when(factory1.getInstance(any(classOf[KyuubiConf]))).thenReturn(Some(filter1))
    when(factory2.getInstance(any(classOf[KyuubiConf]))).thenReturn(Some(filter2))
    when(factory3.getInstance(any(classOf[KyuubiConf]))).thenReturn(Some(filter3))
    val filter = factory.getInstance(conf).get
    filter.apply(search, "User")
    verify(filter1, times(1)).apply(search, "User")
    verify(filter2, times(1)).apply(search, "User")
    verify(filter3, times(1)).apply(search, "User")
  }

  test("ApplyNegative") {
    intercept[AuthenticationException] {
      doThrow(classOf[AuthenticationException])
        .when(filter3)
        .apply(any().asInstanceOf[DirSearch], anyString)
      when(factory1.getInstance(any(classOf[KyuubiConf]))).thenReturn(Some(filter1))
      when(factory2.getInstance(any(classOf[KyuubiConf]))).thenReturn(None)
      when(factory3.getInstance(any(classOf[KyuubiConf]))).thenReturn(Some(filter3))
      val filter = factory.getInstance(conf).get
      filter.apply(search, "User")
    }
  }
}
