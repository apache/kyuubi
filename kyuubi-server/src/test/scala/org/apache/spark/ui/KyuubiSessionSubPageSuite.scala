/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.util.Try

import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext, SparkFunSuite}
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.ui.{ExecutionInfo, KyuubiServerListener, SessionInfo}

class KyuubiSessionSubPageSuite extends SparkFunSuite with MockitoSugar {

  var sc: SparkContext = _
  var user: String = _
  var tab: KyuubiSessionTab = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf(loadDefaults = true).setMaster("local").setAppName("test")
    sc = new SparkContext(conf)
    user = KyuubiSparkUtil.getCurrentUserName
    tab = new KyuubiSessionTab(user, sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("render kyuubi session page") {
    val page = new KyuubiSessionSubPage(tab)

    val request = mock[HttpServletRequest]
    intercept[IllegalArgumentException](page.render(request))

    val id = "id1"
    when(request.getParameter("id")).thenReturn(id)
    intercept[IllegalArgumentException](page.render(request))

    val sessionInfo = mock[SessionInfo]
    val tab1 = mock[KyuubiSessionTab]
    when(request.getParameter("id")).thenReturn(id)
    val listener = mock[KyuubiServerListener]
    when(tab1.listener).thenReturn(listener)
    when(listener.getSession(id)).thenReturn(Some(sessionInfo))
    when(sessionInfo.sessionId).thenReturn("1")
    when(listener.getExecutionList).thenReturn(Seq[ExecutionInfo]())
    when(tab1.appName).thenReturn("name")
    when(tab1.headerTabs).thenReturn(Seq[WebUITab]())
    val page2 = new KyuubiSessionSubPage(tab1)
    assert(Try { page2.render(request) }.isSuccess )
  }
}
