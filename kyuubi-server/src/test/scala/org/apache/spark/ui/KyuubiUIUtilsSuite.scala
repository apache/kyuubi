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
import org.scalatest.mockito.MockitoSugar

class KyuubiUIUtilsSuite extends SparkFunSuite with MockitoSugar {

  test("spark page header") {
    val request = mock[HttpServletRequest]
    val title = "KyuubiServer test ui" * 10
    val user = KyuubiSparkUtil.getCurrentUserName
    val conf = new SparkConf().setMaster("local").setAppName(title)
    val sc = new SparkContext(conf)
    val tab = new KyuubiSessionTab(user, sc)
    val content = <td></td>
    assert(Try { KyuubiUIUtils.headerSparkPage(request, title, content, tab) }.isSuccess)
  }

  test("prepend base uri") {
    val request = mock[HttpServletRequest]
    val baseUri = KyuubiUIUtils.prependBaseUri(request)

    val basePath = KyuubiUIUtils.prependBaseUri(request, "1")
    assert(basePath === baseUri + "1")

    val resourcePath = KyuubiUIUtils.prependBaseUri(request, "1", "2")
    assert(resourcePath === baseUri + "12")
  }

}
