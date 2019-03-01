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

class KyuubiSessionPageSuite extends SparkFunSuite with MockitoSugar {

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
    val page = new KyuubiSessionPage(tab)
    val request = mock[HttpServletRequest]
    assert(Try { page.render(request) }.isSuccess )
  }

}
