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

import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext, SparkFunSuite}

import yaooqinn.kyuubi.ui.KyuubiServerListener

class KyuubiServerTabSuite extends SparkFunSuite {

  var sc: SparkContext = _
  var user: String = _
  var tab: KyuubiServerTab = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf(loadDefaults = true).setMaster("local").setAppName("test")
    sc = new SparkContext(conf)
    user = KyuubiSparkUtil.getCurrentUserName()
    tab = new KyuubiServerTab(user, sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("testParent") {
    assert(tab.parent === sc.ui.get)
  }

  test("testListener") {
    assert(tab.listener !== null)
    assert(tab.listener.isInstanceOf[KyuubiServerListener])
  }

  test("testName") {
    assert(tab.name === "Kyuubi Tab 4 " + user)

  }

  test("testDetach") {
    assert(KyuubiServerTab.getSparkUI(sc).getTabs.contains(tab))
    tab.detach()
    assert(!KyuubiServerTab.getSparkUI(sc).getTabs.contains(tab))
  }

  test("testGetSparkUI") {
    assert(KyuubiServerTab.getSparkUI(sc) === sc.ui.get)
  }
}
