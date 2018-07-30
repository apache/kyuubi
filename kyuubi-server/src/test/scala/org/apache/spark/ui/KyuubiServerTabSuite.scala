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

import yaooqinn.kyuubi.ui.{KyuubiServerListener, KyuubiServerMonitor}

class KyuubiServerTabSuite extends SparkFunSuite {

  var sc: SparkContext = _
  var user: String = _
  var tab: KyuubiServerTab = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf(loadDefaults = true).setMaster("local").setAppName("test")
    sc = new SparkContext(conf)
    user = KyuubiSparkUtil.getCurrentUserName
    tab = new KyuubiServerTab(user, sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("test parent") {
    assert(tab.parent === sc.ui.get)
  }

  test("test listener") {
    assert(tab.listener !== null)
    val lr = new KyuubiServerListener(sc.conf)
    assert(tab.listener !== lr)
    assert(KyuubiServerMonitor.getListener(user).get === tab.listener)
    KyuubiServerMonitor.setListener("user1", lr)
    val tab2 = new KyuubiServerTab("user1", sc)
    assert(tab2.listener === lr)
    val tab3 = new KyuubiServerTab("user2", sc)
    assert(tab3.listener === KyuubiServerMonitor.getListener("user2").get)
  }

  test("test name") {
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
