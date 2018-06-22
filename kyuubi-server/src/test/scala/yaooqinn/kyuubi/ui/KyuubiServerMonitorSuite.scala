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

package yaooqinn.kyuubi.ui

import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.ui.KyuubiServerTab

class KyuubiServerMonitorSuite extends SparkFunSuite {
  val conf = new SparkConf(loadDefaults = true).setAppName("monitor").setMaster("local")

  var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    sc = new SparkContext(conf)
  }

  protected override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  test("kyuubi server monitor") {
    val li = new KyuubiServerListener(conf)
    val user = KyuubiSparkUtil.getCurrentUserName()
    KyuubiServerMonitor.setListener(user, li)
    val liOp = KyuubiServerMonitor.getListener(user)
    assert(liOp.get === li)
    assert(KyuubiServerMonitor.getListener("fake").isEmpty)
    val tab = new KyuubiServerTab(user, sc)
    KyuubiServerMonitor.addUITab(user, tab)
    KyuubiServerMonitor.detachUITab(user)
    KyuubiServerMonitor.detachAllUITabs()
  }

}
