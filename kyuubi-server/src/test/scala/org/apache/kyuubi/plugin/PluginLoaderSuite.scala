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

package org.apache.kyuubi.plugin

import scala.collection.JavaConverters._

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.session.FileSessionConfAdvisor

class PluginLoaderSuite extends KyuubiFunSuite {

  test("test engine conf advisor wrong class") {
    val conf = new KyuubiConf(false)
    assert(PluginLoader.loadSessionConfAdvisor(conf).isInstanceOf[DefaultSessionConfAdvisor])

    conf.set(KyuubiConf.SESSION_CONF_ADVISOR, classOf[FakeAdvisor].getName)
    val msg1 = intercept[KyuubiException] {
      PluginLoader.loadSessionConfAdvisor(conf)
    }.getMessage
    assert(msg1.contains(s"is not a child of '${classOf[SessionConfAdvisor].getName}'"))

    conf.set(KyuubiConf.SESSION_CONF_ADVISOR, "non.exists")
    val msg2 = intercept[IllegalArgumentException] {
      PluginLoader.loadSessionConfAdvisor(conf)
    }.getMessage
    assert(msg2.startsWith("Error while instantiating 'non.exists'"))

  }

  test("test FileSessionConfAdvisor") {
    val conf = new KyuubiConf(false)
    conf.set(KyuubiConf.SESSION_CONF_ADVISOR, classOf[FileSessionConfAdvisor].getName)
    val advisor = PluginLoader.loadSessionConfAdvisor(conf)
    val emptyConfig = advisor.getConfOverlay("chris", conf.getAll.asJava)
    assert(emptyConfig.isEmpty)

    conf.set(KyuubiConf.SESSION_CONF_PROFILE, "non.exists")
    val nonexistsConfig = advisor.getConfOverlay("chris", conf.getAll.asJava)
    assert(nonexistsConfig.isEmpty)

    conf.set(KyuubiConf.SESSION_CONF_PROFILE, "cluster-a")
    val clusteraConf = advisor.getConfOverlay("chris", conf.getAll.asJava)
    assert(clusteraConf.get("kyuubi.ha.namespace") == "kyuubi-ns-a")
    assert(clusteraConf.get("kyuubi.zk.ha.namespace") == null)
    assert(clusteraConf.size() == 5)

    val clusteraConfFromCache = advisor.getConfOverlay("chris", conf.getAll.asJava)
    assert(clusteraConfFromCache.get("kyuubi.ha.namespace") == "kyuubi-ns-a")
    assert(clusteraConfFromCache.get("kyuubi.zk.ha.namespace") == null)
    assert(clusteraConfFromCache.size() == 5)
  }
}

class FakeAdvisor {}
