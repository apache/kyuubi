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
import org.apache.kyuubi.session.{FileSessionConfAdvisor, HadoopGroupProvider}

class PluginLoaderSuite extends KyuubiFunSuite {

  test("SessionConfAdvisor - wrong class") {
    val conf = new KyuubiConf(false)
    assert(PluginLoader.loadSessionConfAdvisor(conf).head.isInstanceOf[DefaultSessionConfAdvisor])

    conf.set(KyuubiConf.SESSION_CONF_ADVISOR, Seq(classOf[InvalidSessionConfAdvisor].getName))
    val msg1 = intercept[KyuubiException] {
      PluginLoader.loadSessionConfAdvisor(conf)
    }.getMessage
    assert(msg1.contains(s"is not a child of '${classOf[SessionConfAdvisor].getName}'"))

    conf.set(KyuubiConf.SESSION_CONF_ADVISOR, Seq("non.exists"))
    val msg2 = intercept[IllegalArgumentException] {
      PluginLoader.loadSessionConfAdvisor(conf)
    }.getMessage
    assert(msg2.startsWith("Error while instantiating 'non.exists'"))
  }

  test("FileSessionConfAdvisor") {
    val conf = new KyuubiConf(false)
    conf.set(KyuubiConf.SESSION_CONF_ADVISOR, Seq(classOf[FileSessionConfAdvisor].getName))
    val advisor = PluginLoader.loadSessionConfAdvisor(conf)
    val emptyConfig =
      advisor.map(_.getConfOverlay("chris", conf.getAll.asJava).asScala).reduce(_ ++ _).asJava
    assert(emptyConfig.isEmpty)

    conf.set(KyuubiConf.SESSION_CONF_PROFILE, Seq("non.exists"))
    val nonExistsConfig =
      advisor.map(_.getConfOverlay("chris", conf.getAll.asJava).asScala).reduce(_ ++ _).asJava
    assert(nonExistsConfig.isEmpty)

    conf.set(KyuubiConf.SESSION_CONF_PROFILE, Seq("cluster-a"))
    val clusterAConf =
      advisor.map(_.getConfOverlay("chris", conf.getAll.asJava).asScala).reduce(_ ++ _).asJava
    assert(clusterAConf.get("kyuubi.ha.namespace") == "kyuubi-ns-a")
    assert(clusterAConf.get("kyuubi.zk.ha.namespace") == null)
    assert(clusterAConf.size() == 5)

    val clusterAConfFromCache =
      advisor.map(_.getConfOverlay("chris", conf.getAll.asJava).asScala).reduce(_ ++ _).asJava
    assert(clusterAConfFromCache.get("kyuubi.ha.namespace") == "kyuubi-ns-a")
    assert(clusterAConfFromCache.get("kyuubi.zk.ha.namespace") == null)
    assert(clusterAConfFromCache.size() == 5)

    conf.set(KyuubiConf.SESSION_CONF_PROFILE, Seq("cluster-a", "cluster-b"))
    val clusterABConf =
      advisor.map(_.getConfOverlay("chris", conf.getAll.asJava).asScala).reduce(_ ++ _).asJava
    assert(clusterABConf.get("kyuubi.ha.namespace") == "kyuubi-ns-b")
    assert(clusterABConf.get("kyuubi.zk.ha.namespace") == null)
    assert(clusterABConf.get("kyuubi.engineEnv.HIVE_DIR") == "/opt/hive_conf_dir")
    assert(clusterABConf.size() == 6)

    val clusterABConfFromCache =
      advisor.map(_.getConfOverlay("chris", conf.getAll.asJava).asScala).reduce(_ ++ _).asJava
    assert(clusterABConfFromCache.get("kyuubi.ha.namespace") == "kyuubi-ns-b")
    assert(clusterABConfFromCache.get("kyuubi.zk.ha.namespace") == null)
    assert(clusterABConf.get("kyuubi.engineEnv.HIVE_DIR") == "/opt/hive_conf_dir")
    assert(clusterABConfFromCache.size() == 6)
  }

  test("SessionConfAdvisor - multi class") {
    val conf = new KyuubiConf(false)
    conf.set(
      KyuubiConf.SESSION_CONF_ADVISOR,
      Seq(classOf[FileSessionConfAdvisor].getName, classOf[TestSessionConfAdvisor].getName))
    val advisor = PluginLoader.loadSessionConfAdvisor(conf)
    conf.set(KyuubiConf.SESSION_CONF_PROFILE, Seq("cluster-a"))
    val clusterAConf =
      advisor.map(_.getConfOverlay("chris", conf.getAll.asJava).asScala).reduce(_ ++ _).asJava
    assert(clusterAConf.get("kyuubi.ha.namespace") == "kyuubi-ns-a")
    assert(clusterAConf.get("kyuubi.zk.ha.namespace") == null)
    assert(clusterAConf.get("spark.k3") == "v3")
    assert(clusterAConf.size() == 7)
  }

  test("GroupProvider - wrong class") {
    val conf = new KyuubiConf(false)
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")
    assert(PluginLoader.loadGroupProvider(conf).isInstanceOf[HadoopGroupProvider])

    conf.set(KyuubiConf.GROUP_PROVIDER, classOf[HadoopGroupProvider].getName)
    assert(PluginLoader.loadGroupProvider(conf).isInstanceOf[HadoopGroupProvider])

    conf.set(KyuubiConf.GROUP_PROVIDER, classOf[InvalidGroupProvider].getName)
    val msg1 = intercept[KyuubiException] {
      PluginLoader.loadGroupProvider(conf)
    }.getMessage
    assert(msg1.contains(s"is not a child of '${classOf[GroupProvider].getName}'"))

    conf.set(KyuubiConf.GROUP_PROVIDER, "non.exists")
    val msg2 = intercept[IllegalArgumentException] {
      PluginLoader.loadGroupProvider(conf)
    }.getMessage
    assert(msg2.startsWith("Error while instantiating 'non.exists'"))
  }

  test("HadoopGroupProvider") {
    val conf = new KyuubiConf(false)
    conf.set(KyuubiConf.GROUP_PROVIDER, "hadoop")
    val groupProvider = PluginLoader.loadGroupProvider(conf)
    assert(groupProvider.isInstanceOf[HadoopGroupProvider])
    val user = "somebody"
    assert(groupProvider.primaryGroup(user, Map.empty[String, String].asJava) === user)
    assert(groupProvider.groups(user, Map.empty[String, String].asJava) === Array(user))
  }
}

class InvalidSessionConfAdvisor
class InvalidGroupProvider

class TestSessionConfAdvisor extends SessionConfAdvisor {
  override def getConfOverlay(
      user: String,
      sessionConf: java.util.Map[String, String]): java.util.Map[String, String] = {
    Map("spark.k3" -> "v3", "spark.k4" -> "v4").asJava
  }
}
