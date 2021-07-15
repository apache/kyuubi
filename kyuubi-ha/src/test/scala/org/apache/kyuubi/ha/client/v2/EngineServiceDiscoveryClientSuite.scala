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

package org.apache.kyuubi.ha.client.v2

import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.nodes.PersistentNode
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.{CreateMode, KeeperException}
import org.apache.zookeeper.CreateMode.PERSISTENT
import org.apache.zookeeper.KeeperException.NodeExistsException

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_NODE_TIMEOUT
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.ha.v2.ServiceDiscoveryConf
import org.apache.kyuubi.ha.v2.engine.{EngineInstance, EngineInstanceSerializer, EngineServiceDiscoveryClient}
import org.apache.kyuubi.ha.v2.engine.strategies.SessionNumStrategy
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

class EngineServiceDiscoveryClientSuite extends KyuubiFunSuite {

  protected val conf: KyuubiConf = KyuubiConf()

  private var zkServer: EmbeddedZookeeper = _

  override def beforeAll(): Unit = {
    zkServer = new EmbeddedZookeeper()
    val zkData = Utils.createTempDir()
    conf.set(ZookeeperConf.ZK_DATA_DIR, zkData.toString)
    zkServer.initialize(conf)
    zkServer.start()
    conf.set(HighAvailabilityConf.HA_ZK_NAMESPACE.key, s"localhost:${ZookeeperConf.ZK_CLIENT_PORT}")
  }

  override def afterAll(): Unit = {
    if (zkServer != null) {
      zkServer.stop()
      zkServer = null
    }
    super.afterAll()
  }

  test("engine discovery client test") {
    val namespace = "/kyuubi_USER/test"
    ServiceDiscovery.withZkClient(conf) { zkClient =>
      val engine1 = EngineInstance(namespace, null, "127.0.0.1", 1111,
        Some("1.2.0"), 2, Seq("SPARK_3", "p0"))
      val path1 = createZkServiceNode(conf, zkClient, engine1).getActualPath

      val engine2 = EngineInstance(namespace, null, "127.0.0.1", 2222,
        Some("1.3.0"), 1, Seq("SPARK_3", "p1"))
      val path2 = createZkServiceNode(conf, zkClient, engine2).getActualPath

      val engine3 = EngineInstance(namespace, null, "127.0.0.1", 3333,
        Some("1.3.0"), 0, Seq("SPARK_3", "p2"))
      val path3 = createZkServiceNode(conf, zkClient, engine3).getActualPath

      // core size
      conf.set(ServiceDiscoveryConf.ENGINE_PROVIDER_CORE_SIZE, 3)
      var instance0 = EngineServiceDiscoveryClient.get(namespace, zkClient, conf).getInstance()
      assert(instance0.isDefined)
      conf.set(ServiceDiscoveryConf.ENGINE_PROVIDER_CORE_SIZE, 4)
      instance0 = EngineServiceDiscoveryClient.get(namespace, zkClient, conf).getInstance()
      assert(instance0.isEmpty)
      conf.unset(ServiceDiscoveryConf.ENGINE_PROVIDER_CORE_SIZE)

      // session number base
      conf.set(ServiceDiscoveryConf.ENGINE_PROVIDER_STRATEGY, SessionNumStrategy.NAME)
      val instance1 = EngineServiceDiscoveryClient.get(namespace, zkClient, conf)
        .getInstance().get
      assert(instance1.instance.equals(engine3.instance))

      // version filter
      conf.set(ServiceDiscoveryConf.ENGINE_PROVIDER_VERSION, "1.2.0")
      val instance2 = EngineServiceDiscoveryClient.get(namespace, zkClient, conf).getInstance().get
      assert(instance2.instance.equals(engine1.instance))

      // tags filter
      conf.unset(ServiceDiscoveryConf.ENGINE_PROVIDER_VERSION)
      conf.set(ServiceDiscoveryConf.ENGINE_PROVIDER_TAGS, "SPARK_3,p1")
      val instance3 = EngineServiceDiscoveryClient.get(namespace, zkClient, conf).getInstance().get
      assert(instance3.instance.equals(engine2.instance))
    }
  }

  def createZkServiceNode(
      conf: KyuubiConf,
      zkClient: CuratorFramework,
      engine: EngineInstance): PersistentNode = {
    val ns = ZKPaths.makePath(null, engine.namespace)
    try {
      zkClient
        .create()
        .creatingParentsIfNeeded()
        .withMode(PERSISTENT)
        .forPath(ns)
    } catch {
      case _: NodeExistsException => // do nothing
      case e: KeeperException =>
        throw new KyuubiException(s"Failed to create namespace '$ns'", e)
    }
    val pathPrefix = ZKPaths.makePath(
      engine.namespace,
      s"serviceUri=${engine.instance};version=${engine.version};sequence=")
    var serviceNode: PersistentNode = null
    try {
      serviceNode = new PersistentNode(
        zkClient,
        CreateMode.EPHEMERAL_SEQUENTIAL,
        false,
        pathPrefix,
        EngineInstanceSerializer.serialize(engine))
      serviceNode.start()
      val znodeTimeout = conf.get(HA_ZK_NODE_TIMEOUT)
      if (!serviceNode.waitForInitialCreate(znodeTimeout, TimeUnit.MILLISECONDS)) {
        throw new KyuubiException(s"Max znode creation wait time $znodeTimeout s exhausted")
      }
      info(s"Created a ${serviceNode.getActualPath} on ZooKeeper for KyuubiServer uri: "
        + engine.instance)
    } catch {
      case e: Exception =>
        if (serviceNode != null) {
          serviceNode.close()
        }
        throw new KyuubiException(
          s"Unable to create a znode for this server instance: ${engine.instance}", e)
    }
    serviceNode
  }

}
