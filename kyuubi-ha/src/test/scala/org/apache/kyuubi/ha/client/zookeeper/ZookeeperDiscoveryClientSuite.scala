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

package org.apache.kyuubi.ha.client.zookeeper

import java.io.{File, IOException}
import java.net.InetAddress
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import javax.security.auth.login.Configuration

import scala.collection.JavaConverters._

import org.apache.hadoop.util.StringUtils
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{KerberizedTestHelper, KYUUBI_VERSION}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client._
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.zookeeper.ZookeeperClientProvider._
import org.apache.kyuubi.jdbc.hive.strategy.{ChooseServerStrategy, StrategyFactory}
import org.apache.kyuubi.service._
import org.apache.kyuubi.shaded.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.kyuubi.shaded.curator.retry.ExponentialBackoffRetry
import org.apache.kyuubi.shaded.zookeeper.ZooDefs
import org.apache.kyuubi.shaded.zookeeper.data.ACL
import org.apache.kyuubi.util.reflect.ReflectUtils._
import org.apache.kyuubi.zookeeper.EmbeddedZookeeper
import org.apache.kyuubi.zookeeper.ZookeeperConf.ZK_CLIENT_PORT

class EmbeddedZookeeperDiscoveryClientSuite extends ZookeeperDiscoveryClientSuite {

  private var _zkServer: EmbeddedZookeeper = _

  override def getConnectString: String = _zkServer.getConnectString

  override def startZk(): Unit = {
    val embeddedZkConf = KyuubiConf()
    embeddedZkConf.set(ZK_CLIENT_PORT, 0)
    _zkServer = new EmbeddedZookeeper()
    _zkServer.initialize(embeddedZkConf)
    _zkServer.start()
  }

  override def stopZk(): Unit = {
    _zkServer.stop()
  }
}

abstract class ZookeeperDiscoveryClientSuite extends DiscoveryClientTests
  with KerberizedTestHelper {

  var conf: KyuubiConf = KyuubiConf()

  def startZk(): Unit

  def stopZk(): Unit

  override def beforeEach(): Unit = {
    startZk()
    conf = new KyuubiConf().set(HA_ADDRESSES, getConnectString)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    stopZk()
  }

  test("acl for zookeeper") {
    val expectedNoACL = new util.ArrayList[ACL](ZooDefs.Ids.OPEN_ACL_UNSAFE)
    val expectedEnableACL = new util.ArrayList[ACL](ZooDefs.Ids.READ_ACL_UNSAFE)
    expectedEnableACL.addAll(ZooDefs.Ids.CREATOR_ALL_ACL)

    def assertACL(expected: util.List[ACL], actual: util.List[ACL]): Unit = {
      assert(actual.size() == expected.size())
      assert(actual === expected)
    }

    val acl = new ZookeeperACLProvider(conf).getDefaultAcl
    assertACL(expectedNoACL, acl)

    val serverConf = conf.clone.set(HA_ZK_AUTH_TYPE, AuthTypes.KERBEROS.toString)
    val serverACL = new ZookeeperACLProvider(serverConf).getDefaultAcl
    assertACL(expectedEnableACL, serverACL)

    val engineConf = serverConf.clone.set(HA_ENGINE_REF_ID, "ref")
    engineConf.set(HA_ZK_ENGINE_AUTH_TYPE, AuthTypes.NONE.toString)
    val engineACL = new ZookeeperACLProvider(engineConf).getDefaultAcl
    assertACL(expectedNoACL, engineACL)

    val enableEngineACLConf = serverConf.clone.set(HA_ENGINE_REF_ID, "ref")
    enableEngineACLConf.set(HA_ZK_ENGINE_AUTH_TYPE, AuthTypes.KERBEROS.toString)
    val enableEngineACL = new ZookeeperACLProvider(enableEngineACLConf).getDefaultAcl
    assertACL(expectedEnableACL, enableEngineACL)
  }

  test("set up zookeeper auth") {
    tryWithSecurityEnabled {
      val keytab = File.createTempFile("kentyao", ".keytab")
      val principal = "kentyao/_HOST@apache.org"

      conf.set(HA_ZK_AUTH_KEYTAB.key, keytab.getCanonicalPath)
      conf.set(HA_ZK_AUTH_PRINCIPAL.key, principal)
      conf.set(HA_ZK_AUTH_TYPE.key, AuthTypes.KERBEROS.toString)

      setUpZooKeeperAuth(conf)
      val configuration = Configuration.getConfiguration
      val entries = configuration.getAppConfigurationEntry("KyuubiZooKeeperClient")

      assert(entries.head.getLoginModuleName === "com.sun.security.auth.module.Krb5LoginModule")
      val options = entries.head.getOptions.asScala.toMap

      val hostname = StringUtils.toLowerCase(InetAddress.getLocalHost.getCanonicalHostName)
      assert(options("principal") === s"kentyao/$hostname@apache.org")
      assert(options("useKeyTab").toString.toBoolean)

      conf.set(HA_ZK_AUTH_KEYTAB.key, s"${keytab.getName}")
      val e = intercept[IOException](setUpZooKeeperAuth(conf))
      assert(
        e.getMessage === s"${HA_ZK_AUTH_KEYTAB.key}: ${getKeyTabFile(conf).get} does not exists")
    }
  }

  test("stop engine in time while zk ensemble terminates") {
    var discovery: ServiceDiscovery = null
    val service = new NoopTBinaryFrontendServer() {
      override val frontendServices: Seq[NoopTBinaryFrontendService] = Seq(
        new NoopTBinaryFrontendService(this) {
          override val discoveryService: Option[Service] = {
            discovery = new EngineServiceDiscovery(this)
            Some(discovery)
          }
        })
    }
    conf.set(HA_ZK_CONN_RETRY_POLICY, "ONE_TIME")
      .set(HA_ZK_CONN_BASE_RETRY_WAIT, 1)
      .set(HA_ZK_SESSION_TIMEOUT, 2000)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    service.initialize(conf)
    service.start()
    assert(service.getServiceState === ServiceState.STARTED)

    stopZk()
    val isServerLost =
      getField[AtomicBoolean]((discovery.getClass.getSuperclass, discovery), "isServerLost")

    eventually(timeout(10.seconds), interval(100.millis)) {
      assert(isServerLost.get())
      assert(discovery.getServiceState === ServiceState.STOPPED)
      assert(service.getServiceState === ServiceState.STOPPED)
    }
  }

  test("watcher for zookeeper") {
    val namespace = "kyuubiwatcher"
    var discovery: ServiceDiscovery = null
    val service = new NoopTBinaryFrontendServer() {
      override val frontendServices: Seq[NoopTBinaryFrontendService] = Seq(
        new NoopTBinaryFrontendService(this) {
          override val discoveryService: Option[Service] = {
            discovery = new EngineServiceDiscovery(this)
            Some(discovery)
          }
        })
    }

    conf.set(HA_ZK_CONN_RETRY_POLICY, "ONE_TIME")
      .set(HA_ZK_CONN_BASE_RETRY_WAIT, 1)
      .set(HA_ZK_SESSION_TIMEOUT, 2000)
      .set(HA_ADDRESSES, getConnectString)
      .set(HA_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    service.initialize(conf)
    service.start()
    assert(service.getServiceState === ServiceState.STARTED)

    val basePath = s"/$namespace"
    try {
      withDiscoveryClient(conf) { discoveryClient =>
        assert(discoveryClient.pathExists(basePath))
        val children = discoveryClient.getChildren(basePath)
        assert(children.head ===
          s"serverUri=${service.frontendServices.head.connectionUrl};" +
          s"version=$KYUUBI_VERSION;sequence=0000000000")

        children.foreach { child =>
          val childPath = s"""$basePath/$child"""
          val nodeData = discoveryClient.getData(childPath)

          val zkClient = CuratorFrameworkFactory.builder()
            .connectString(getConnectString)
            .sessionTimeoutMs(5000)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build
          zkClient.start()

          // Trigger the NodeDataChanged event
          zkClient.setData().forPath(childPath, nodeData)
          Thread.sleep(3000)
          // Trigger the NodeDeleted event
          zkClient.delete().forPath(childPath)
          zkClient.close()
        }
        eventually(timeout(10.seconds), interval(100.millis)) {
          assert(discovery.getServiceState === ServiceState.STOPPED)
          assert(service.getServiceState === ServiceState.STOPPED)
        }
      }
    } finally {
      service.stop()
      discovery.stop()
    }
  }

  test("strategy for zookeeper") {
    val zkClient = CuratorFrameworkFactory.builder()
      .connectString(getConnectString)
      .sessionTimeoutMs(5000)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .build
    zkClient.start()
    val namespace = "kyuubi-strategy-test"
    val testServerHosts = Seq(
      "testNode1",
      "testNode2",
      "testNode3").toList.asJava
    // test poll strategy
    val pollingChooseStrategy = StrategyFactory.createStrategy("poll")
    assert(pollingChooseStrategy.chooseServer(testServerHosts, zkClient, namespace) === "testNode1")
    assert(pollingChooseStrategy.chooseServer(testServerHosts, zkClient, namespace) === "testNode2")
    assert(pollingChooseStrategy.chooseServer(testServerHosts, zkClient, namespace) === "testNode3")
    // test only get first serverHost strategy
    assert(TestStrategy.chooseServer(testServerHosts, zkClient, namespace) === "testNode1")
    assert(TestStrategy.chooseServer(testServerHosts, zkClient, namespace) === "testNode1")
    assert(TestStrategy.chooseServer(testServerHosts, zkClient, namespace) === "testNode1")
    zkClient.close()
  }

  object TestStrategy extends ChooseServerStrategy {
    override def chooseServer(
        serverHosts: util.List[String],
        zkClient: CuratorFramework,
        namespace: String): String = {
      serverHosts.get(0)
    }
  }
}
