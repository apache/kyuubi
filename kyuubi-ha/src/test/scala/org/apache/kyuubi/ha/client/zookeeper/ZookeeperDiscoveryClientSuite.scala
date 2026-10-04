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
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.TimeUnit
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
import org.apache.kyuubi.jdbc.hive.strategy.{ServerSelectStrategy, ServerSelectStrategyFactory}
import org.apache.kyuubi.service._
import org.apache.kyuubi.shaded.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.kyuubi.shaded.curator.framework.recipes.nodes.PersistentNode
import org.apache.kyuubi.shaded.curator.framework.state.ConnectionState
import org.apache.kyuubi.shaded.curator.retry.ExponentialBackoffRetry
import org.apache.kyuubi.shaded.zookeeper.{CreateMode, ZooDefs}
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
  var loginConf: javax.security.auth.login.Configuration = _

  def startZk(): Unit

  def stopZk(): Unit

  private def setField(target: AnyRef, fieldName: String, value: AnyRef): Unit = {
    val field = target.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.set(target, value)
  }

  override def beforeEach(): Unit = {
    startZk()
    conf = new KyuubiConf().set(HA_ADDRESSES, getConnectString)
    loginConf = javax.security.auth.login.Configuration.getConfiguration
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    javax.security.auth.login.Configuration.setConfiguration(loginConf)
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
      val keytabPath = getKeyTabFile(conf, HA_ZK_AUTH_KEYTAB).get
      assert(e.getMessage === s"${HA_ZK_AUTH_KEYTAB.key}: ${keytabPath} does not exists")
    }
  }

  test("set up zookeeper auth for engine") {
    tryWithSecurityEnabled {
      val keytab = File.createTempFile("engine", ".keytab")
      val principal = "engine/_HOST@apache.org"

      conf.set(HA_ZK_ENGINE_AUTH_KEYTAB.key, keytab.getCanonicalPath)
      conf.set(HA_ZK_ENGINE_AUTH_PRINCIPAL.key, principal)
      conf.set(HA_ENGINE_REF_ID, "ref")
      conf.set(HA_ZK_ENGINE_AUTH_TYPE.key, AuthTypes.KERBEROS.toString)

      setUpZooKeeperAuth(conf)
      val configuration = Configuration.getConfiguration
      val entries = configuration.getAppConfigurationEntry("KyuubiZooKeeperClient")

      assert(entries.head.getLoginModuleName === "com.sun.security.auth.module.Krb5LoginModule")
      val options = entries.head.getOptions.asScala.toMap

      val hostname = StringUtils.toLowerCase(InetAddress.getLocalHost.getCanonicalHostName)
      assert(options("principal") === s"engine/$hostname@apache.org")
      assert(options("useKeyTab").toString.toBoolean)

      conf.set(HA_ZK_ENGINE_AUTH_KEYTAB.key, s"${keytab.getName}")
      val e = intercept[IOException](setUpZooKeeperAuth(conf))
      val keytabPath = getKeyTabFile(conf, HA_ZK_ENGINE_AUTH_KEYTAB).get
      assert(e.getMessage === s"${HA_ZK_ENGINE_AUTH_KEYTAB.key}: ${keytabPath} does not exists")
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

  test("server select strategy with zookeeper") {
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
      "testNode3").asJava
    // test polling strategy
    val pollingStrategy = ServerSelectStrategyFactory.createStrategy("polling")
    1 to testServerHosts.size() * 2 foreach { _ =>
      assertResult(f"testNode1")(pollingStrategy.chooseServer(testServerHosts, zkClient, namespace))
      assertResult(f"testNode2")(pollingStrategy.chooseServer(testServerHosts, zkClient, namespace))
      assertResult(f"testNode3")(pollingStrategy.chooseServer(testServerHosts, zkClient, namespace))
    }

    // test only get first serverHost strategy
    val customStrategy = new ServerSelectStrategy {
      override def chooseServer(
          serverHosts: util.List[String],
          zkClient: CuratorFramework,
          namespace: String): String = serverHosts.get(0)
    }
    1 to testServerHosts.size() * 2 foreach { _ =>
      assertResult("testNode1") {
        customStrategy.chooseServer(testServerHosts, zkClient, namespace)
      }
    }

    zkClient.close()
  }

  test("rewatch service node after zookeeper reconnect") {
    val namespace = "kyuubirewatch"
    conf.set(HA_ZK_CONN_RETRY_POLICY, "ONE_TIME")
      .set(HA_ZK_CONN_BASE_RETRY_WAIT, 1)
      .set(HA_ZK_SESSION_TIMEOUT, 2000)
      .set(HA_ADDRESSES, getConnectString)
      .set(HA_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    val stopped = new AtomicBoolean(false)
    val service = new NoopTBinaryFrontendServer()
    val discovery = new ServiceDiscovery("test discovery", service.frontendServices.head) {
      override def stopGracefully(isLost: Boolean = false): Unit = {
        stopped.set(true)
      }
    }
    val zkDiscoveryClient = new ZookeeperDiscoveryClient(conf)
    val basePath = s"/$namespace"
    var serviceNode: PersistentNode = null

    try {
      zkDiscoveryClient.createClient()
      val zkClient =
        getField[CuratorFramework](zkDiscoveryClient, "zkClient")
      zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(basePath)

      val instance = "localhost:10009"
      serviceNode = new PersistentNode(
        zkClient,
        CreateMode.EPHEMERAL_SEQUENTIAL,
        false,
        s"$basePath/serviceUri=$instance;version=$KYUUBI_VERSION;sequence=",
        instance.getBytes(StandardCharsets.UTF_8))
      serviceNode.start()
      assert(serviceNode.waitForInitialCreate(conf.get(HA_ZK_NODE_TIMEOUT), TimeUnit.MILLISECONDS))

      val watcher = new zkDiscoveryClient.DeRegisterWatcher(instance, discovery)
      setField(zkDiscoveryClient, "serviceNode", serviceNode)
      setField(zkDiscoveryClient, "watcher", watcher)
      assert(zkDiscoveryClient.rewatchServiceNodeOnce("test-reconnect"))

      withDiscoveryClient(conf) { discoveryClient =>
        assert(discoveryClient.pathExists(serviceNode.getActualPath))
        discoveryClient.delete(serviceNode.getActualPath)

        eventually(timeout(10.seconds), interval(100.millis)) {
          assert(stopped.get())
        }
      }
    } finally {
      if (serviceNode != null) {
        try {
          serviceNode.close()
        } catch {
          case _: Exception =>
        }
      }
      zkDiscoveryClient.closeClient()
    }
  }

  // Drive a real RECONNECTED event through the ConnectionStateListener registered by the
  // production monitorState path and prove the engine-side DeRegisterWatcher is rearmed so a
  // post-reconnect delete triggers stop. Retain the registered listener instead of enumerating
  // Curator's internal listener container, whose implementation differs across Curator versions.
  test("real RECONNECTED rearms DeRegisterWatcher so delete triggers stop") {
    val namespace = "kyuubireconnect"
    val reconnectConf = new KyuubiConf()
      .set(HA_ZK_CONN_RETRY_POLICY, "ONE_TIME")
      .set(HA_ZK_CONN_BASE_RETRY_WAIT, 1)
      .set(HA_ZK_SESSION_TIMEOUT, 2000)
      .set(HA_ADDRESSES, getConnectString)
      .set(HA_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    val stopped = new AtomicBoolean(false)
    val service = new NoopTBinaryFrontendServer()
    val discovery = new ServiceDiscovery("test discovery", service.frontendServices.head) {
      override def stopGracefully(isLost: Boolean = false): Unit = {
        stopped.set(true)
      }
    }
    val zkDiscoveryClient = new ZookeeperDiscoveryClient(reconnectConf)
    val basePath = s"/$namespace"
    var serviceNode: PersistentNode = null

    try {
      zkDiscoveryClient.createClient()
      val zkClient = getField[CuratorFramework](zkDiscoveryClient, "zkClient")
      val connectionStateListener =
        zkDiscoveryClient.registerConnectionStateListener(discovery)
      zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(basePath)

      val instance = "localhost:10010"
      serviceNode = new PersistentNode(
        zkClient,
        CreateMode.EPHEMERAL_SEQUENTIAL,
        false,
        s"$basePath/serviceUri=$instance;version=$KYUUBI_VERSION;sequence=",
        instance.getBytes(StandardCharsets.UTF_8))
      serviceNode.start()
      assert(serviceNode.waitForInitialCreate(
        reconnectConf.get(HA_ZK_NODE_TIMEOUT),
        TimeUnit.MILLISECONDS))

      val watcher = new zkDiscoveryClient.DeRegisterWatcher(instance, discovery)
      setField(zkDiscoveryClient, "serviceNode", serviceNode)
      setField(zkDiscoveryClient, "watcher", watcher)
      // NOTE: no initial watch is armed here. This models a session reconnect where the previous
      // one-shot watch from registerService is gone (ZooKeeper drops a disconnected session's
      // watches without delivering an event). On the old code RECONNECTED only sets isConnected, so
      // the watcher stays unarmed; on the fixed code RECONNECTED schedules the rearm.

      // Dispatch RECONNECTED through the listener implementation used by monitorState. On the
      // fixed code this rearms the DeRegisterWatcher off the connection-state dispatch thread; on
      // the old code it does nothing.
      connectionStateListener.stateChanged(zkClient, ConnectionState.RECONNECTED)
      // give the async rewatch executor a moment to rearm
      Thread.sleep(1000)

      // Now delete the live service node. The rearmed watcher must observe NodeDeleted -> stop.
      // On the old code (no rearm on RECONNECTED) this delete is missed and stopped stays false.
      withDiscoveryClient(reconnectConf) { discoveryClient =>
        assert(discoveryClient.pathExists(serviceNode.getActualPath))
        discoveryClient.delete(serviceNode.getActualPath)
      }
      eventually(timeout(10.seconds), interval(100.millis)) {
        assert(stopped.get(), "stop not triggered after RECONNECTED-delete sequence")
      }
    } finally {
      if (serviceNode != null) {
        try serviceNode.close()
        catch { case _: Exception => }
      }
      zkDiscoveryClient.closeClient()
    }
  }

  // Absent path -> PersistentNode recreate (NodeCreated) ->
  // DeRegisterWatcher rearms on NodeCreated -> next delete is observed.
  test("absent node then NodeCreated rearms watcher so delete is observed") {
    val namespace = "kyuubirearm"
    conf.set(HA_ZK_CONN_RETRY_POLICY, "ONE_TIME")
      .set(HA_ZK_CONN_BASE_RETRY_WAIT, 1)
      .set(HA_ZK_SESSION_TIMEOUT, 2000)
      .set(HA_ADDRESSES, getConnectString)
      .set(HA_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    val stopped = new AtomicBoolean(false)
    val service = new NoopTBinaryFrontendServer()
    val discovery = new ServiceDiscovery("test discovery", service.frontendServices.head) {
      override def stopGracefully(isLost: Boolean = false): Unit = {
        stopped.set(true)
      }
    }
    val zkDiscoveryClient = new ZookeeperDiscoveryClient(conf)
    val basePath = s"/$namespace"
    var serviceNode: PersistentNode = null

    try {
      zkDiscoveryClient.createClient()
      val zkClient = getField[CuratorFramework](zkDiscoveryClient, "zkClient")
      zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(basePath)

      val instance = "localhost:10011"
      serviceNode = new PersistentNode(
        zkClient,
        CreateMode.EPHEMERAL_SEQUENTIAL,
        false,
        s"$basePath/serviceUri=$instance;version=$KYUUBI_VERSION;sequence=",
        instance.getBytes(StandardCharsets.UTF_8))
      serviceNode.start()
      assert(serviceNode.waitForInitialCreate(conf.get(HA_ZK_NODE_TIMEOUT), TimeUnit.MILLISECONDS))

      val watcher = new zkDiscoveryClient.DeRegisterWatcher(instance, discovery)
      setField(zkDiscoveryClient, "serviceNode", serviceNode)
      setField(zkDiscoveryClient, "watcher", watcher)

      // Capture the actual path, then close the PersistentNode. close() drops the ephemeral node
      // and clears nodePath (getActualPath -> null); PersistentNode's auto-recreate stops, giving a
      // deterministic absent path. Re-inject livePath into the nodePath AtomicReference so the
      // production rewatch helper still resolves the path to re-arm on.
      val livePath = serviceNode.getActualPath
      serviceNode.close()
      val nodePathField = classOf[PersistentNode].getDeclaredField("nodePath")
      nodePathField.setAccessible(true)
      val nodePathRef =
        nodePathField.get(serviceNode).asInstanceOf[
          java.util.concurrent.atomic.AtomicReference[String]]
      nodePathRef.set(livePath)
      assert(serviceNode.getActualPath === livePath)

      // rewatch on the absent path leaves an exists watch that fires on NodeCreated; returns false
      assert(
        zkDiscoveryClient.rewatchServiceNodeOnce("absent path rewatch") === false,
        "rewatch on an absent path must return false but leave an exists watch")

      // Create the node on the SAME path via an external client; ZK emits NodeCreated on the
      // exists watch, and the DeRegisterWatcher's NodeCreated branch must re-arm itself.
      withDiscoveryClient(conf) { discoveryClient =>
        discoveryClient.create(livePath, "PERSISTENT")
        assert(discoveryClient.pathExists(livePath))
      }
      // give the NodeCreated watch a moment to rearm via the DeRegisterWatcher NodeCreated branch
      Thread.sleep(1500)

      // now delete the recreated node; the rearmed watcher must observe NodeDeleted -> stop
      withDiscoveryClient(conf) { discoveryClient =>
        assert(discoveryClient.pathExists(livePath))
        discoveryClient.delete(livePath)
      }
      eventually(timeout(10.seconds), interval(100.millis)) {
        assert(stopped.get(), "delete after NodeCreated rearm was not observed")
      }
    } finally {
      if (serviceNode != null) {
        try serviceNode.close()
        catch { case _: Exception => }
      }
      zkDiscoveryClient.closeClient()
    }
  }

  // After deregisterService, a pending or subsequent rewatch
  // must not rearm the watcher on the torn-down node.
  test("deregister cancels pending rewatch and does not rearm watcher") {
    val namespace = "kyuubideregister"
    conf.set(HA_ZK_CONN_RETRY_POLICY, "ONE_TIME")
      .set(HA_ZK_CONN_BASE_RETRY_WAIT, 1)
      .set(HA_ZK_SESSION_TIMEOUT, 2000)
      .set(HA_ADDRESSES, getConnectString)
      .set(HA_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    val stopped = new AtomicBoolean(false)
    val service = new NoopTBinaryFrontendServer()
    val discovery = new ServiceDiscovery("test discovery", service.frontendServices.head) {
      override def stopGracefully(isLost: Boolean = false): Unit = {
        stopped.set(true)
      }
    }
    val zkDiscoveryClient = new ZookeeperDiscoveryClient(conf)
    val basePath = s"/$namespace"
    var serviceNode: PersistentNode = null

    try {
      zkDiscoveryClient.createClient()
      val zkClient = getField[CuratorFramework](zkDiscoveryClient, "zkClient")
      zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(basePath)

      val instance = "localhost:10012"
      serviceNode = new PersistentNode(
        zkClient,
        CreateMode.EPHEMERAL_SEQUENTIAL,
        false,
        s"$basePath/serviceUri=$instance;version=$KYUUBI_VERSION;sequence=",
        instance.getBytes(StandardCharsets.UTF_8))
      serviceNode.start()
      assert(serviceNode.waitForInitialCreate(conf.get(HA_ZK_NODE_TIMEOUT), TimeUnit.MILLISECONDS))

      val watcher = new zkDiscoveryClient.DeRegisterWatcher(instance, discovery)
      setField(zkDiscoveryClient, "serviceNode", serviceNode)
      setField(zkDiscoveryClient, "watcher", watcher)
      // Arm a pending rewatch WITHOUT arming the actual DeRegisterWatcher yet, so deregister's
      // node close does not fire NodeDeleted->stop. Dispatch RECONNECTED through the production
      // monitorState listener to schedule the rearm on the dedicated rewatch executor.
      val connectionStateListener =
        zkDiscoveryClient.registerConnectionStateListener(discovery)
      val livePath = serviceNode.getActualPath
      // drop the live znode WITHOUT a watch armed, so no NodeDeleted fires
      serviceNode.close()
      // now dispatch RECONNECTED; the production path schedules a rearm attempt that should find
      // the path absent and start retrying. deregisterService() below must cancel that retry.
      connectionStateListener.stateChanged(zkClient, ConnectionState.RECONNECTED)
      Thread.sleep(300)

      // Deregister tears down the node and must cancel the pending rewatch retry.
      zkDiscoveryClient.deregisterService()
      // serviceNode is now null; a subsequent direct rewatch attempt must be a no-op.
      assert(!zkDiscoveryClient.rewatchServiceNodeOnce("after deregister"))

      // Recreate the node on the same path; the cancelled/stale rewatch must NOT have rearmed a
      // watcher, so this delete must not trigger stop.
      withDiscoveryClient(conf) { discoveryClient =>
        discoveryClient.create(livePath, "PERSISTENT")
        discoveryClient.delete(livePath)
      }
      Thread.sleep(2000)
      assert(!stopped.get(), "stale rewatch rearmed a torn-down node")
    } finally {
      if (serviceNode != null) {
        try serviceNode.close()
        catch { case _: Exception => }
      }
      zkDiscoveryClient.closeClient()
    }
  }

  // Binary and HTTP discovery each own their own
  // ZookeeperDiscoveryClient; their rewatch executors and serviceNodes are independent, so a rearm
  // on one client must not arm or unarm the other client's watcher.
  test("binary and HTTP discovery clients rearm independently") {
    val namespace = "kyuubiindependent"
    conf.set(HA_ZK_CONN_RETRY_POLICY, "ONE_TIME")
      .set(HA_ZK_CONN_BASE_RETRY_WAIT, 1)
      .set(HA_ZK_SESSION_TIMEOUT, 2000)
      .set(HA_ADDRESSES, getConnectString)
      .set(HA_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    val stoppedBinary = new AtomicBoolean(false)
    val stoppedHttp = new AtomicBoolean(false)
    val service = new NoopTBinaryFrontendServer()
    val discoveryBinary =
      new ServiceDiscovery("binary discovery", service.frontendServices.head) {
        override def stopGracefully(isLost: Boolean = false): Unit = stoppedBinary.set(true)
      }
    val discoveryHttp =
      new ServiceDiscovery("http discovery", service.frontendServices.head) {
        override def stopGracefully(isLost: Boolean = false): Unit = stoppedHttp.set(true)
      }
    val binaryClient = new ZookeeperDiscoveryClient(conf)
    val httpClient = new ZookeeperDiscoveryClient(conf)
    val basePath = s"/$namespace"
    var binaryNode: PersistentNode = null
    var httpNode: PersistentNode = null

    try {
      binaryClient.createClient()
      httpClient.createClient()
      val binZkClient = getField[CuratorFramework](binaryClient, "zkClient")
      val httpZkClient = getField[CuratorFramework](httpClient, "zkClient")
      binZkClient.create().creatingParentsIfNeeded().withMode(
        CreateMode.PERSISTENT).forPath(basePath)

      val binaryInstance = "localhost:10013"
      val httpInstance = "localhost:10014"
      binaryNode = new PersistentNode(
        binZkClient,
        CreateMode.EPHEMERAL_SEQUENTIAL,
        false,
        s"$basePath/serviceUri=$binaryInstance;version=$KYUUBI_VERSION;sequence=",
        binaryInstance.getBytes(StandardCharsets.UTF_8))
      httpNode = new PersistentNode(
        httpZkClient,
        CreateMode.EPHEMERAL_SEQUENTIAL,
        false,
        s"$basePath/serviceUri=$httpInstance;version=$KYUUBI_VERSION;sequence=",
        httpInstance.getBytes(StandardCharsets.UTF_8))
      binaryNode.start()
      httpNode.start()
      assert(binaryNode.waitForInitialCreate(conf.get(HA_ZK_NODE_TIMEOUT), TimeUnit.MILLISECONDS))
      assert(httpNode.waitForInitialCreate(conf.get(HA_ZK_NODE_TIMEOUT), TimeUnit.MILLISECONDS))

      setField(binaryClient, "serviceNode", binaryNode)
      setField(
        binaryClient,
        "watcher",
        new binaryClient.DeRegisterWatcher(binaryInstance, discoveryBinary))
      setField(httpClient, "serviceNode", httpNode)
      setField(httpClient, "watcher", new httpClient.DeRegisterWatcher(httpInstance, discoveryHttp))
      assert(binaryClient.rewatchServiceNodeOnce("binary initial"))
      assert(httpClient.rewatchServiceNodeOnce("http initial"))

      // Delete the binary node only; the HTTP watcher must remain armed and unaffected.
      withDiscoveryClient(conf) { discoveryClient =>
        assert(discoveryClient.pathExists(binaryNode.getActualPath))
        discoveryClient.delete(binaryNode.getActualPath)
      }
      eventually(timeout(10.seconds), interval(100.millis)) {
        assert(stoppedBinary.get())
      }
      // HTTP client was not deleted and must not have stopped.
      assert(!stoppedHttp.get(), "independent HTTP client stopped on binary delete")

      // Now delete the HTTP node; its own (still armed) watcher must fire.
      withDiscoveryClient(conf) { discoveryClient =>
        assert(discoveryClient.pathExists(httpNode.getActualPath))
        discoveryClient.delete(httpNode.getActualPath)
      }
      eventually(timeout(10.seconds), interval(100.millis)) {
        assert(stoppedHttp.get())
      }
    } finally {
      if (binaryNode != null) {
        try binaryNode.close()
        catch { case _: Exception => }
      }
      if (httpNode != null) {
        try httpNode.close()
        catch { case _: Exception => }
      }
      binaryClient.closeClient()
      httpClient.closeClient()
    }
  }
}
