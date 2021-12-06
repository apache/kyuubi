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

package org.apache.kyuubi.ha.client

import java.io.{File, IOException}
import java.net.InetAddress
import java.util
import javax.security.auth.login.Configuration

import scala.collection.JavaConverters._

import org.apache.hadoop.util.StringUtils
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{KerberizedTestHelper, KYUUBI_VERSION}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.service.{NoopThriftBinaryFrontendServer, Serverable, ServiceState}
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

class ServiceDiscoverySuite extends KerberizedTestHelper {
  import ZooKeeperClientProvider._

  val zkServer = new EmbeddedZookeeper()
  val conf: KyuubiConf = KyuubiConf()

  override def beforeAll(): Unit = {
    conf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    zkServer.initialize(conf)
    zkServer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    conf.unset(KyuubiConf.SERVER_KEYTAB)
    conf.unset(KyuubiConf.SERVER_PRINCIPAL)
    conf.unset(HA_ZK_QUORUM)
    zkServer.stop()
    super.afterAll()
  }

  test("publish instance to embedded zookeeper server") {
    val namespace = "kyuubiserver"

    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ZK_QUORUM, zkServer.getConnectString)
      .set(HA_ZK_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    val server: Serverable = new NoopThriftBinaryFrontendServer()
    server.initialize(conf)
    server.start()

    val znodeRoot = s"/$namespace"
    val serviceDiscovery = new KyuubiServiceDiscovery(server.frontendServices.head)
    withZkClient(conf) { framework =>
      try {
        serviceDiscovery.initialize(conf)
        serviceDiscovery.start()

        assert(framework.checkExists().forPath("/abc") === null)
        assert(framework.checkExists().forPath(znodeRoot) !== null)
        val children = framework.getChildren.forPath(znodeRoot).asScala
        assert(children.head ===
          s"serviceUri=${server.frontendServices.head.connectionUrl};" +
          s"version=$KYUUBI_VERSION;sequence=0000000000")

        children.foreach { child =>
          framework.delete().forPath(s"""$znodeRoot/$child""")
        }
        eventually(timeout(5.seconds), interval(1.second)) {
          assert(serviceDiscovery.getServiceState === ServiceState.STOPPED)
          assert(server.getServiceState === ServiceState.STOPPED)
        }
      } finally {
        server.stop()
        serviceDiscovery.stop()
      }
    }
  }

  test("acl for zookeeper") {
    val expectedNoACL = new util.ArrayList[ACL](ZooDefs.Ids.OPEN_ACL_UNSAFE)
    val expectedEnableACL = new util.ArrayList[ACL](ZooDefs.Ids.READ_ACL_UNSAFE)
    expectedEnableACL.addAll(ZooDefs.Ids.CREATOR_ALL_ACL)

    def assertACL(expected: util.List[ACL], actual: util.List[ACL]): Unit = {
      assert(actual.size() == expected.size())
      assert(actual === expected)
    }

    val acl = new ZooKeeperACLProvider(conf).getDefaultAcl
    assertACL(expectedNoACL, acl)

    val serverConf = conf.clone.set(HA_ZK_AUTH_TYPE, ZooKeeperAuthTypes.KERBEROS.toString)
    val serverACL = new ZooKeeperACLProvider(serverConf).getDefaultAcl
    assertACL(expectedEnableACL, serverACL)

    val engineConf = serverConf.clone.set(HA_ZK_ENGINE_REF_ID, "ref")
    engineConf.set(HA_ZK_ENGINE_AUTH_TYPE, ZooKeeperAuthTypes.NONE.toString)
    val engineACL = new ZooKeeperACLProvider(engineConf).getDefaultAcl
    assertACL(expectedNoACL, engineACL)

    val enableEngineACLConf = serverConf.clone.set(HA_ZK_ENGINE_REF_ID, "ref")
    enableEngineACLConf.set(HA_ZK_ENGINE_AUTH_TYPE, ZooKeeperAuthTypes.KERBEROS.toString)
    val enableEngineACL = new ZooKeeperACLProvider(enableEngineACLConf).getDefaultAcl
    assertACL(expectedEnableACL, enableEngineACL)
  }

  test("set up zookeeper auth") {
    tryWithSecurityEnabled {
      val keytab = File.createTempFile("kentyao", ".keytab")
      val principal = "kentyao/_HOST@apache.org"

      conf.set(HA_ZK_AUTH_KEYTAB.key, keytab.getCanonicalPath)
      conf.set(HA_ZK_AUTH_PRINCIPAL.key, principal)
      conf.set(HA_ZK_AUTH_TYPE.key, ZooKeeperAuthTypes.KERBEROS.toString)

      ZooKeeperClientProvider.setUpZooKeeperAuth(conf)
      val configuration = Configuration.getConfiguration
      val entries = configuration.getAppConfigurationEntry("KyuubiZooKeeperClient")

      assert(entries.head.getLoginModuleName === "com.sun.security.auth.module.Krb5LoginModule")
      val options = entries.head.getOptions.asScala.toMap

      val hostname = StringUtils.toLowerCase(InetAddress.getLocalHost.getCanonicalHostName)
      assert(options("principal") === s"kentyao/$hostname@apache.org")
      assert(options("useKeyTab").toString.toBoolean)

      conf.set(HA_ZK_AUTH_KEYTAB.key, s"${keytab.getName}")
      val e = intercept[IOException](ZooKeeperClientProvider.setUpZooKeeperAuth(conf))
      assert(e.getMessage === s"${HA_ZK_AUTH_KEYTAB.key} does not exists")
    }
  }

  test("KYUUBI-304: Stop engine service gracefully when related zk node is deleted") {
    val logAppender = new LogAppender("test stop engine gracefully")
    withLogAppender(logAppender) {
      val namespace = "kyuubiengine"

      conf
        .unset(KyuubiConf.SERVER_KEYTAB)
        .unset(KyuubiConf.SERVER_PRINCIPAL)
        .set(HA_ZK_QUORUM, zkServer.getConnectString)
        .set(HA_ZK_NAMESPACE, namespace)
        .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
        .set(HA_ZK_AUTH_TYPE, ZooKeeperAuthTypes.NONE.toString)

      val server: Serverable = new NoopThriftBinaryFrontendServer()
      server.initialize(conf)
      server.start()

      val znodeRoot = s"/$namespace"
      val serviceDiscovery = new EngineServiceDiscovery(server.frontendServices.head)
      withZkClient(conf) { framework =>
        try {
          serviceDiscovery.initialize(conf)
          serviceDiscovery.start()

          assert(framework.checkExists().forPath("/abc") === null)
          assert(framework.checkExists().forPath(znodeRoot) !== null)
          val children = framework.getChildren.forPath(znodeRoot).asScala
          assert(children.head ===
            s"serviceUri=${server.frontendServices.head.connectionUrl};" +
            s"version=$KYUUBI_VERSION;sequence=0000000000")

          children.foreach { child =>
            framework.delete().forPath(s"""$znodeRoot/$child""")
          }
          eventually(timeout(5.seconds), interval(1.second)) {
            assert(serviceDiscovery.getServiceState === ServiceState.STOPPED)
            assert(server.getServiceState === ServiceState.STOPPED)
            val msg = s"This Kyuubi instance ${server.frontendServices.head.connectionUrl}" +
              s" is now de-registered"
            assert(logAppender.loggingEvents.exists(_.getRenderedMessage.contains(msg)))
          }
        } finally {
          server.stop()
          serviceDiscovery.stop()
        }
      }
    }
  }

  test("parse host and port from instance string") {
    val host = "127.0.0.1"
    val port = 10009
    val instance1 = s"$host:$port"
    val (host1, port1) = ServiceDiscovery.parseInstanceHostPort(instance1)
    assert(host === host1)
    assert(port === port1)

    val instance2 = s"hive.server2.thrift.sasl.qop=auth;hive.server2.thrift.bind.host=$host;" +
      s"hive.server2.transport.mode=binary;hive.server2.authentication=KERBEROS;" +
      s"hive.server2.thrift.port=$port;" +
      s"hive.server2.authentication.kerberos.principal=test/_HOST@apache.org"
    val (host2, port2) = ServiceDiscovery.parseInstanceHostPort(instance2)
    assert(host === host2)
    assert(port === port2)
  }
}
