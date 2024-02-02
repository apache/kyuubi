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

package org.apache.kyuubi.ctl

import java.util.concurrent.atomic.AtomicInteger

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ctl.cli.{ControlCli, ControlCliArguments}
import org.apache.kyuubi.ctl.util.{CtlUtils, Render}
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ADDRESSES, HA_NAMESPACE}
import org.apache.kyuubi.ha.client.{DiscoveryClientProvider, ServiceNodeInfo}
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

class ControlCliSuite extends KyuubiFunSuite with TestPrematureExit {
  import DiscoveryClientProvider._

  val zkServer = new EmbeddedZookeeper()
  val conf: KyuubiConf = KyuubiConf()
  var envZkNamespaceProperty: String = System.getProperty(HA_NAMESPACE.key)
  val namespace = "kyuubiserver"
  val host = "localhost"
  val port = "10000"
  val user = "kyuubi"
  val ctl = new ControlCli()
  val counter = new AtomicInteger(0)

  override def beforeAll(): Unit = {
    setSystemProperty(HA_NAMESPACE.key, namespace)
    conf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    zkServer.initialize(conf)
    zkServer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    setSystemProperty(HA_NAMESPACE.key, envZkNamespaceProperty)
    conf.unset(KyuubiConf.SERVER_KEYTAB)
    conf.unset(KyuubiConf.SERVER_PRINCIPAL)
    conf.unset(HA_ADDRESSES)
    zkServer.stop()
    super.afterAll()
  }

  private def getUniqueNamespace(): String = {
    s"${namespace}_${"%02d".format(counter.getAndIncrement())}"
  }

  /** Get the rendered service node info without title */
  private def getRenderedNodesInfoWithoutTitle(nodesInfo: Seq[ServiceNodeInfo]): String = {
    Render.renderServiceNodesInfo("", nodesInfo)
  }

  test("test expose to same namespace") {
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    val args = Array(
      "create",
      "server",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--host",
      host,
      "--port",
      port)
    testPrematureExitForControlCli(
      args,
      "Only support expose Kyuubi server instance to another domain")
  }

  test("test not specified namespace") {
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    val args2 = Array(
      "create",
      "server",
      "--zk-quorum",
      zkServer.getConnectString,
      "--host",
      host,
      "--port",
      port)
    testPrematureExitForControlCli(
      args2,
      "Only support expose Kyuubi server instance to another domain")
  }

  test("test expose to another namespace") {
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    val args = Array(
      "create",
      "server",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      "other-kyuubi-server",
      "--host",
      host,
      "--port",
      port)
    testPrematureExitForControlCli(args, "")
  }

  test("test render zookeeper service node info") {
    val title = "test render"
    val nodes = Seq(
      ServiceNodeInfo("/kyuubi", "serviceNode", "localhost", 10000, Some("version"), None))
    val renderedInfo = Render.renderServiceNodesInfo(title, nodes)
    // scalastyle:off
    val expected = {
      s"\n                $title                " +
        """
          |╔═══════════╤═══════════╤═══════╤═════════╗
          |║ Namespace │ Host      │ Port  │ Version ║
          |╠═══════════╪═══════════╪═══════╪═════════╣
          |║ /kyuubi   │ localhost │ 10000 │ version ║
          |╚═══════════╧═══════════╧═══════╧═════════╝
          |1 row(s)
          |""".stripMargin
    }
    // scalastyle:on
    assert(renderedInfo == expected)
    assert(renderedInfo.contains(getRenderedNodesInfoWithoutTitle(nodes)))
  }

  test("test expose zk service node to another namespace") {
    val uniqueNamespace = getUniqueNamespace()
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_NAMESPACE, uniqueNamespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    System.setProperty(HA_NAMESPACE.key, uniqueNamespace)

    withDiscoveryClient(conf) { framework =>
      framework.createAndGetServiceNode(conf, uniqueNamespace, "localhost:10000")
      framework.createAndGetServiceNode(conf, uniqueNamespace, "localhost:10001")

      val newNamespace = getUniqueNamespace()
      val args = Array(
        "create",
        "server",
        "--zk-quorum",
        zkServer.getConnectString,
        "--namespace",
        newNamespace)

      val expectedCreatedNodes = Seq(
        ServiceNodeInfo(s"/$newNamespace", "", "localhost", 10000, Some(KYUUBI_VERSION), None),
        ServiceNodeInfo(s"/$newNamespace", "", "localhost", 10001, Some(KYUUBI_VERSION), None))

      testPrematureExitForControlCli(
        args,
        getRenderedNodesInfoWithoutTitle(expectedCreatedNodes))
      val znodeRoot = s"/$newNamespace"
      val children = framework.getChildren(znodeRoot).sorted
      assert(children.size == 2)

      assert(children.head.startsWith(
        s"serverUri=localhost:10000;version=$KYUUBI_VERSION;sequence="))
      assert(children.last.startsWith(
        s"serverUri=localhost:10001;version=$KYUUBI_VERSION;sequence="))
      children.foreach { child =>
        framework.delete(s"""$znodeRoot/$child""")
      }
    }
  }

  test("test get zk server namespace") {
    val args = Array(
      "list",
      "server",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace)
    val scArgs = new ControlCliArguments(args)
    assert(
      CtlUtils.getZkServerNamespace(
        scArgs.command.conf,
        scArgs.command.normalizedCliConfig) === s"/$namespace")
  }

  test("test get zk engine namespace") {
    val args = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--user",
      user)
    val scArgs = new ControlCliArguments(args)
    val expected = (s"/${namespace}_${KYUUBI_VERSION}_USER_SPARK_SQL/$user", None)
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs.command.conf,
      scArgs.command.normalizedCliConfig) === expected)
  }

  test("test list zk service nodes info") {
    val uniqueNamespace = getUniqueNamespace()
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_NAMESPACE, uniqueNamespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    withDiscoveryClient(conf) { framework =>
      framework.createAndGetServiceNode(conf, uniqueNamespace, "localhost:10000")
      framework.createAndGetServiceNode(conf, uniqueNamespace, "localhost:10001")

      val args = Array(
        "list",
        "server",
        "--zk-quorum",
        zkServer.getConnectString,
        "--namespace",
        uniqueNamespace)

      val expectedNodes = Seq(
        ServiceNodeInfo(s"/$uniqueNamespace", "", "localhost", 10000, Some(KYUUBI_VERSION), None),
        ServiceNodeInfo(s"/$uniqueNamespace", "", "localhost", 10001, Some(KYUUBI_VERSION), None))

      testPrematureExitForControlCli(args, getRenderedNodesInfoWithoutTitle(expectedNodes))
    }
  }

  test("test get zk service nodes info") {
    val uniqueNamespace = getUniqueNamespace()
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_NAMESPACE, uniqueNamespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    withDiscoveryClient(conf) { framework =>
      framework.createAndGetServiceNode(conf, uniqueNamespace, "localhost:10000")
      framework.createAndGetServiceNode(conf, uniqueNamespace, "localhost:10001")

      val args = Array(
        "get",
        "server",
        "--zk-quorum",
        zkServer.getConnectString,
        "--namespace",
        uniqueNamespace,
        "--host",
        "localhost",
        "--port",
        "10000")

      val expectedNodes = Seq(
        ServiceNodeInfo(s"/$uniqueNamespace", "", "localhost", 10000, Some(KYUUBI_VERSION), None))

      testPrematureExitForControlCli(args, getRenderedNodesInfoWithoutTitle(expectedNodes))
    }
  }

  test("test delete zk service nodes info") {
    val uniqueNamespace = getUniqueNamespace()
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_NAMESPACE, uniqueNamespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    withDiscoveryClient(conf) { framework =>
      withDiscoveryClient(conf) { zc =>
        framework.createAndGetServiceNode(conf, uniqueNamespace, "localhost:10000", external = true)
        framework.createAndGetServiceNode(conf, uniqueNamespace, "localhost:10001", external = true)
      }

      val args = Array(
        "delete",
        "server",
        "--zk-quorum",
        zkServer.getConnectString,
        "--namespace",
        uniqueNamespace,
        "--host",
        "localhost",
        "--port",
        "10000")

      val expectedDeletedNodes = Seq(
        ServiceNodeInfo(s"/$uniqueNamespace", "", "localhost", 10000, Some(KYUUBI_VERSION), None))

      testPrematureExitForControlCli(
        args,
        getRenderedNodesInfoWithoutTitle(expectedDeletedNodes))
    }
  }

  test("test verbose output") {
    val uniqueNamespace = getUniqueNamespace()
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ADDRESSES, zkServer.getConnectString)
      .set(HA_NAMESPACE, uniqueNamespace)
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)

    withDiscoveryClient(conf) { framework =>
      framework.createAndGetServiceNode(conf, uniqueNamespace, "localhost:10000")
      framework.createAndGetServiceNode(conf, uniqueNamespace, "localhost:10001")

      val args = Array(
        "list",
        "server",
        "--zk-quorum",
        zkServer.getConnectString,
        "--namespace",
        uniqueNamespace,
        "--verbose")

      val expectedNodes = Seq(
        ServiceNodeInfo(s"/$uniqueNamespace", "", "localhost", 10000, Some(KYUUBI_VERSION), None),
        ServiceNodeInfo(s"/$uniqueNamespace", "", "localhost", 10001, Some(KYUUBI_VERSION), None))

      testPrematureExitForControlCli(args, getRenderedNodesInfoWithoutTitle(expectedNodes))
    }
  }

  test("test get zk namespace for different engine type and subdomain") {
    val arg1 = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--user",
      user)
    val scArgs1 = new ControlCliArguments(arg1)
    val expected1 = (s"/${namespace}_${KYUUBI_VERSION}_USER_SPARK_SQL/$user", None)
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs1.command.conf,
      scArgs1.command.normalizedCliConfig) === expected1)

    val arg2 = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--user",
      user,
      "--engine-type",
      "FLINK_SQL")
    val scArgs2 = new ControlCliArguments(arg2)
    val expected2 = (s"/${namespace}_${KYUUBI_VERSION}_USER_FLINK_SQL/$user", None)
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs2.command.conf,
      scArgs2.command.normalizedCliConfig) === expected2)

    val arg3 = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--user",
      user,
      "--engine-type",
      "TRINO")
    val scArgs3 = new ControlCliArguments(arg3)
    val expected3 = (s"/${namespace}_${KYUUBI_VERSION}_USER_TRINO/$user", None)
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs3.command.conf,
      scArgs3.command.normalizedCliConfig) === expected3)

    val arg4 = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--user",
      user,
      "--engine-type",
      "SPARK_SQL",
      "--engine-subdomain",
      "sub_1")
    val scArgs4 = new ControlCliArguments(arg4)
    val expected4 = (s"/${namespace}_${KYUUBI_VERSION}_USER_SPARK_SQL/$user", Some("sub_1"))
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs4.command.conf,
      scArgs4.command.normalizedCliConfig) === expected4)

    val arg5 = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--version",
      "1.5.0",
      "--user",
      user,
      "--engine-type",
      "SPARK_SQL",
      "--engine-subdomain",
      "sub_1")
    val scArgs5 = new ControlCliArguments(arg5)
    val expected5 = (s"/${namespace}_1.5.0_USER_SPARK_SQL/$user", Some("sub_1"))
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs5.command.conf,
      scArgs5.command.normalizedCliConfig) === expected5)
  }

  test("test get zk namespace for different share level engines") {
    val arg1 = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--user",
      user)
    val scArgs1 = new ControlCliArguments(arg1)
    val expected1 = (s"/${namespace}_${KYUUBI_VERSION}_USER_SPARK_SQL/$user", None)
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs1.command.conf,
      scArgs1.command.normalizedCliConfig) === expected1)

    val arg2 = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--user",
      user,
      "--engine-share-level",
      "CONNECTION")
    val scArgs2 = new ControlCliArguments(arg2)
    val expected2 = (s"/${namespace}_${KYUUBI_VERSION}_CONNECTION_SPARK_SQL/$user", None)
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs2.command.conf,
      scArgs2.command.normalizedCliConfig) === expected2)

    val arg3 = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--user",
      user,
      "--engine-share-level",
      "USER")
    val scArgs3 = new ControlCliArguments(arg3)
    val expected3 = (s"/${namespace}_${KYUUBI_VERSION}_USER_SPARK_SQL/$user", None)
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs3.command.conf,
      scArgs3.command.normalizedCliConfig) === expected3)

    val arg4 = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--user",
      user,
      "--engine-share-level",
      "GROUP")
    val scArgs4 = new ControlCliArguments(arg4)
    val expected4 = (s"/${namespace}_${KYUUBI_VERSION}_GROUP_SPARK_SQL/$user", None)
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs4.command.conf,
      scArgs4.command.normalizedCliConfig) === expected4)

    val arg5 = Array(
      "list",
      "engine",
      "--zk-quorum",
      zkServer.getConnectString,
      "--namespace",
      namespace,
      "--user",
      user,
      "--engine-share-level",
      "SERVER")
    val scArgs5 = new ControlCliArguments(arg5)
    val expected5 = (s"/${namespace}_${KYUUBI_VERSION}_SERVER_SPARK_SQL/$user", None)
    assert(CtlUtils.getZkEngineNamespaceAndSubdomain(
      scArgs5.command.conf,
      scArgs5.command.normalizedCliConfig) === expected5)
  }
}
