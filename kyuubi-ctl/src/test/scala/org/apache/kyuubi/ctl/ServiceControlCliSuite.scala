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

import java.io.{OutputStream, PrintStream}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ZK_NAMESPACE, HA_ZK_QUORUM}
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

trait TestPrematureExit {
  suite: KyuubiFunSuite =>

  private val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  /** Simple PrintStream that reads data into a buffer */
  private class BufferPrintStream extends PrintStream(noOpOutputStream) {
    var lineBuffer = ArrayBuffer[String]()
    // scalastyle:off println
    override def println(line: Any): Unit = {
      lineBuffer += line.toString
    }
    // scalastyle:on println
  }

  /** Returns true if the script exits and the given search string is printed. */
  private[kyuubi] def testPrematureExit(
      input: Array[String],
      searchString: String,
      mainObject: CommandLineUtils = ServiceControlCli) : Unit = {
    val printStream = new BufferPrintStream()
    mainObject.printStream = printStream

    @volatile var exitedCleanly = false
    val original = mainObject.exitFn
    mainObject.exitFn = (_) => exitedCleanly = true
    try {
      @volatile var exception: Exception = null
      val thread = new Thread {
        override def run() = try {
          mainObject.main(input)
        } catch {
          // Capture the exception to check whether the exception contains searchString or not
          case e: Exception => exception = e
        }
      }
      thread.start()
      thread.join()
      if (exitedCleanly) {
        val joined = printStream.lineBuffer.mkString("\n")
        assert(joined.contains(searchString))
      } else {
        assert(exception != null)
        if (!exception.getMessage.contains(searchString)) {
          throw exception
        }
      }
    } finally {
      mainObject.exitFn = original
    }
  }
}

class ServiceControlCliSuite extends KyuubiFunSuite with TestPrematureExit {
  import ServiceControlCli._
  import ServiceDiscovery._

  val zkServer = new EmbeddedZookeeper()
  val conf: KyuubiConf = KyuubiConf()
  var envZkNamespaceProperty: String = System.getProperty(HA_ZK_NAMESPACE.key)
  val namespace = "kyuubiserver"
  val host = "localhost"
  val port = "10000"
  val user = "kyuubi"
  val ctl = new ServiceControlCli()
  val counter = new AtomicInteger(0)

  override def beforeAll(): Unit = {
    setSystemProperty(HA_ZK_NAMESPACE.key, namespace)
    conf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    zkServer.initialize(conf)
    zkServer.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    setSystemProperty(HA_ZK_NAMESPACE.key, envZkNamespaceProperty)
    conf.unset(KyuubiConf.SERVER_KEYTAB)
    conf.unset(KyuubiConf.SERVER_PRINCIPAL)
    conf.unset(HA_ZK_QUORUM)
    zkServer.stop()
    super.afterAll()
  }

  private def getUniqueNamespace(): String = {
    s"${namespace}_${counter.getAndIncrement()}"
  }

  test("test help") {
    val args = Array("--help")
    testPrematureExit(args, "Usage: kyuubi-ctl")
  }

  test("test expose to same namespace or not specified namespace") {
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ZK_QUORUM, zkServer.getConnectString)
      .set(HA_ZK_NAMESPACE, namespace)
      .set(KyuubiConf.FRONTEND_BIND_PORT, 0)

    val args = Array(
      "create", "server",
      "--zk-quorum", zkServer.getConnectString,
      "--namespace", namespace,
      "--host", host,
      "--port", port
    )
    testPrematureExit(args, "Only support expose Kyuubi server instance to another domain")

    val args2 = Array(
      "create", "server",
      "--zk-quorum", zkServer.getConnectString,
      "--host", host,
      "--port", port
    )
    testPrematureExit(args2, "Zookeeper namespace is not specified")
  }


  test("test expose zk service node to another namespace") {
    val uniqueNamespace = getUniqueNamespace()
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ZK_QUORUM, zkServer.getConnectString)
      .set(HA_ZK_NAMESPACE, uniqueNamespace)
      .set(KyuubiConf.FRONTEND_BIND_PORT, 0)
    System.setProperty(HA_ZK_NAMESPACE.key, uniqueNamespace)

    withZkClient(conf) { framework =>
      createZkServiceNode(conf, framework, uniqueNamespace, "localhost:10000")
      createZkServiceNode(conf, framework, uniqueNamespace, "localhost:10001")

      val newNamespace = s"${uniqueNamespace}_new"
      val args = Array(
        "create", "server",
        "--zk-quorum", zkServer.getConnectString,
        "--namespace", newNamespace
      )

      // scalastyle:off
      val expectedAns =
        s"""
          |+---------------------------------------------------------------------+----------+----------+--------------+
          ||Service Node                                                         |HOST      |PORT      |VERSION       |
          |+---------------------------------------------------------------------+----------+----------+--------------+
          ||serviceUri=localhost:10000;version=$KYUUBI_VERSION;sequence=0000000000|localhost |10000     |$KYUUBI_VERSION|
          ||serviceUri=localhost:10001;version=$KYUUBI_VERSION;sequence=0000000001|localhost |10001     |$KYUUBI_VERSION|
          |+---------------------------------------------------------------------+----------+----------+--------------+
          |2 row(s)""".stripMargin
      // scalastyle:on
      testPrematureExit(args, expectedAns)
      val znodeRoot = s"/$newNamespace"
      val children = framework.getChildren.forPath(znodeRoot).asScala
      assert(children.size == 2)
      assert(children.head ===
        s"serviceUri=localhost:10000;version=$KYUUBI_VERSION;sequence=0000000000")
      assert(children.last ===
        s"serviceUri=localhost:10001;version=$KYUUBI_VERSION;sequence=0000000001")
      children.foreach { child =>
        framework.delete().forPath(s"""$znodeRoot/$child""")
      }
    }
  }

  test("test get zk namespace for different service type") {
    val arg1 = Array(
      "list", "server",
      "--zk-quorum", zkServer.getConnectString,
      "--namespace", namespace
    )
    assert(getZkNamespace(new KyuubiCtlArguments(arg1)) == s"/$namespace")

    val arg2 = Array(
      "list", "engine",
      "--zk-quorum", zkServer.getConnectString,
      "--namespace", namespace,
      "--user", user
    )
    assert(getZkNamespace(new KyuubiCtlArguments(arg2)) == s"/${namespace}_USER/$user")
  }

  test("test list zk service nodes info") {
    val uniqueNamespace = getUniqueNamespace()
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ZK_QUORUM, zkServer.getConnectString)
      .set(HA_ZK_NAMESPACE, uniqueNamespace)
      .set(KyuubiConf.FRONTEND_BIND_PORT, 0)

    withZkClient(conf) { framework =>
      createZkServiceNode(conf, framework, uniqueNamespace, "localhost:10000")
      createZkServiceNode(conf, framework, uniqueNamespace, "localhost:10001")

      val args = Array(
        "list", "server",
        "--zk-quorum", zkServer.getConnectString,
        "--namespace", uniqueNamespace
      )

      // scalastyle:off
      val expectedAns =
        s"""
          |+---------------------------------------------------------------------+----------+----------+--------------+
          ||Service Node                                                         |HOST      |PORT      |VERSION       |
          |+---------------------------------------------------------------------+----------+----------+--------------+
          ||serviceUri=localhost:10000;version=$KYUUBI_VERSION;sequence=0000000000|localhost |10000     |$KYUUBI_VERSION|
          ||serviceUri=localhost:10001;version=$KYUUBI_VERSION;sequence=0000000001|localhost |10001     |$KYUUBI_VERSION|
          |+---------------------------------------------------------------------+----------+----------+--------------+
          |2 row(s)""".stripMargin
      // scalastyle:on

      testPrematureExit(args, expectedAns)
    }
  }

  test("test get zk service nodes info") {
    val uniqueNamespace = getUniqueNamespace()
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ZK_QUORUM, zkServer.getConnectString)
      .set(HA_ZK_NAMESPACE, uniqueNamespace)
      .set(KyuubiConf.FRONTEND_BIND_PORT, 0)

    withZkClient(conf) { framework =>
      createZkServiceNode(conf, framework, uniqueNamespace, "localhost:10000")
      createZkServiceNode(conf, framework, uniqueNamespace, "localhost:10001")

      val args = Array(
        "get", "server",
        "--zk-quorum", zkServer.getConnectString,
        "--namespace", uniqueNamespace,
        "--host", "localhost",
        "--port", "10000"
      )

      // scalastyle:off
      val expectedAns =
        s"""
          |+---------------------------------------------------------------------+----------+----------+--------------+
          ||Service Node                                                         |HOST      |PORT      |VERSION       |
          |+---------------------------------------------------------------------+----------+----------+--------------+
          ||serviceUri=localhost:10000;version=$KYUUBI_VERSION;sequence=0000000000|localhost |10000     |$KYUUBI_VERSION|
          |+---------------------------------------------------------------------+----------+----------+--------------+
          |1 row(s)""".stripMargin
      // scalastyle:on

      testPrematureExit(args, expectedAns)
    }
  }

  test("test delete zk service nodes info") {
    val uniqueNamespace = getUniqueNamespace()
    conf
      .unset(KyuubiConf.SERVER_KEYTAB)
      .unset(KyuubiConf.SERVER_PRINCIPAL)
      .set(HA_ZK_QUORUM, zkServer.getConnectString)
      .set(HA_ZK_NAMESPACE, uniqueNamespace)
      .set(KyuubiConf.FRONTEND_BIND_PORT, 0)

    withZkClient(conf) { framework =>
      withZkClient(conf) { zc =>
        createZkServiceNode(conf, zc, uniqueNamespace, "localhost:10000", external = true)
        createZkServiceNode(conf, zc, uniqueNamespace, "localhost:10001", external = true)
      }

      val args = Array(
        "delete", "server",
        "--zk-quorum", zkServer.getConnectString,
        "--namespace", uniqueNamespace,
        "--host", "localhost",
        "--port", "10000"
      )

      // scalastyle:off
      val expectedAns =
        s"""
          |Deleted zookeeper nodes:
          |
          |+---------------------------------------------------------------------+----------+----------+--------------+
          ||Service Node                                                         |HOST      |PORT      |VERSION       |
          |+---------------------------------------------------------------------+----------+----------+--------------+
          ||serviceUri=localhost:10000;version=$KYUUBI_VERSION;sequence=0000000000|localhost |10000     |$KYUUBI_VERSION|
          |+---------------------------------------------------------------------+----------+----------+--------------+
          |1 row(s)""".stripMargin
      // scalastyle:on

      testPrematureExit(args, expectedAns)
    }
  }
}
