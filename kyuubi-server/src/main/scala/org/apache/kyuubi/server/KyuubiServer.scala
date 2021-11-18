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

package org.apache.kyuubi.server

import java.util

import scala.util.Properties

import org.apache.curator.utils.ZKPaths
import org.apache.hadoop.security.UserGroupInformation
import org.apache.zookeeper.CreateMode.PERSISTENT
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NodeExistsException

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_PROTOCOLS, FrontendProtocols}
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols._
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{ServiceDiscovery, ZooKeeperAuthTypes}
import org.apache.kyuubi.ha.client.ZooKeeperClientProvider._
import org.apache.kyuubi.metrics.{MetricsConf, MetricsSystem}
import org.apache.kyuubi.service.{AbstractBackendService, AbstractFrontendService, Serverable}
import org.apache.kyuubi.util.{KyuubiHadoopUtils, SignalRegister}
import org.apache.kyuubi.zookeeper.EmbeddedZookeeper

object KyuubiServer extends Logging {
  private val zkServer = new EmbeddedZookeeper()
  private[kyuubi] var kyuubiServer: KyuubiServer = _

  def startServer(conf: KyuubiConf): KyuubiServer = {
    if (!ServiceDiscovery.supportServiceDiscovery(conf)) {
      zkServer.initialize(conf)
      zkServer.start()
      conf.set(HA_ZK_QUORUM, zkServer.getConnectString)
      conf.set(HA_ZK_AUTH_TYPE, ZooKeeperAuthTypes.NONE.toString)
    } else {
      // create chroot path if necessary
      val connectionStr = conf.get(HA_ZK_QUORUM)
      val addresses = connectionStr.split(",")
      val slashOption = util.Arrays.copyOfRange(addresses, 0, addresses.length -1)
        .toList
        .find(_.contains("/"))
      if (slashOption.isDefined) {
        throw new IllegalArgumentException(s"Illegal zookeeper quorum '$connectionStr', " +
          s"the chroot path started with / is only allowed at the end!")
      }
      val chrootIndex = connectionStr.indexOf("/")
      val chrootOption = {
        if (chrootIndex > 0) Some(connectionStr.substring(chrootIndex))
        else None
      }
      chrootOption.foreach { chroot =>
        val zkConnectionForChrootCreation = connectionStr.substring(0, chrootIndex)
        val overrideQuorumConf = conf.clone.set(HA_ZK_QUORUM, zkConnectionForChrootCreation)
        withZkClient(overrideQuorumConf) { zkClient =>
          if (zkClient.checkExists().forPath(chroot) == null) {
            val chrootPath = ZKPaths.makePath(null, chroot)
            try {
              zkClient
                .create()
                .creatingParentsIfNeeded()
                .withMode(PERSISTENT)
                .forPath(chrootPath)
            } catch {
              case _: NodeExistsException => // do nothing
              case e: KeeperException =>
                throw new KyuubiException(s"Failed to create chroot path '$chrootPath'", e)
            }
          }
        }
        info(s"Created zookeeper chroot path $chroot")
      }
    }

    val server = new KyuubiServer()
    server.initialize(conf)
    server.start()
    Utils.addShutdownHook(() => server.stop(), Utils.SERVER_SHUTDOWN_PRIORITY)
    server
  }

  def main(args: Array[String]): Unit = {
    info(
       """
         |                  Welcome to
         |  __  __                           __
         | /\ \/\ \                         /\ \      __
         | \ \ \/'/'  __  __  __  __  __  __\ \ \____/\_\
         |  \ \ , <  /\ \/\ \/\ \/\ \/\ \/\ \\ \ '__`\/\ \
         |   \ \ \\`\\ \ \_\ \ \ \_\ \ \ \_\ \\ \ \L\ \ \ \
         |    \ \_\ \_\/`____ \ \____/\ \____/ \ \_,__/\ \_\
         |     \/_/\/_/`/___/> \/___/  \/___/   \/___/  \/_/
         |                /\___/
         |                \/__/
       """.stripMargin)
    info(s"Version: $KYUUBI_VERSION, Revision: $REVISION, Branch: $BRANCH," +
      s" Java: $JAVA_COMPILE_VERSION, Scala: $SCALA_COMPILE_VERSION," +
      s" Spark: $SPARK_COMPILE_VERSION, Hadoop: $HADOOP_COMPILE_VERSION," +
      s" Hive: $HIVE_COMPILE_VERSION")
    info(s"Using Scala ${Properties.versionString}, ${Properties.javaVmName}," +
      s" ${Properties.javaVersion}")
    SignalRegister.registerLogger(logger)
    val conf = new KyuubiConf().loadFileDefaults()
    UserGroupInformation.setConfiguration(KyuubiHadoopUtils.newHadoopConf(conf))
    startServer(conf)
  }
}

class KyuubiServer(name: String) extends Serverable(name) {

  def this() = this(classOf[KyuubiServer].getSimpleName)

  override val backendService: AbstractBackendService = new KyuubiBackendService()

  override lazy val frontendServices: Seq[AbstractFrontendService] =
    conf.get(FRONTEND_PROTOCOLS).map(FrontendProtocols.withName).map {
      case THRIFT_BINARY => new KyuubiThriftBinaryFrontendService(this)
      case REST =>
        warn("REST frontend protocol is experimental, API may change in the future.")
        new KyuubiRestFrontendService(this)
      case other =>
        throw new UnsupportedOperationException(s"Frontend protocol $other is not supported yet.")
    }

  private val eventLoggingService: EventLoggingService = new EventLoggingService

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    val kinit = new KinitAuxiliaryService()
    addService(kinit)
    addService(eventLoggingService)

    if (conf.get(MetricsConf.METRICS_ENABLED)) {
      addService(new MetricsSystem)
    }
    super.initialize(conf)
  }

  override def start(): Unit = {
    super.start()
    KyuubiServer.kyuubiServer = this
  }

  override protected def stopServer(): Unit = {}
}
