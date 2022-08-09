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

package org.apache.kyuubi

import scala.util.control.NonFatal

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols.FrontendProtocol
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ADDRESSES, HA_ZK_AUTH_TYPE}
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.service.AbstractFrontendService
import org.apache.kyuubi.session.KyuubiSessionManager
import org.apache.kyuubi.zookeeper.{EmbeddedZookeeper, ZookeeperConf}

trait WithMultipleKyuubiServer extends KyuubiFunSuite {

  protected val conf: KyuubiConf

  protected val frontendProtocols: Seq[FrontendProtocol] =
    FrontendProtocols.THRIFT_BINARY :: Nil

  private var zkServer: EmbeddedZookeeper = _
  protected val size: Int = 2
  protected var servers: Seq[KyuubiServer] = _

  override def beforeAll(): Unit = {
    conf.setIfMissing(FRONTEND_PROTOCOLS, frontendProtocols.map(_.toString))
    conf.set(FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    conf.set(FRONTEND_REST_BIND_PORT, 0)
    conf.set(FRONTEND_MYSQL_BIND_PORT, 0)

    zkServer = new EmbeddedZookeeper()
    conf.set(ZookeeperConf.ZK_CLIENT_PORT, 0)
    val zkData = Utils.createTempDir()
    conf.set(ZookeeperConf.ZK_CLIENT_PORT_ADDRESS, "127.0.0.1") // TODO
    conf.set(ZookeeperConf.ZK_DATA_DIR, zkData.toString)
    zkServer.initialize(conf)
    zkServer.start()
    conf.set(HA_ADDRESSES, zkServer.getConnectString)
    conf.set(HA_ZK_AUTH_TYPE, AuthTypes.NONE.toString)
    conf.set("spark.ui.enabled", "false")
    conf.setIfMissing("spark.sql.catalogImplementation", "in-memory")
    conf.setIfMissing("kyuubi.ha.zookeeper.connection.retry.policy", "ONE_TIME")
    conf.setIfMissing(ENGINE_CHECK_INTERVAL, 1000L)
    conf.setIfMissing(ENGINE_IDLE_TIMEOUT, 5000L)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (zkServer != null) {
      zkServer.stop()
      zkServer = null
    }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    servers = (0 until size).map(i => {
      val newConf = conf.clone
      newConf.set(SERVER_NAME, s"TestKyuubiServer-$i")
      KyuubiServer.startServer(newConf)
    })
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    servers.foreach { server =>
      server.frontendServices.foreach {
        case frontend: AbstractFrontendService =>
          val sessionManager = frontend.be.sessionManager.asInstanceOf[KyuubiSessionManager]
          sessionManager.allSessions().foreach { session =>
            logger.warn(s"found unclosed session ${session.handle}.")
            try {
              sessionManager.closeSession(session.handle)
            } catch {
              case NonFatal(e) =>
                logger.warn(s"catching an error while closing the session ${session.handle}", e)
            }
          }
        case _ =>
      }
      server.stop()
    }
    servers = Seq()
    super.afterEach()
  }

  protected def getJdbcUrl(i: Int = 0): String =
    s"jdbc:hive2://${servers(i).frontendServices.head.connectionUrl}/;"

  protected def getConnectionUrl(i: Int = 0): String =
    servers(i).frontendServices.head.connectionUrl

  protected def getHAJdbcUrl: String = {
    val quorum = conf.get(HighAvailabilityConf.HA_ADDRESSES)
    val ns = conf.get(HighAvailabilityConf.HA_NAMESPACE)
    s"jdbc:hive2://$quorum/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=$ns"
  }

}
