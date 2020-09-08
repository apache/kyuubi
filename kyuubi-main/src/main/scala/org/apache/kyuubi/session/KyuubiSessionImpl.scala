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

package org.apache.kyuubi.session

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import org.apache.curator.utils.ZKPaths
import org.apache.hive.service.rpc.thrift.{TCLIService, TOpenSessionReq, TProtocolVersion, TSessionHandle, TStatus, TStatusCode}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.service.BackendService

class KyuubiSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf,
    zkNamespacePrefix: String)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  private def configureSession(): Unit = {
    conf.foreach { case (k, v) => sessionConf.set(k, v) }
  }

  configureSession()

  private val zkNamespace = s"$zkNamespacePrefix-$user"
  private val zkPath = ZKPaths.makePath(null, zkNamespace)
  private lazy val zkClient = ServiceDiscovery.newZookeeperClient(sessionConf)

  private var transport: TTransport = _
  private var client: TCLIService.Client = _
  private var remoteSessionHandle: TSessionHandle = _

  private def getServerHost(): Option[String] = {
    try {
      val hosts = zkClient.getChildren.forPath(zkPath)
      hosts.asScala.headOption
    } catch {
      case e: Exception => throw KyuubiSQLException(e)
    }
  }

  override def open(): Unit = {
    getServerHost().map { h =>
      val path = ZKPaths.makePath(null, zkNamespace, h)
      new String(zkClient.getData.forPath(path), StandardCharsets.UTF_8)
    } match {
      case Some(hostPort) =>
        val loginTimeout = sessionConf.get(SessionConf.ENGINE_LOGIN_TIMEOUT)
        val strings = hostPort.split(":")
        val host = strings.head
        val port = strings(1).toInt
        transport = new TSocket(host, port, loginTimeout)
        if (!transport.isOpen) transport.open()
        client = new TCLIService.Client(new TBinaryProtocol(transport))
        openSession()
      case None =>
//        new SparkProcessBuilder(sessionConf.toSparkPrefixedConf, Some(user), )

    }
  }


  private def openSession(): Unit = {
    val req = new TOpenSessionReq()
    req.setClient_protocol(BackendService.SERVER_VERSION)
    req.setUsername(user)
    req.setPassword(password)

    val resp = client.OpenSession(req)
    verifyTStatus(resp.getStatus)
    remoteSessionHandle = resp.getSessionHandle
  }


  protected def verifyTStatus(tStatus: TStatus): Unit = {
    if (tStatus.getStatusCode != TStatusCode.SUCCESS_STATUS) {
      throw KyuubiSQLException(tStatus)
    }
  }

  override def close(): Unit = {
    super.close()
  }
}
