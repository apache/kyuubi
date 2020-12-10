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
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.curator.utils.ZKPaths
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport}

import org.apache.kyuubi.{KyuubiSQLException, ThriftUtils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.service.authentication.PlainSASLHelper

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
    conf.foreach {
      case (HIVE_VAR_PREFIX(key), value) => sessionConf.set(key, value)
      case (HIVE_CONF_PREFIX(key), value) => sessionConf.set(key, value)
      case ("use:database", _) =>
      case (key, value) => sessionConf.set(key, value)
    }
  }

  private val timeout: Long = sessionConf.get(ENGINE_INIT_TIMEOUT)
  private val zkNamespace = s"$zkNamespacePrefix-$user"
  private val zkPath = ZKPaths.makePath(null, zkNamespace)
  private lazy val zkClient = ServiceDiscovery.startZookeeperClient(sessionConf)

  private var transport: TTransport = _
  private var client: TCLIService.Client = _
  private var remoteSessionHandle: TSessionHandle = _

  private def getServerHost: Option[(String, Int)] = {
    try {
      val hosts = zkClient.getChildren.forPath(zkPath)
      hosts.asScala.headOption.map { p =>
        val path = ZKPaths.makePath(null, zkNamespace, p)
        val hostPort = new String(zkClient.getData.forPath(path), StandardCharsets.UTF_8)
        val strings = hostPort.split(":")
        val host = strings.head
        val port = strings(1).toInt
        (host, port)
      }
    } catch {
      case _: Exception => None
    }
  }

  override def open(): Unit = {
    super.open()
    // Init zookeeper client here to capture errors
    zkClient
    try {
      getServerHost match {
        case Some((host, port)) => openSession(host, port)
        case None =>
          configureSession()
          val builder = new SparkProcessBuilder(user, sessionConf.toSparkPrefixedConf)
          val process = builder.start
          info(s"Launching SQL engine: $builder")
          var sh = getServerHost
          val started = System.currentTimeMillis()
          while (sh.isEmpty) {
            if (process.waitFor(1, TimeUnit.SECONDS)) {
              throw builder.getError
            }
            if (started + timeout <= System.currentTimeMillis()) {
              process.destroyForcibly()
              throw KyuubiSQLException(s"Timed out($timeout ms) to launched Spark with $builder",
                builder.getError)
            }
            sh = getServerHost
          }
          val Some((host, port)) = getServerHost
          openSession(host, port)
      }
    } catch {
      case e: Exception =>
        throw KyuubiSQLException("Error while open session", e)
    } finally {
      zkClient.close()
    }
  }

  private def openSession(host: String, port: Int): Unit = {
    val passwd = Option(password).getOrElse("anonymous")
    val loginTimeout = sessionConf.get(ENGINE_LOGIN_TIMEOUT).toInt
    transport = PlainSASLHelper.getPlainTransport(
      user, passwd, new TSocket(host, port, loginTimeout))
    if (!transport.isOpen) transport.open()
    client = new TCLIService.Client(new TBinaryProtocol(transport))
    val req = new TOpenSessionReq()
    req.setUsername(user)
    req.setPassword(passwd)
    req.setConfiguration(conf.asJava)
    val resp = client.OpenSession(req)
    ThriftUtils.verifyTStatus(resp.getStatus)
    remoteSessionHandle = resp.getSessionHandle
    sessionManager.operationManager.setConnection(handle, client, remoteSessionHandle)
  }

  override def close(): Unit = {
    super.close()
    sessionManager.operationManager.removeConnection(handle)
    try {
      if (remoteSessionHandle != null) {
        val req = new TCloseSessionReq(remoteSessionHandle)
        val resp = client.CloseSession(req)
        ThriftUtils.verifyTStatus(resp.getStatus)
      }
    } catch {
      case e: TException =>
        throw KyuubiSQLException("Error while cleaning up the engine resources", e)
    } finally {
      client = null
      if (transport != null) {
        transport.close()
      }
    }
  }
}
