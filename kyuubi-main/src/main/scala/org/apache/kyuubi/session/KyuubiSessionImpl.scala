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

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.curator.utils.ZKPaths
import org.apache.hive.service.rpc.thrift.{TCLIService, TCloseSessionReq, TOpenSessionReq, TProtocolVersion, TSessionHandle}
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport}

import org.apache.kyuubi.{KyuubiSQLException, ThriftUtils, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.{ShareLevel, SQLEngineAppName}
import org.apache.kyuubi.engine.ShareLevel.{SERVER, ShareLevel}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.service.authentication.PlainSASLHelper

class KyuubiSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  private def mergeConf(): Unit = {
    conf.foreach {
      case (HIVE_VAR_PREFIX(key), value) => sessionConf.set(key, value)
      case (HIVE_CONF_PREFIX(key), value) => sessionConf.set(key, value)
      case ("use:database", _) =>
      case (key, value) => sessionConf.set(key, value)
    }
  }

  mergeConf()

  private val shareLevel: ShareLevel = ShareLevel.withName(sessionConf.get(ENGINE_SHARED_LEVEL))

  private val appUser: String = shareLevel match {
    case SERVER => Utils.currentUser
    case _ => user
  }

  private val boundAppName: SQLEngineAppName = SQLEngineAppName(shareLevel, appUser, handle)

  private val appZkNamespace: String = boundAppName.getZkNamespace(sessionConf.get(HA_ZK_NAMESPACE))

  private lazy val zkClient = ServiceDiscovery.startZookeeperClient(sessionConf)
  private val timeout: Long = sessionConf.get(ENGINE_INIT_TIMEOUT)

  private var transport: TTransport = _
  private var client: TCLIService.Client = _
  private var remoteSessionHandle: TSessionHandle = _

  private def getServerHost: Option[(String, Int)] = {
    try {
      val hosts = zkClient.getChildren.forPath(appZkNamespace)
      // TODO: use last one because to avoid touching some maybe-crashed engines
      // We need a big improvement here.
      hosts.asScala.lastOption.map { p =>
        val path = ZKPaths.makePath(appZkNamespace, p)
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
    getServerHost match {
      case Some((host, port)) => openSession(host, port)
      case None =>
        sessionConf.set(SparkProcessBuilder.APP_KEY, boundAppName.toString)
        sessionConf.set(HA_ZK_NAMESPACE, appZkNamespace)
        val builder = new SparkProcessBuilder(appUser, sessionConf.toSparkPrefixedConf)
        try {
          val process = builder.start
          info(s"Launching SQL engine: $builder, is cluster mode: ${builder.isClusterMode}")
          var sh = getServerHost
          val started = System.currentTimeMillis()
          // Here are 4 option:
          // 1. Spark exit error (exitValue != 0)
          // 2. Client mode, Spark exit normally (exitValue = 0) but failed to get server host
          // 3. timeout
          // 4. We get server host successfully
          while (sh.isEmpty) {
            if (process.waitFor(1, TimeUnit.SECONDS)) {
              if (process.exitValue() != 0) {
                throw builder.getError
              } else {
                if (!builder.isClusterMode) {
                  throw builder.getError
                }
              }
            }
            if (started + timeout <= System.currentTimeMillis()) {
              process.destroyForcibly()
              throw KyuubiSQLException(s"Timed out($timeout ms) to launched Spark with $builder",
                builder.getError)
            }
            sh = getServerHost
          }
          val Some((host, port)) = sh
          openSession(host, port)
        } finally {
          // we must close the process builder whether session open is success or failure since
          // we have a log capture thread in process builder.
          builder.close()
        }
    }

    try {
      zkClient.close()
    } catch {
      case e: IOException => error("Failed to release the zkClient after session established", e)
    }
  }

  private def openSession(host: String, port: Int): Unit = {
    val passwd = Option(password).filter(_.nonEmpty).getOrElse("anonymous")
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
