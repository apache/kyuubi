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

package org.apache.kyuubi.operation

import java.nio.ByteBuffer
import java.util.Base64

import scala.collection.JavaConverters._

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.service.FrontendService
import org.apache.kyuubi.service.authentication.PlainSASLHelper
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TCLIService.Iface
import org.apache.kyuubi.shaded.thrift.protocol.TBinaryProtocol
import org.apache.kyuubi.shaded.thrift.transport.TSocket

object TClientTestUtils extends Logging {

  def withThriftClient[T](url: String, user: Option[String] = None)(f: Iface => T): T = {
    val hostport = url.split(':')
    val socket = new TSocket(hostport.head, hostport.last.toInt)
    val transport = PlainSASLHelper.getPlainTransport(
      user.getOrElse(Utils.currentUser),
      "anonymous",
      socket)
    val protocol = new TBinaryProtocol(transport)
    val client = new TCLIService.Client(protocol)
    transport.open()
    try {
      f(client)
    } finally {
      socket.close()
    }
  }

  def withThriftClientAndConnectionConf[T](
      url: String,
      user: Option[String] = None)(f: (Iface, Map[String, String]) => T): T = {
    val hostPortPrefix = url.split("/;").head
    val hostport = hostPortPrefix.split(':')
    val connectionConf = url.stripPrefix(hostPortPrefix) match {
      case connectionStr: String if connectionStr.startsWith("/;#") =>
        val kvPairs = connectionStr.stripPrefix("/;#")
        if (kvPairs.contains("=")) {
          kvPairs.split(";").map(kv => (kv.split("=")(0), kv.split("=")(1))).toMap
        } else {
          Map.empty[String, String]
        }
      case _ =>
        Map.empty[String, String]
    }

    val socket = new TSocket(hostport.head, hostport.last.toInt)
    val transport = PlainSASLHelper.getPlainTransport(
      user.getOrElse(Utils.currentUser),
      "anonymous",
      socket)
    val protocol = new TBinaryProtocol(transport)
    val client = new TCLIService.Client(protocol)
    transport.open()
    try {
      f(client, connectionConf)
    } finally {
      socket.close()
    }
  }

  /**
   * s shall be [[TFrontendService]]
   */
  def withThriftClient[T](s: FrontendService)(f: Iface => T): T = {
    withThriftClient(s.connectionUrl)(f)
  }

  def withSessionHandle[T](url: String, configs: Map[String, String])(
      f: (TCLIService.Iface, TSessionHandle) => T): T = {
    withThriftClient(url) { client =>
      val req = new TOpenSessionReq()
      req.setUsername(Utils.currentUser)
      req.setPassword("anonymous")
      req.setConfiguration(configs.asJava)
      val resp = client.OpenSession(req)
      val handle = resp.getSessionHandle
      try {
        f(client, handle)
      } finally {
        val tCloseSessionReq = new TCloseSessionReq(handle)
        try {
          client.CloseSession(tCloseSessionReq)
        } catch {
          case e: Exception => error(s"Failed to close $handle", e)
        }
      }
    }
  }

  def withSessionAndLaunchEngineHandle[T](url: String, configs: Map[String, String])(
      f: (TCLIService.Iface, TSessionHandle, Option[TOperationHandle]) => T): T = {
    withThriftClient(url) { client =>
      val req = new TOpenSessionReq()
      req.setUsername(Utils.currentUser)
      req.setPassword("anonymous")
      req.setConfiguration(configs.asJava)
      val resp = client.OpenSession(req)
      val sessionHandle = resp.getSessionHandle

      val guid = resp.getConfiguration.get(KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_GUID)
      val secret = resp.getConfiguration.get(KYUUBI_SESSION_ENGINE_LAUNCH_HANDLE_SECRET)

      val launchOpHandleOpt =
        if (guid != null && secret != null) {
          val launchHandleId = new THandleIdentifier(
            ByteBuffer.wrap(Base64.getDecoder.decode(guid)),
            ByteBuffer.wrap(Base64.getDecoder.decode(secret)))
          Some(new TOperationHandle(launchHandleId, TOperationType.UNKNOWN, false))
        } else None

      try {
        f(client, sessionHandle, launchOpHandleOpt)
      } finally {
        val tCloseSessionReq = new TCloseSessionReq(sessionHandle)
        try {
          client.CloseSession(tCloseSessionReq)
        } catch {
          case e: Exception => error(s"Failed to close $sessionHandle", e)
        }
      }
    }
  }
}
