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

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TCLIService, TCloseSessionReq, TOpenSessionReq, TSessionHandle}
import org.apache.hive.service.rpc.thrift.TCLIService.Iface

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.client.KyuubiSyncThriftClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.FrontendService

object TClientTestUtils extends Logging {

  def withThriftClient[T](url: String)(f: Iface => T): T = {
    val hostport = url.split(':')
    val client = KyuubiSyncThriftClient.createClient(
      Utils.currentUser,
      "anonymous",
      hostport.head,
      hostport.last.toInt,
      KyuubiConf())
    try {
      f(client)
    } finally {
      val transport = client.getInputProtocol.getTransport
      if (transport.isOpen) transport.close()
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
}
