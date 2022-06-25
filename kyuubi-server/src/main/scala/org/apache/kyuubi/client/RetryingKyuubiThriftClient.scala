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

package org.apache.kyuubi.client

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy, UndeclaredThrowableException}

import org.apache.hive.service.rpc.thrift.{TProtocolVersion, TSessionHandle}
import org.apache.thrift.TException

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor

case class RetryingKyuubiThriftClient(
    protocol: TProtocolVersion,
    user: String,
    passwd: String,
    host: String,
    port: Int,
    conf: KyuubiConf,
    openSessionConf: Map[String, String],
    createClient: String => KyuubiThriftClient) extends InvocationHandler with Logging {

  private val maxAttempts = conf.get(KyuubiConf.OPERATION_THRIFT_CLIENT_REQUEST_MAX_ATTEMPTS)
  private val attemptWait = conf.get(KyuubiConf.OPERATION_THRIFT_CLIENT_REQUEST_ATTEMPT_WAIT)

  private var thriftClient: KyuubiThriftClient = _

  newThriftClient()

  private def newThriftClient(): Unit = {
    var remoteSessionHandle: Option[TSessionHandle] = None
    if (thriftClient != null) {
      remoteSessionHandle = thriftClient.remoteSessionHandle
      try {
        thriftClient.protocol.getTransport.close()
        thriftClient = null
      } catch {
        case e: Throwable =>
          warn("Failed to close thrift protocol", e)
      }
    }

    val password = Option(InternalSecurityAccessor.get()).map(_.issueToken()).getOrElse(passwd)
    try {
      thriftClient = createClient(password)

      if (remoteSessionHandle.nonEmpty) {
        thriftClient.openSession(protocol, user, password, openSessionConf, remoteSessionHandle)
      }
    } catch {
      case e: Throwable =>
        error("Failed to new/renew the thrift client", e)
        throw e
    }
  }

  override def invoke(proxy: Object, method: Method, args: Array[Object]): Object = {
    var retryCount = 0
    while (true) {
      try {
        return Option(thriftClient).map(method.invoke(_, args: _*)).orNull
      } catch {
        case e: UndeclaredThrowableException => throw e.getCause
        case e: InvocationTargetException =>
          if (e.getCause == null) {
            throw e
          } else if (e.getCause.isInstanceOf[TException]) {
            retryCount += 1
            if (retryCount <= maxAttempts) {
              Thread.sleep(attemptWait)
            } else {
              error(s"Attempt over $maxAttempts times", e.getCause)
              throw e.getCause
            }
            newThriftClient()
          } else {
            throw e.getCause
          }
      }
    }
    null
  }
}

object RetryingKyuubiThriftClient {
  def createClient(
      protocol: TProtocolVersion,
      user: String,
      passwd: String,
      host: String,
      port: Int,
      conf: KyuubiConf,
      openSessionConf: Map[String, String],
      createClient: String => KyuubiThriftClient): KyuubiThriftClient = {
    val client = new RetryingKyuubiThriftClient(
      protocol,
      user,
      passwd,
      host,
      port,
      conf,
      openSessionConf,
      createClient)
    Proxy.newProxyInstance(
      Thread.currentThread().getContextClassLoader(),
      Array(classOf[KyuubiThriftClient]),
      client).asInstanceOf[KyuubiThriftClient]
  }

  def getKyuubiSyncThriftClient(
      protocol: TProtocolVersion,
      user: String,
      passwd: String,
      host: String,
      port: Int,
      conf: KyuubiConf,
      openSessionConf: Map[String, String]): KyuubiThriftClient = {
    createClient(
      protocol,
      user,
      passwd,
      host,
      port,
      conf,
      openSessionConf,
      password => KyuubiSyncThriftClient.createClient(user, password, host, port, conf))
  }
}
