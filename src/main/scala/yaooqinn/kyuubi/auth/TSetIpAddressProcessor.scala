/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package yaooqinn.kyuubi.auth

import org.apache.hive.service.cli.thrift.TCLIService.{Iface, Processor}
import org.apache.thrift.TException
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.{TSaslClientTransport, TSaslServerTransport, TSocket, TTransport}

import yaooqinn.kyuubi.Logging

/**
 * This class is responsible for setting the ipAddress for operations executed via Kyuubi.
 * <p>
 * <ul>
 * <li>IP address is only set for operations that calls listeners with hookContext</li>
 * <li>IP address is only set if the underlying transport mechanism is socket</li>
 * </ul>
 * </p>
 *
 */

class TSetIpAddressProcessor[I <: Iface](iface: Iface)
  extends Processor[Iface](iface) with Logging {

  import TSetIpAddressProcessor._

  @throws[TException]
  override def process(in: TProtocol, out: TProtocol): Boolean = {
    setIpAddress(in)
    setUserName(in)
    try {
      super.process(in, out)
    } finally {
      THREAD_LOCAL_USER_NAME.remove()
      THREAD_LOCAL_IP_ADDRESS.remove()
    }
  }

  private def setUserName(in: TProtocol) = {
    val transport = in.getTransport
    transport match {
      case transport1: TSaslServerTransport =>
        val userName = transport1.getSaslServer.getAuthorizationID
        THREAD_LOCAL_USER_NAME.set(userName)
      case _ =>
    }
  }

  private def setIpAddress(in: TProtocol): Unit = {
    val transport = in.getTransport
    val tSocket = getUnderlyingSocketFromTransport(transport)
    if (tSocket == null) {
      warn("Unknown Transport, cannot determine ipAddress")
    } else {
      THREAD_LOCAL_IP_ADDRESS.set(tSocket.getSocket.getInetAddress.getHostAddress)
    }
  }

  private def getUnderlyingSocketFromTransport(transport: TTransport): TSocket = transport match {
    case t: TSaslServerTransport => getUnderlyingSocketFromTransport(t.getUnderlyingTransport)
    case t: TSaslClientTransport => getUnderlyingSocketFromTransport(t.getUnderlyingTransport)
    case t: TSocket => t
    case _ => null
  }
}

object TSetIpAddressProcessor {

  private val THREAD_LOCAL_IP_ADDRESS = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }
  private val THREAD_LOCAL_USER_NAME = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  def getUserIpAddress: String = THREAD_LOCAL_IP_ADDRESS.get

  def getUserName: String = THREAD_LOCAL_USER_NAME.get
}
