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

package org.apache.kyuubi.service.authentication.ldap

import java.io.IOException
import java.net.{InetAddress, Socket}
import javax.net.SocketFactory
import javax.net.ssl.SSLContext

final class LdapSSLSocketFactory private (socketFactory: SocketFactory) extends SocketFactory {

  @throws[IOException]
  override def createSocket(): Socket = socketFactory.createSocket()

  @throws[IOException]
  override def createSocket(host: String, port: Int): Socket =
    socketFactory.createSocket(host, port)

  @throws[IOException]
  override def createSocket(
      host: String,
      port: Int,
      localHost: InetAddress,
      localPort: Int): Socket = {
    socketFactory.createSocket(host, port, localHost, localPort)
  }

  @throws[IOException]
  override def createSocket(host: InetAddress, port: Int): Socket =
    socketFactory.createSocket(host, port)

  @throws[IOException]
  override def createSocket(
      address: InetAddress,
      port: Int,
      localAddress: InetAddress,
      localPort: Int): Socket = {
    socketFactory.createSocket(address, port, localAddress, localPort)
  }
}

object LdapSSLSocketFactory {

  private val sslContext = new ThreadLocal[SSLContext]

  def getDefault(): SocketFactory = {
    val context = sslContext.get()
    if (context == null) {
      throw new IllegalStateException("SSLContext was not set for LDAP SSL connection")
    }
    new LdapSSLSocketFactory(context.getSocketFactory)
  }

  def setSSLContextForCurrentThread(context: SSLContext): Unit = {
    sslContext.set(context)
  }

  def clearSslContextForCurrentThread(): Unit = {
    sslContext.remove()
  }
}
