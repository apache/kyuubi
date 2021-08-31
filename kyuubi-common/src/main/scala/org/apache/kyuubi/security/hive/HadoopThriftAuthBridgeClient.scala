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

package org.apache.kyuubi.security.hive

import java.io.IOException
import java.security.PrivilegedExceptionAction
import java.util.{Map => JMap}
import javax.security.sasl.SaslException

import org.apache.hadoop.security.{SaslRpcServer, SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.SaslRpcServer.AuthMethod._
import org.apache.thrift.transport.{TSaslClientTransport, TTransport, TTransportException}

import org.apache.kyuubi.Logging

class HadoopThriftAuthBridgeClient extends Logging {

  /**
   * Create a client-side SASL transport that wraps an underlying transport.
   *
   * @param principalConfig     The Kerberos principal of the target server.
   * @param underlyingTransport The underlying transport mechanism, usually a TSocket.
   * @param saslProps           the sasl properties to create the client with
   */
  @throws[IOException]
  def createClientTransport(
    principalConfig: String,
    host: String,
    underlyingTransport: TTransport,
    saslProps: JMap[String, String]): TTransport = {
    val serverPrincipal: String = SecurityUtil.getServerPrincipal(principalConfig, host)
    val names: Array[String] = SaslRpcServer.splitKerberosName(serverPrincipal)
    if (names.length != 3) {
      throw new IOException(
        "Kerberos principal name does NOT have the expected hostname part: " + serverPrincipal)
    }
    try {
      UserGroupInformation.getCurrentUser.doAs(
        new PrivilegedExceptionAction[TUGIAssumingTransport]() {
          @throws[IOException]
          override def run: TUGIAssumingTransport = {
            val saslTransport = new TSaslClientTransport(
              KERBEROS.getMechanismName,
              null,
              names(0),
              names(1),
              saslProps,
              null,
              underlyingTransport)
            new TUGIAssumingTransport(saslTransport, UserGroupInformation.getCurrentUser)
          }
        })
    } catch {
      case se@(_: InterruptedException | _: SaslException) =>
        throw new IOException("Could not instantiate SASL transport", se)
    }
  }
}

/**
 * The Thrift SASL transports call Sasl.createSaslServer and Sasl.createSaslClient inside open().
 * So, we need to assume the correct UGI when the transport is opened so that the SASL mechanisms
 * have access to the right principal. This transport wraps the Sasl transports to set up the
 * right UGI context for open().
 *
 * This is used on the client side, where the API explicitly opens a transport to the server.
 */
class TUGIAssumingTransport(wrapped: TTransport, ugi: UserGroupInformation)
    extends TFilterTransport(wrapped) {

  @throws[TTransportException]
  override def open(): Unit = {
    try {
      ugi.doAs(new PrivilegedExceptionAction[Unit]() {
        override def run: Unit = {
          try wrapped.open()
          catch {
            case tte: TTransportException =>
              // Wrap the transport exception in an RTE, since UGI.doAs() then goes
              // and unwraps this for us out of the doAs block. We then unwrap one
              // more time in our catch clause to get back the TTE. (ugh)
              throw new RuntimeException(tte)
          }
        }
      })
    } catch {
      case ioe: IOException =>
        throw new RuntimeException("Received an ioe we never threw!", ioe)
      case ie: InterruptedException =>
        throw new RuntimeException("Received an ie we never threw!", ie)
      case rte: RuntimeException =>
        if (rte.getCause.isInstanceOf[TTransportException]) {
          throw rte.getCause.asInstanceOf[TTransportException]
        } else {
          throw rte
        }
    }
  }

}

class TFilterTransport(val wrapped: TTransport) extends TTransport {

  @throws[TTransportException]
  override def open(): Unit = wrapped.open()

  override def isOpen: Boolean = wrapped.isOpen

  override def peek: Boolean = wrapped.peek

  override def close(): Unit = wrapped.close()

  @throws[TTransportException]
  override def read(buf: Array[Byte], off: Int, len: Int): Int = wrapped.read(buf, off, len)

  @throws[TTransportException]
  override def readAll(buf: Array[Byte], off: Int, len: Int): Int = wrapped.readAll(buf, off, len)

  @throws[TTransportException]
  override def write(buf: Array[Byte]): Unit = wrapped.write(buf)

  @throws[TTransportException]
  override def write(buf: Array[Byte], off: Int, len: Int): Unit = wrapped.write(buf, off, len)

  @throws[TTransportException]
  override def flush(): Unit = wrapped.flush()

  override def getBuffer: Array[Byte] = wrapped.getBuffer

  override def getBufferPosition: Int = wrapped.getBufferPosition

  override def getBytesRemainingInBuffer: Int = wrapped.getBytesRemainingInBuffer

  override def consumeBuffer(len: Int): Unit = wrapped.consumeBuffer(len)
}
