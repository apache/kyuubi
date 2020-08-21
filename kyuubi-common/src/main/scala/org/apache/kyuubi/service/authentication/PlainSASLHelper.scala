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

package org.apache.kyuubi.service.authentication

import java.security.Security
import javax.security.auth.callback.{Callback, CallbackHandler, NameCallback, PasswordCallback, UnsupportedCallbackException}
import javax.security.auth.login.LoginException
import javax.security.sasl.{AuthenticationException, AuthorizeCallback}

import org.apache.hive.service.rpc.thrift.TCLIService.Iface
import org.apache.thrift.{TProcessor, TProcessorFactory}
import org.apache.thrift.transport.{TSaslServerTransport, TTransport, TTransportFactory}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthMethods.AuthMethod
import org.apache.kyuubi.service.authentication.PlainSASLServer.SaslPlainProvider

object PlainSASLHelper {

  // Register Plain SASL server provider
  Security.addProvider(new SaslPlainProvider())

  private case class SQLPlainProcessorFactory(service: Iface) extends TProcessorFactory(null) {
    override def getProcessor(trans: TTransport): TProcessor =
      new TSetIpAddressProcessor[Iface](service)
  }

  private class PlainServerCallbackHandler private(authMethod: AuthMethod, conf: KyuubiConf)
    extends CallbackHandler {

    def this(authMethodStr: String, conf: KyuubiConf) =
      this(AuthMethods.withName(authMethodStr), conf)

    @throws[UnsupportedCallbackException]
    override def handle(callbacks: Array[Callback]): Unit = {
      var username: String = null
      var password: String = null
      var ac: AuthorizeCallback = null
      for (callback <- callbacks) {
        callback match {
          case nc: NameCallback =>
            username = nc.getName
          case pc: PasswordCallback =>
            password = new String(pc.getPassword)
          case a: AuthorizeCallback => ac = a
          case _ => throw new UnsupportedCallbackException(callback)
        }
      }
      val provider = AuthenticationProviderFactory.getAuthenticationProvider(authMethod, conf)
      provider.authenticate(username, password)
      if (ac != null) ac.setAuthorized(true)
    }
  }

  def getProcessFactory(service: Iface): TProcessorFactory = {
    SQLPlainProcessorFactory(service)
  }

  def getTransportFactory(authTypeStr: String, conf: KyuubiConf): TTransportFactory = {
    val saslFactory = new TSaslServerTransport.Factory()
    try {
      val handler = new PlainServerCallbackHandler(authTypeStr, conf)
      val props = new java.util.HashMap[String, String]
      saslFactory.addServerDefinition("PLAIN", authTypeStr, null, props, handler)
    } catch {
      case e: AuthenticationException =>
        throw new LoginException("Error setting callback handler" + e);
    }
    saslFactory
  }
}
