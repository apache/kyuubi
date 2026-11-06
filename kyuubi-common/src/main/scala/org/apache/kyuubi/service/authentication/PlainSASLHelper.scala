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
import java.util
import java.util.Collections
import javax.security.auth.callback.{Callback, CallbackHandler, NameCallback, PasswordCallback, UnsupportedCallbackException}
import javax.security.sasl.{AuthorizeCallback, Sasl}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthMethods.AuthMethod
import org.apache.kyuubi.service.authentication.PlainSASLServer.SaslPlainProvider
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TCLIService.Iface
import org.apache.kyuubi.shaded.thrift.{TProcessor, TProcessorFactory}
import org.apache.kyuubi.shaded.thrift.transport.{TSaslClientTransport, TSaslServerTransport, TTransport, TTransportFactory}

object PlainSASLHelper {

  // Register Plain SASL server provider
  Security.addProvider(new SaslPlainProvider())

  private case class SQLPlainProcessorFactory(service: Iface) extends TProcessorFactory(null) {
    override def getProcessor(trans: TTransport): TProcessor =
      new TSetIpAddressProcessor[Iface](service)
  }

  private class PlainServerCallbackHandler private (
      authMethod: AuthMethod,
      conf: KyuubiConf,
      isServer: Boolean)
    extends CallbackHandler {

    def this(authMethodStr: String, conf: KyuubiConf, isServer: Boolean) =
      this(AuthMethods.withName(authMethodStr), conf, isServer)

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
      val provider =
        AuthenticationProviderFactory.getAuthenticationProvider(authMethod, conf, isServer)
      provider.authenticate(username, password)
      if (ac != null) ac.setAuthorized(true)
    }
  }

  def getProcessFactory(service: Iface): TProcessorFactory = {
    SQLPlainProcessorFactory(service)
  }

  def getTransportFactory(
      authTypeStr: String,
      conf: KyuubiConf,
      transportFactory: Option[TSaslServerTransport.Factory] = None,
      isServer: Boolean = true): TTransportFactory = {
    val handler =
      try {
        new PlainServerCallbackHandler(authTypeStr, conf, isServer)
      } catch {
        case _: NoSuchElementException =>
          throw new IllegalArgumentException(
            s"Illegal authentication type $authTypeStr for plain transport")
      }
    val saslFactory = transportFactory.getOrElse {
      val _factory = new TSaslServerTransport.Factory()
      _factory.setSaslServerFactory { d =>
        if (d.mechanism == "PLAIN") {
          // [KYUUBI #7142]: There may be multiple SaslServer classes registered for PLAIN
          // mechanism, we should not use JDK Sasl.createSaslServer to avoid picking up the
          // unexpected SaslServer implementation.
          val kyuubiFactory = new PlainSASLServer.SaslPlainServerFactory()
          kyuubiFactory.createSaslServer(d.mechanism, d.protocol, d.serverName, d.props, d.cbh)
        } else {
          Sasl.createSaslServer(d.mechanism, d.protocol, d.serverName, d.props, d.cbh)
        }
      }
      _factory
    }
    val props = new util.HashMap[String, String]
    props.put("org.apache.kyuubi.service.name", if (isServer) "SERVER" else "ENGINE")
    saslFactory.addServerDefinition("PLAIN", authTypeStr, null, props, handler)
    saslFactory
  }

  private class PlainCallBackHandler(user: String, password: String) extends CallbackHandler {
    override def handle(callbacks: Array[Callback]): Unit = {
      callbacks.foreach {
        case nc: NameCallback => nc.setName(user)
        case pc: PasswordCallback => pc.setPassword(Option(password).map(_.toCharArray).orNull)
        case other => throw new UnsupportedCallbackException(other)
      }
    }
  }

  def getPlainTransport(
      user: String,
      password: String,
      underlyingTransport: TTransport): TTransport = {
    val callBackHandler = new PlainCallBackHandler(user, password)
    new TSaslClientTransport(
      "PLAIN",
      null,
      null,
      null,
      Collections.emptyMap[String, String](),
      callBackHandler,
      underlyingTransport)
  }
}
