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

import java.io.IOException
import java.security.Security
import javax.security.auth.callback._
import javax.security.auth.login.LoginException
import javax.security.sasl.{AuthenticationException, AuthorizeCallback}

import scala.collection.JavaConverters._

import org.apache.hive.service.auth.AuthenticationProviderFactory
import org.apache.hive.service.auth.AuthenticationProviderFactory.AuthMethods
import org.apache.hive.service.cli.thrift.TCLIService.Iface
import org.apache.thrift.{TProcessor, TProcessorFactory}
import org.apache.thrift.transport.{TSaslServerTransport, TTransport, TTransportFactory}

import yaooqinn.kyuubi.auth.PlainSaslServer.SaslPlainProvider

object PlainSaslHelper {

  // Register Plain SASL server provider

  Security.addProvider(new SaslPlainProvider())

  def getProcessFactory(service: Iface): TProcessorFactory = {
    SQLPlainProcessorFactory(service)
  }

  @throws[LoginException]
  def getTransportFactory(authTypeStr: String): TTransportFactory = {
    val saslFactory = new TSaslServerTransport.Factory()
    try {
      val handler = new PlainServerCallbackHandler(authTypeStr)
      val props = Map.empty[String, String]
      saslFactory.addServerDefinition("PLAIN", authTypeStr, null, props.asJava, handler)
    } catch {
      case e: AuthenticationException =>
      throw new LoginException("Error setting callback handler" + e);
    }
    saslFactory
  }

  private class PlainServerCallbackHandler private(authMethod: AuthMethods)
    extends CallbackHandler {
    @throws[AuthenticationException]
    def this(authMethodStr: String) = this(AuthMethods.getValidAuthMethod(authMethodStr))

    @throws[IOException]
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
      val provider = AuthenticationProviderFactory.getAuthenticationProvider(authMethod)
      provider.Authenticate(username, password)
      if (ac != null) ac.setAuthorized(true)
    }
  }

  private case class SQLPlainProcessorFactory(service: Iface) extends TProcessorFactory(null) {
    override def getProcessor(trans: TTransport): TProcessor =
      new TSetIpAddressProcessor[Iface](service)
  }
}
