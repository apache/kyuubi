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

import javax.security.sasl.AuthenticationException

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthMethods.AuthMethod
import org.apache.kyuubi.util.ClassUtils

/**
 * This class helps select a [[PasswdAuthenticationProvider]] for a given [[AuthMethods]]
 */
object AuthenticationProviderFactory {
  @throws[AuthenticationException]
  def getAuthenticationProvider(
      method: AuthMethod,
      conf: KyuubiConf,
      isServer: Boolean = true): PasswdAuthenticationProvider = {
    if (isServer) {
      getAuthenticationProviderForServer(method, conf)
    } else {
      getAuthenticationProviderForEngine(conf)
    }
  }

  private def getAuthenticationProviderForServer(
      method: AuthMethod,
      conf: KyuubiConf): PasswdAuthenticationProvider = method match {
    case AuthMethods.NONE => new AnonymousAuthenticationProviderImpl
    case AuthMethods.LDAP => new LdapAuthenticationProviderImpl(conf)
    case AuthMethods.JDBC => new JdbcAuthenticationProviderImpl(conf)
    case AuthMethods.CUSTOM =>
      val className = conf.get(KyuubiConf.AUTHENTICATION_CUSTOM_CLASS)
      require(
        className.nonEmpty,
        "kyuubi.authentication.custom.class must be set when auth method was CUSTOM.")
      ClassUtils.createInstance(className.get, classOf[PasswdAuthenticationProvider], conf)
    case _ => throw new AuthenticationException("Not a valid authentication method")
  }

  private def getAuthenticationProviderForEngine(conf: KyuubiConf): PasswdAuthenticationProvider = {
    if (conf.get(KyuubiConf.ENGINE_SECURITY_ENABLED)) {
      new EngineSecureAuthenticationProviderImpl
    } else {
      new AnonymousAuthenticationProviderImpl
    }
  }

  def getHttpBasicAuthenticationProvider(
      method: AuthMethod,
      conf: KyuubiConf): PasswdAuthenticationProvider = method match {
    case AuthMethods.NONE => new AnonymousAuthenticationProviderImpl
    case AuthMethods.LDAP => new LdapAuthenticationProviderImpl(conf)
    case AuthMethods.JDBC => new JdbcAuthenticationProviderImpl(conf)
    case AuthMethods.CUSTOM =>
      val className = conf.get(KyuubiConf.AUTHENTICATION_CUSTOM_BASIC_CLASS)
      require(
        className.nonEmpty,
        "kyuubi.authentication.custom.basic.class must be set for http basic authentication.")
      ClassUtils.createInstance(className.get, classOf[PasswdAuthenticationProvider], conf)
    case _ => throw new AuthenticationException("Not a valid authentication method")
  }

  def getHttpBearerAuthenticationProvider(
      providerClass: String,
      conf: KyuubiConf): TokenAuthenticationProvider = {
    require(
      !StringUtils.isBlank(providerClass),
      "kyuubi.authentication.custom.bearer.class must be set for http bearer authentication.")
    ClassUtils.createInstance(providerClass, classOf[TokenAuthenticationProvider], conf)
  }
}
