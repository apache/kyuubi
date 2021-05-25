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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthMethods.AuthMethod

/**
 * This class helps select a [[PasswdAuthenticationProvider]] for a given [[AuthMethods]]
 */
object AuthenticationProviderFactory {
  @throws[AuthenticationException]
  def getAuthenticationProvider(
      method: AuthMethod,
      conf: KyuubiConf): PasswdAuthenticationProvider = method match {
    case AuthMethods.NONE => new AnonymousAuthenticationProviderImpl
    case AuthMethods.LDAP => new LdapAuthenticationProviderImpl(conf)
    case AuthMethods.CUSTOM =>
      val classLoader = Thread.currentThread.getContextClassLoader
      val className = conf.get(KyuubiConf.AUTHENTICATION_CUSTOM_CLASS)
      if (!className.isDefined) {
        throw new AuthenticationException(
          "authentication.custom.class must be set when auth method was CUSTOM.")
      }
      val cls = Class.forName(className.get, true, classLoader)
      cls match {
        case c if classOf[PasswdAuthenticationProvider].isAssignableFrom(cls) =>
          val confConstructor = c.getConstructors.exists(p => {
            val params = p.getParameterTypes
            params.length == 1 && classOf[KyuubiConf].isAssignableFrom(params(0))
          })
          if (confConstructor) {
            c.getConstructor(classOf[KyuubiConf]).newInstance(conf)
              .asInstanceOf[PasswdAuthenticationProvider]
          } else {
            c.newInstance().asInstanceOf[PasswdAuthenticationProvider]
          }
        case _ => throw new AuthenticationException(
          s"$className must extend of PasswdAuthenticationProvider.")
      }
    case _ => throw new AuthenticationException("Not a valid authentication method")
  }
}
