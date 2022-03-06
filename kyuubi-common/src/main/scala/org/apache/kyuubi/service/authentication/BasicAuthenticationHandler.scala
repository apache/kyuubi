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

import java.nio.charset.Charset
import javax.security.sasl.AuthenticationException
import javax.servlet.http.HttpServletRequest

import org.apache.commons.codec.binary.Base64

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthSchemes.AuthScheme
import org.apache.kyuubi.service.authentication.AuthTypes._

class BasicAuthenticationHandler(basicAuthType: AuthTypes.AuthType)
  extends AuthenticationHandler with Logging {
  private var conf: KyuubiConf = _

  override val authScheme: AuthScheme = AuthSchemes.BASIC

  override def init(conf: KyuubiConf): Unit = {
    this.conf = conf
  }

  override def authenticate(request: HttpServletRequest): AuthUser = {
    beforeAuth(request)
    val authorization = getAuthorization(request)
    val inputToken = Base64.decodeBase64(authorization.getBytes())
    val creds = new String(inputToken, Charset.forName("UTF-8")).split(":")

    basicAuthType match {
      case NOSASL | NONE =>
        val user = creds.take(1).headOption.getOrElse("anonymous")
        AuthUser(user, "")

      case authType =>
        if (creds.size < 2 || creds(0).trim.isEmpty || creds(1).trim.isEmpty) {
          throw new AuthenticationException(
            "Authorization header received from the client does not contain username or password.")
        }
        val Seq(user, password) = creds.toSeq.take(2)
        val passwdAuthenticationProvider = AuthenticationProviderFactory
          .getAuthenticationProvider(AuthMethods.withName(authType.toString), conf)
        passwdAuthenticationProvider.authenticate(user, password)
        AuthUser(user, password)
    }
  }
}
