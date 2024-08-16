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

package org.apache.kyuubi.server.http.authentication

import javax.security.sasl.AuthenticationException
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.AUTHENTICATION_CUSTOM_HTTP_BEARER_HEADER
import org.apache.kyuubi.server.http.authentication.AuthSchemes.AuthScheme
import org.apache.kyuubi.server.http.util.HttpAuthUtils
import org.apache.kyuubi.server.http.util.HttpAuthUtils.{AUTHORIZATION_HEADER, WWW_AUTHENTICATE_HEADER}
import org.apache.kyuubi.service.authentication.{AnonymousAuthenticationProviderImpl, AuthenticationProviderFactory, DefaultTokenCredential, TokenAuthenticationProvider}

class BearerAuthenticationHandler(providerClass: String)
  extends AuthenticationHandler with Logging {
  private var conf: KyuubiConf = _
  private val allowAnonymous = classOf[AnonymousAuthenticationProviderImpl].getName == providerClass

  override val authScheme: AuthScheme = AuthSchemes.BEARER

  override def init(conf: KyuubiConf): Unit = {
    this.conf = conf
  }

  override def authenticationSupported: Boolean = {
    Option(providerClass).exists { _ =>
      try {
        Class.forName(providerClass).isAssignableFrom(classOf[TokenAuthenticationProvider])
        true
      } catch {
        case _: Throwable => false
      }
    }
  }

  override def matchAuthScheme(authorization: String): Boolean = {
    if (authorization == null || authorization.isEmpty) {
      allowAnonymous
    } else {
      super.matchAuthScheme(authorization)
    }
  }

  override def getAuthorization(request: HttpServletRequest): String = {
    // Due to HIVE-22655, pass bearer token via a customized header for thrift protocol
    var authHeader: String = request.getHeader(
      conf.get(AUTHENTICATION_CUSTOM_HTTP_BEARER_HEADER))
    if (authHeader == null || authHeader.isEmpty) {
      authHeader = request.getHeader(AUTHORIZATION_HEADER)
      if (allowAnonymous && (authHeader == null || authHeader.isEmpty)) {
        ""
      } else {
        super.getAuthorization(request)
      }
    } else {
      val authorization = authHeader.substring(authScheme.toString.length).trim
      if (authorization == null || authorization.isEmpty) {
        throw new AuthenticationException(
          "Authorization header received from the client does not contain any data.")
      }
      authorization
    }
  }

  override def authenticate(
      request: HttpServletRequest,
      response: HttpServletResponse): String = {
    var principal: String = null
    val inputToken = getAuthorization(request)

    if (!allowAnonymous && StringUtils.isBlank(inputToken)) {
      response.setHeader(WWW_AUTHENTICATE_HEADER, authScheme.toString)
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
    } else {
      val credential = DefaultTokenCredential(inputToken, HttpAuthUtils.getCredentialExtraInfo)
      principal = AuthenticationProviderFactory
        .getHttpBearerAuthenticationProvider(providerClass, conf)
        .authenticate(credential).getName
      response.setStatus(HttpServletResponse.SC_OK)
    }
    principal
  }

  override def destroy(): Unit = {}
}
