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

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthSchemes.AuthScheme
import org.apache.kyuubi.service.authentication.utils.HttpAuthUtils
import org.apache.kyuubi.service.authentication.utils.HttpAuthUtils.{AUTHORIZATION_HEADER, WWW_AUTHENTICATE_HEADER}

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
    val authHeader = request.getHeader(AUTHORIZATION_HEADER)
    if (allowAnonymous && (authHeader == null || authHeader.isEmpty)) {
      ""
    } else {
      super.getAuthorization(request)
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
