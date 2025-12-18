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

import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthSchemes.AuthScheme
import org.apache.kyuubi.service.authentication.utils.HttpAuthUtils.WWW_AUTHENTICATE_HEADER

class KyuubiInternalAuthenticationHandler extends AuthenticationHandler with Logging {

  private var conf: KyuubiConf = _
  override val authScheme: AuthScheme = AuthSchemes.KYUUBI_INTERNAL
  private val internalSecurityAccessor = InternalSecurityAccessor.get()

  override def init(conf: KyuubiConf): Unit = {
    this.conf = conf
  }

  override def authenticationSupported: Boolean = {
    internalSecurityAccessor != null
  }

  override def authenticate(
      request: HttpServletRequest,
      response: HttpServletResponse): String = {
    var authUser: String = null
    val authorization = getAuthorization(request)
    val inputToken = Option(authorization).map(a => Base64.getDecoder.decode(a.getBytes()))
      .getOrElse(Array.empty[Byte])
    val creds = new String(inputToken, StandardCharsets.UTF_8).split(":")

    if (creds.size < 2 || creds(0).trim.isEmpty || creds(1).trim.isEmpty) {
      response.setHeader(WWW_AUTHENTICATE_HEADER, authScheme.toString)
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
    } else {
      val Seq(user, password) = creds.toSeq.take(2)
      internalSecurityAccessor.authToken(password)
      response.setStatus(HttpServletResponse.SC_OK)
      authUser = user
    }
    authUser
  }

  override def destroy(): Unit = {}
}
