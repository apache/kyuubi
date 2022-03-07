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
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthSchemes.AuthScheme

trait AuthenticationHandler {
  import AuthenticationHandler._

  val authScheme: AuthScheme

  def init(conf: KyuubiConf): Unit

  def destroy(): Unit

  @throws[AuthenticationException]
  def authenticate(request: HttpServletRequest, response: HttpServletResponse): AuthUser

  def matchAuthScheme(authorization: String): Boolean = {
    if (authorization == null || authorization.isEmpty) {
      false
    } else {
      authorization.trim.regionMatches(true, 0, authScheme.toString, 0, authScheme.toString.length)
    }
  }

  def beforeAuth(request: HttpServletRequest): Unit = {
    require(
      matchAuthScheme(request.getHeader(AUTHORIZATION_HEADER)),
      s"Auth schema[$authScheme] is not matched")
  }

  def getAuthorization(request: HttpServletRequest): String = {
    val authHeader = request.getHeader(AUTHORIZATION_HEADER)
    // each http request must have an Authorization header
    if (authHeader == null || authHeader.isEmpty) {
      throw new AuthenticationException("Authorization header received from the client is empty.")
    }

    val authorization = authHeader.substring(authScheme.toString.length).trim
    // Authorization header must have a payload
    if (authorization == null || authorization.isEmpty()) {
      throw new AuthenticationException(
        "Authorization header received from the client does not contain any data.")
    }
    authorization
  }
}

object AuthenticationHandler {
  final val AUTHORIZATION_HEADER = "Authorization"
}

case class AuthUser(user: String, password: String)
