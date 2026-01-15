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
import org.apache.kyuubi.service.authentication.utils.HttpAuthUtils.AUTHORIZATION_HEADER

trait AuthenticationHandler {

  /**
   * HTTP header prefix used during the authentication sequence.
   */
  val authScheme: AuthScheme

  /**
   *  Initializes the authentication handler instance.
   */
  def init(conf: KyuubiConf): Unit

  /**
   * Whether this authentication handler is configured well and support authentication.
   */
  def authenticationSupported: Boolean

  /**
   * Destroys the authentication handler instance.
   * <p>
   * This method is invoked by the [[AuthenticationFilter.destroy]] method.
   */
  def destroy(): Unit

  /**
   * Performs an authentication step for the given HTTP client request.
   * <p>
   * This method is invoked by the [[AuthenticationFilter]] only if the HTTP client request is
   * not yet authenticated.
   * <p>
   * Depending upon the authentication mechanism being implemented, a particular HTTP client may
   * end up making a sequence of invocations before authentication is successfully established
   * (this is the case of Kerberos SPNEGO).
   * <p>
   * This method must return a user name only if the the HTTP client request has been successfully
   * and fully authenticated.
   * <p>
   * If the HTTP client request has not been completely authenticated, this method must take over
   * the corresponding HTTP response and it must return <code>null</code>.
   *
   * @param request  the HTTP client request.
   * @param response the HTTP client response.
   * @return the user name
   * @throws AuthenticationException thrown if an Authentication error occurred.
   */
  def authenticate(request: HttpServletRequest, response: HttpServletResponse): String

  /**
   * This method checks if the specified <code>authorization</code> belongs to the
   * auth schema of current authentication handler.
   * @param authorization Authentication header value which is to be compared with the
   *                      authentication scheme.
   */
  def matchAuthScheme(authorization: String): Boolean = {
    if (authorization == null || authorization.isEmpty) {
      false
    } else {
      authorization.trim.regionMatches(true, 0, authScheme.toString, 0, authScheme.toString.length)
    }
  }

  /**
   * Get the decoded authorization value after auth schema in Authentication header.
   */
  def getAuthorization(request: HttpServletRequest): String = {
    val authHeader = request.getHeader(AUTHORIZATION_HEADER)
    // each http request must have an Authorization header
    if (authHeader == null || authHeader.isEmpty) {
      throw new AuthenticationException("Authorization header received from the client is empty.")
    }

    var authorization = authHeader.substring(authScheme.toString.length).trim
    // For thrift http spnego authorization, its format is 'NEGOTIATE : $token', see HIVE-26353
    if (authorization.startsWith(":")) {
      authorization = authorization.stripPrefix(":").trim
    }
    // Authorization header must have a payload
    if (authorization == null || authorization.isEmpty) {
      throw new AuthenticationException(
        "Authorization header received from the client does not contain any data.")
    }
    authorization
  }
}
