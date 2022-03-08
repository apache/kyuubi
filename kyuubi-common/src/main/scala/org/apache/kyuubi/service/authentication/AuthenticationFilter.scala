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

import java.io.IOException
import java.util.HashSet
import javax.servlet.{Filter, FilterChain, FilterConfig, ServletException, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.collection.JavaConverters._

import org.apache.hadoop.security.authentication.client.AuthenticationException

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.AUTHENTICATION_METHOD
import org.apache.kyuubi.service.authentication.AuthTypes.{KERBEROS, NOSASL}

class AuthenticationFilter(conf: KyuubiConf) extends Filter with Logging {
  import AuthenticationFilter._
  import AuthenticationHandler._

  private val authHandlers = new HashSet[AuthenticationHandler]()

  private def addAuthHandler(authHandler: AuthenticationHandler): Unit = {
    authHandler.init(conf)
    if (authHandler.authenticationSupported) {
      authHandlers.add(authHandler)
    }
  }

  override def init(filterConfig: FilterConfig): Unit = {
    val authTypes = conf.get(AUTHENTICATION_METHOD).map(AuthTypes.withName)
    val spnegoKerberosEnabled = authTypes.contains(KERBEROS)
    val basicAuthTypeOpt = {
      if (authTypes == Seq(NOSASL)) {
        authTypes.headOption
      } else {
        authTypes.filterNot(_.equals(KERBEROS)).filterNot(_.equals(NOSASL)).headOption
      }
    }
    if (spnegoKerberosEnabled) {
      val kerberosHandler = new KerberosAuthenticationHandler
      addAuthHandler(kerberosHandler)
    }
    basicAuthTypeOpt.foreach { basicAuthType =>
      val basicHandler = new BasicAuthenticationHandler(basicAuthType)
      addAuthHandler(basicHandler)

    }
    super.init(filterConfig)
  }

  /**
   * If the request has a valid authentication token it allows the request to continue to the
   * target resource, otherwise it triggers an authentication sequence using the configured
   * {@link AuthenticationHandler}.
   *
   * @param request     the request object.
   * @param response    the response object.
   * @param filterChain the filter chain object.
   * @throws IOException      thrown if an IO error occurred.
   * @throws ServletException thrown if a processing error occurred.
   */
  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      filterChain: FilterChain): Unit = {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    THREAD_LOCAL_IP_ADDRESS.set(httpRequest.getRemoteAddr)
    val httpResponse = response.asInstanceOf[HttpServletResponse]

    val authorization = httpRequest.getHeader(AUTHORIZATION_HEADER)
    var matchedHandler: AuthenticationHandler = null

    for (authHandler <- authHandlers.asScala if matchedHandler == null) {
      if (authHandler.matchAuthScheme(authorization)) {
        matchedHandler = authHandler
      }
    }

    if (matchedHandler == null) {
      debug(s"No auth scheme matched for url: ${httpRequest.getRequestURL}")
      clearAuthFilterThreadLocals()
      httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
      httpResponse.sendError(
        HttpServletResponse.SC_UNAUTHORIZED,
        s"No auth scheme matched for $authorization")
    } else {
      try {
        val authUser = matchedHandler.authenticate(httpRequest, httpResponse)
        THREAD_LOCAL_USER_NAME.set(authUser.user)
        doFilter(filterChain, httpRequest, httpResponse)
      } catch {
        case e: AuthenticationException =>
          clearAuthFilterThreadLocals()
          httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN)
          httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, e.getMessage)
      }
    }
  }

  /**
   * Delegates call to the servlet filter chain. Sub-classes my override this
   * method to perform pre and post tasks.
   *
   * @param filterChain the filter chain object.
   * @param request     the request object.
   * @param response    the response object.
   * @throws IOException      thrown if an IO error occurred.
   * @throws ServletException thrown if a processing error occurred.
   */
  @throws[IOException]
  @throws[ServletException]
  protected def doFilter(
      filterChain: FilterChain,
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    filterChain.doFilter(request, response)
  }

  override def destroy(): Unit = {
    if (!authHandlers.isEmpty) {
      authHandlers.asScala.foreach(_.destroy())
      authHandlers.clear()
    }
  }
}

object AuthenticationFilter {
  private val THREAD_LOCAL_IP_ADDRESS = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }
  private val THREAD_LOCAL_USER_NAME = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  def getUserIpAddress: String = THREAD_LOCAL_IP_ADDRESS.get

  def getUserName: String = THREAD_LOCAL_USER_NAME.get

  def clearAuthFilterThreadLocals(): Unit = {
    THREAD_LOCAL_IP_ADDRESS.remove()
    THREAD_LOCAL_USER_NAME.remove()
  }
}
