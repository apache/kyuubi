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
package org.apache.kyuubi.metrics

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64
import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.kyuubi.Logging

/**
 * A servlet filter that implements HTTP Basic Authentication
 */
class BasicAuthFilter(username: String, password: String)
  extends Filter with Logging {

  private val credentials = s"$username:$password"
  private val encodedCredentials =
    Base64.getEncoder.encodeToString(credentials.getBytes(StandardCharsets.UTF_8))

  override def init(filterConfig: FilterConfig): Unit = {
    info("BasicAuthFilter initialized for Prometheus metrics endpoint")
  }

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain): Unit = {

    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val httpResponse = response.asInstanceOf[HttpServletResponse]

    val authHeader = httpRequest.getHeader("Authorization")

    if (authHeader != null && authHeader.startsWith("Basic ")) {
      val providedCredentials = authHeader.substring(6) // Remove "Basic " prefix

      if (isValidCredentials(providedCredentials)) {
        // Authentication successful, continue the chain
        chain.doFilter(request, response)
      } else {
        // Invalid credentials
        warn(s"Invalid credentials provided from ${httpRequest.getRemoteAddr}")
        sendUnauthorizedResponse(httpResponse)
      }
    } else {
      // No credentials provided
      debug(s"No credentials provided from ${httpRequest.getRemoteAddr}")
      sendUnauthorizedResponse(httpResponse)
    }
  }

  override def destroy(): Unit = {
    info("BasicAuthFilter destroyed")
  }

  private def isValidCredentials(providedCredentials: String): Boolean = {
    try {
      // Use Java's constant-time comparison to prevent timing attacks
      MessageDigest.isEqual(
        providedCredentials.getBytes(StandardCharsets.UTF_8),
        encodedCredentials.getBytes(StandardCharsets.UTF_8))
    } catch {
      case e: Exception =>
        error("Error validating credentials", e)
        false
    }
  }

  private def sendUnauthorizedResponse(response: HttpServletResponse): Unit = {
    response.setHeader("WWW-Authenticate", "Basic realm=\"Kyuubi Prometheus Metrics\"")
    response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized")
  }
}
