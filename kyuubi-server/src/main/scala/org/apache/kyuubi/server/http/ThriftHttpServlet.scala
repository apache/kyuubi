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

package org.apache.kyuubi.server.http

import java.io.IOException
import java.security.SecureRandom
import javax.security.sasl.AuthenticationException
import javax.servlet.ServletException
import javax.servlet.http.{Cookie, HttpServletRequest, HttpServletResponse}
import javax.ws.rs.core.NewCookie

import scala.collection.mutable

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER
import org.apache.kyuubi.server.http.util.CookieSigner
import org.apache.kyuubi.service.authentication.{AuthenticationAuditLogger, AuthenticationFilter, KyuubiAuthenticationFactory}
import org.apache.kyuubi.service.authentication.utils.HttpAuthUtils
import org.apache.kyuubi.service.authentication.utils.HttpAuthUtils.AUTHORIZATION_HEADER
import org.apache.kyuubi.shaded.thrift.TProcessor
import org.apache.kyuubi.shaded.thrift.protocol.TProtocolFactory
import org.apache.kyuubi.shaded.thrift.server.TServlet

class ThriftHttpServlet(
    processor: TProcessor,
    protocolFactory: TProtocolFactory,
    authFactory: KyuubiAuthenticationFactory,
    conf: KyuubiConf)
  extends TServlet(processor, protocolFactory) with Logging {
  // Class members for cookie based authentication.
  private var signer: CookieSigner = _
  val AUTH_COOKIE = "hive.server2.auth"
  private val RAN = new SecureRandom()
  private var isCookieAuthEnabled: Boolean = false
  private var cookieDomain: String = _
  private var cookiePath: String = _
  private var cookieMaxAge: Int = 0
  private var isCookieSecure = false
  private var isHttpOnlyCookie = false
  private val X_FORWARDED_FOR_HEADER = "X-Forwarded-For"
  private val authenticationFilter = new AuthenticationFilter(conf)
  private val XSRF_HEADER_DEFAULT = "X-XSRF-HEADER"
  private val XSRF_METHODS_TO_IGNORE_DEFAULT = Set("GET", "OPTIONS", "HEAD", "TRACE")

  override def init(): Unit = {
    isCookieAuthEnabled = conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_COOKIE_AUTH_ENABLED)
    // Initialize the cookie based authentication related variables.
    if (isCookieAuthEnabled) { // Generate the signer with secret.
      val secret = RAN.nextLong().toString
      debug("Using the random number as the secret for cookie generation " + secret)
      signer = new CookieSigner(secret.getBytes)
      cookieMaxAge = conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_COOKIE_MAX_AGE)
      cookieDomain = conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_COOKIE_DOMAIN).orNull
      cookiePath = conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_COOKIE_PATH).orNull
      // always send secure cookies for SSL mode
      isCookieSecure = conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_USE_SSL)
      isHttpOnlyCookie = conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_COOKIE_IS_HTTPONLY)
    }
    authenticationFilter.initAuthHandlers()
  }

  @throws[ServletException]
  @throws[IOException]
  override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    var clientUserName: String = null
    var requireNewCookie: Boolean = false
    var doAuth: Boolean = false
    try {
      if (conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_XSRF_FILTER_ENABLED)) {
        val continueProcessing = doXsrfFilter(request, response)
        if (!continueProcessing) {
          warn("Request did not have valid XSRF header, rejecting.")
          return
        }
      }

      // If the cookie based authentication is already enabled, parse the
      // request and validate the request cookies.
      if (isCookieAuthEnabled) {
        debug("Cookie Auth Enabled")
        clientUserName = validateCookie(request)
        requireNewCookie = clientUserName == null
        if (requireNewCookie) {
          info("Could not validate cookie sent, will try to generate a new cookie")
        } else {
          debug("Got userName From Cookie: " + clientUserName)
        }
      }

      AuthenticationFilter.HTTP_CLIENT_IP_ADDRESS.set(request.getRemoteAddr)
      AuthenticationFilter.HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.set(
        request.getHeader(conf.get(FRONTEND_PROXY_HTTP_CLIENT_IP_HEADER)))
      Option(request.getHeader(X_FORWARDED_FOR_HEADER)).map(_.split(",").toList).foreach(
        AuthenticationFilter.HTTP_FORWARDED_ADDRESSES.set)

      // If the cookie based authentication is not enabled or the request does not have a valid
      // cookie, use authentication depending on the server setup.
      if (clientUserName == null) {
        doAuth = true
        clientUserName = authenticate(request, response)
      }

      require(clientUserName != null, "No valid authorization provided")
      // Set the thread local username to be used for doAs if true
      AuthenticationFilter.HTTP_CLIENT_USER_NAME.set(clientUserName)
      // find proxy user if any from query param
      AuthenticationFilter.HTTP_CLIENT_PROXY_USER_NAME.set(
        getDoAsQueryParam(request.getQueryString))

      // Generate new cookie and add it to the response
      if (requireNewCookie && !authFactory.saslDisabled) {
        val cookieToken = HttpAuthUtils.createCookieToken(clientUserName)
        val hs2Cookie = createCookie(signer.signCookie(cookieToken))
        if (isHttpOnlyCookie) response.setHeader("SET-COOKIE", getHttpOnlyCookieHeader(hs2Cookie))
        else response.addCookie(hs2Cookie)
        info("Cookie added for clientUserName " + clientUserName)
      }
      super.doPost(request, response)
    } catch {
      case e: AuthenticationException =>
        error("Error: ", e)
        // Send a 401 to the client
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)

        // scalastyle:off println
        response.getWriter.println("Authentication Error: " + e.getMessage)
      // scalastyle:on println
      case e: Throwable =>
        error("Error: ", e)
        throw e
    } finally {
      if (doAuth) AuthenticationAuditLogger.audit(request, response)
      AuthenticationFilter.HTTP_CLIENT_USER_NAME.remove()
      AuthenticationFilter.HTTP_CLIENT_IP_ADDRESS.remove()
      AuthenticationFilter.HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.remove()
      AuthenticationFilter.HTTP_AUTH_TYPE.remove()
      AuthenticationFilter.HTTP_CLIENT_PROXY_USER_NAME.remove()
      AuthenticationFilter.HTTP_FORWARDED_ADDRESSES.remove()
    }
  }

  /**
   * Retrieves the client name from cookieString. If the cookie does not
   * correspond to a valid client, the function returns null.
   *
   * @param cookies HTTP Request cookies.
   * @return Client Username if cookieString has a HS2 Generated cookie that is currently valid.
   *         Else, returns null.
   */
  private def getClientNameFromCookie(cookies: Array[Cookie]): String = {
    // Current Cookie Name, Current Cookie Value
    var currName: String = null
    var currValue: String = null
    // Following is the main loop which iterates through all the cookies send by the client.
    // The HS2 generated cookies are of the format hive.server2.auth=<value>
    // A cookie which is identified as a hiveserver2 generated cookie is validated
    // by calling signer.verifyAndExtract(). If the validation passes, send the
    // username for which the cookie is validated to the caller. If no client side
    // cookie passes the validation, return null to the caller.
    for (currCookie <- cookies) { // Get the cookie name
      currName = currCookie.getName
      if (currName == AUTH_COOKIE) {
        // If we reached here, we have match for HS2 generated cookie
        currValue = currCookie.getValue
        // Validate the value.
        try currValue = signer.verifyAndExtract(currValue)
        catch {
          case e: IllegalArgumentException =>
            debug("Invalid cookie" + e.getMessage)
            currValue = null
        }
        // Retrieve the user name, do the final validation step.
        if (currValue != null) {
          val userName = HttpAuthUtils.getUserNameFromCookieToken(currValue)
          if (userName == null) {
            warn("Invalid cookie token " + currValue)
          } else {
            // We have found a valid cookie in the client request.
            debug("Validated the cookie for user " + userName)
            return userName
          }
        }
      }
    }
    // No valid HS2 generated cookies found, return null
    null
  }

  /**
   * Convert cookie array to human readable cookie string
   *
   * @param cookies Cookie Array
   * @return String containing all the cookies separated by a newline character.
   *         Each cookie is of the format [key]=[value]
   */
  private def toCookieStr(cookies: Array[Cookie]) = {
    val cookieStr = new mutable.StringBuilder
    for (c <- cookies) {
      cookieStr.append(c.getName).append('=').append(c.getValue).append(" ;\n")
    }
    cookieStr.toString
  }

  /**
   * Validate the request cookie. This function iterates over the request cookie headers
   * and finds a cookie that represents a valid client/server session. If it finds one, it
   * returns the client name associated with the session. Else, it returns null.
   *
   * @param request The HTTP Servlet Request send by the client
   * @return Client Username if the request has valid HS2 cookie, else returns null
   */
  private def validateCookie(request: HttpServletRequest): String = {
    // Find all the valid cookies associated with the request.
    val cookies = request.getCookies
    if (cookies == null) {
      debug("No valid cookies associated with the request " + request)
      return null
    }
    debug("Received cookies: " + toCookieStr(cookies))
    getClientNameFromCookie(cookies)
  }

  import java.io.UnsupportedEncodingException

  /**
   * Generate a server side cookie given the cookie value as the input.
   *
   * @param str Input string token.
   * @return The generated cookie.
   * @throws UnsupportedEncodingException
   */
  @throws[UnsupportedEncodingException]
  private def createCookie(str: String): Cookie = {
    debug("Cookie name = " + AUTH_COOKIE + " value = " + str)
    val cookie = new Cookie(AUTH_COOKIE, str)
    cookie.setMaxAge(cookieMaxAge)
    if (cookieDomain != null) cookie.setDomain(cookieDomain)
    if (cookiePath != null) cookie.setPath(cookiePath)
    cookie.setSecure(isCookieSecure)
    cookie
  }

  /**
   * Generate httponly cookie from HS2 cookie
   *
   * @param cookie HS2 generated cookie
   * @return The httponly cookie
   */
  private def getHttpOnlyCookieHeader(cookie: Cookie): String = {
    val newCookie = new NewCookie(
      cookie.getName,
      cookie.getValue,
      cookie.getPath,
      cookie.getDomain,
      cookie.getVersion,
      cookie.getComment,
      cookie.getMaxAge,
      cookie.getSecure)
    newCookie + "; HttpOnly"
  }

  private def authenticate(request: HttpServletRequest, response: HttpServletResponse): String = {
    val authorization = request.getHeader(AUTHORIZATION_HEADER)
    authenticationFilter.getMatchedHandler(authorization).map { authHandler =>
      AuthenticationFilter.HTTP_AUTH_TYPE.set(authHandler.authScheme.toString)
      authHandler.authenticate(request, response)
    }.orNull
  }

  private def getDoAsQueryParam(queryString: String): String = {
    debug("URL query string:" + queryString)
    if (queryString == null) return null
    val params = javax.servlet.http.HttpUtils.parseQueryString(queryString)
    val keySet = params.keySet
    keySet.forEach(key => {
      if (key.equalsIgnoreCase("doAs")) return params.get(key)(0)
    })

    null
  }

  private def doXsrfFilter(
      httpRequest: HttpServletRequest,
      response: HttpServletResponse): Boolean = {
    if (XSRF_METHODS_TO_IGNORE_DEFAULT.contains(httpRequest.getMethod)
      || httpRequest.getHeader(XSRF_HEADER_DEFAULT) != null) {
      true
    } else {
      response.sendError(
        HttpServletResponse.SC_BAD_REQUEST,
        "Missing Required Header for Vulnerability Protection")
      // scalastyle:off println
      response.getWriter.println(
        "XSRF filter denial, requests must contain header : " + XSRF_HEADER_DEFAULT)
      // scalastyle:on println
      false
    }
  }
}
