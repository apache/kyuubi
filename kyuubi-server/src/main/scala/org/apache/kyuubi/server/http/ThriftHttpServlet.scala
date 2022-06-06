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
import java.security.PrivilegedExceptionAction
import java.security.SecureRandom
import javax.security.sasl.AuthenticationException
import javax.servlet.ServletException
import javax.servlet.http.Cookie
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.NewCookie

import scala.collection.mutable

import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.binary.StringUtils
import org.apache.hadoop.hive.shims.HadoopShims.KerberosNameShim
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.thrift.TProcessor
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.server.TServlet
import org.ietf.jgss.GSSContext
import org.ietf.jgss.GSSCredential
import org.ietf.jgss.GSSException
import org.ietf.jgss.GSSManager
import org.ietf.jgss.Oid

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.http.authentication.KerberosAuthenticationHandler
import org.apache.kyuubi.server.http.util.{CookieSigner, HttpAuthUtils, SessionManager}
import org.apache.kyuubi.service.authentication.{AuthenticationProviderFactory, KyuubiAuthenticationFactory}

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
  private val HIVE_DELEGATION_TOKEN_HEADER = "X-Hive-Delegation-Token"
  private val X_FORWARDED_FOR = "X-Forwarded-For"
  private val kerberosAuthHandler = new KerberosAuthenticationHandler()

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

    kerberosAuthHandler.init(conf)
  }

  @throws[ServletException]
  @throws[IOException]
  override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    var clientUserName: String = null
    var clientIpAddress: String = null
    var requireNewCookie: Boolean = false
    try {
      if (conf.get(KyuubiConf.FRONTEND_THRIFT_HTTP_XSRF_FILTER_ENABLED)) {
        val continueProcessing = Utils.doXsrfFilter(request, response, null, null)
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

      // If the cookie based authentication is not enabled or the request does not have a valid
      // cookie, use authentication depending on the server setup.
      if (clientUserName == null) {
        // For a kerberos setup
        if (authFactory.isKerberosEnabled) {
          val delegationToken = request.getHeader(HIVE_DELEGATION_TOKEN_HEADER)
          // Each http request must have an Authorization header
          if ((delegationToken != null) && delegationToken.nonEmpty) {
            clientUserName = doTokenAuth(request)
          } else {
            clientUserName = doKerberosAuth(request, response)
          }
        } else {
          clientUserName = doPasswdAuth(request, authFactory)
        }
      }

      assert(clientUserName != null)
      debug("Client username: " + clientUserName)

      // Set the thread local username to be used for doAs if true
      SessionManager.setUserName(clientUserName)

      // find proxy user if any from query param
      val doAsQueryParam = getDoAsQueryParam(request.getQueryString)
      if (doAsQueryParam != null) SessionManager.setProxyUserName(doAsQueryParam)

      clientIpAddress = request.getRemoteAddr
      debug("Client IP Address: " + clientIpAddress)
      // Set the thread local ip address
      SessionManager.setIpAddress(clientIpAddress)

      val forwarded_for = request.getHeader(X_FORWARDED_FOR)
      if (forwarded_for != null) {
        debug(X_FORWARDED_FOR + ":" + forwarded_for)
        val forwardedAddresses = forwarded_for.split(",").toList
        SessionManager.setForwardedAddresses(forwardedAddresses)
      } else SessionManager.setForwardedAddresses(List.empty[String])

      // Generate new cookie and add it to the response
      if (requireNewCookie && !authFactory.isNoSaslEnabled) {
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
        if (authFactory.isKerberosEnabled) {
          response.addHeader(HttpAuthUtils.WWW_AUTHENTICATE, HttpAuthUtils.NEGOTIATE)
        }

        // scalastyle:off println
        response.getWriter.println("Authentication Error: " + e.getMessage)
      // scalastyle:on println
      case e: Throwable =>
        error("Error: ", e)
        throw e
    } finally {
      // Clear the thread locals
      SessionManager.clearUserName()
      SessionManager.clearIpAddress()
      SessionManager.clearProxyUserName()
      SessionManager.clearForwardedAddresses()
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

  /**
   * Do the LDAP/PAM authentication
   *
   * @param request
   * @param authFactory
   * @throws AuthenticationException
   */
  @throws[AuthenticationException]
  private def doPasswdAuth(
      request: HttpServletRequest,
      authFactory: KyuubiAuthenticationFactory): String = {
    val userName = getUsername(request, authFactory: KyuubiAuthenticationFactory)
    debug("Is No SASL Enabled : " + authFactory.isNoSaslEnabled)
    // No-op when authType is NOSASL
    if (authFactory.isNoSaslEnabled) return userName
    try {
      debug("Initiating Password Authentication")
      val password = getPassword(request, authFactory)
      val authMethod = authFactory.getValidPasswordAuthMethod
      debug("Password Method: " + authMethod)
      val provider = AuthenticationProviderFactory.getAuthenticationProvider(authMethod, conf)
      debug("Password Provider obtained")
      provider.authenticate(userName, password)
      debug("Password Provider authenticated username successfully")
    } catch {
      case e: Exception =>
        throw new AuthenticationException(e.getMessage, e)
    }
    userName
  }

  @throws[AuthenticationException]
  private def doTokenAuth(request: HttpServletRequest): String = {
//    debug("Token Auth Initiated")
//    val tokenStr = request.getHeader(HIVE_DELEGATION_TOKEN_HEADER)
//    debug("token is: " + tokenStr)
//    try {
//      // TODO: FIXME: TokenAuth
//      val hiveAuthFactory = new HiveAuthFactory(new HiveConf())
//      hiveAuthFactory.verifyDelegationToken(tokenStr)
//    } catch {
//      case e: HiveSQLException =>
//        throw new AuthenticationException(e)
//    }
    // TODO: FIXME: support tokenAuth in Kerberos
    throw new AuthenticationException("Token Auth is not supported")
  }

  /**
   * Do the GSS-API kerberos authentication.
   * We already have a logged in subject in the form of serviceUGI,
   * which GSS-API will extract information from.
   * In case of a SPNego request we use the httpUGI,
   * for the authenticating service tickets.
   *
   * @param request
   * @return
   * @throws AuthenticationException
   */
  @throws[AuthenticationException]
  private def doKerberosAuth(request: HttpServletRequest, response: HttpServletResponse): String = {
    kerberosAuthHandler.authenticate(request, response)
  }

  class HttpKerberosServerAction(
      var request: HttpServletRequest,
      var serviceUGI: UserGroupInformation)
    extends PrivilegedExceptionAction[String] {
    @throws[AuthenticationException]
    override def run: String = { // Get own Kerberos credentials for accepting connection
      val manager = GSSManager.getInstance
      var gssContext: GSSContext = null
      val serverPrincipal = getPrincipalWithoutRealm(serviceUGI.getUserName)

      try { // This Oid for Kerberos GSS-API mechanism.
        val kerberosMechOid = new Oid("1.2.840.113554.1.2.2")
        // Oid for SPNego GSS-API mechanism.
        val spnegoMechOid = new Oid("1.3.6.1.5.5.2")
        // Oid for kerberos principal name
        val krb5PrincipalOid = new Oid("1.2.840.113554.1.2.2.1")

        // GSS name for server
        val serverName = manager.createName(serverPrincipal, krb5PrincipalOid)

        // GSS credentials for server
        val serverCreds = manager.createCredential(
          serverName,
          GSSCredential.DEFAULT_LIFETIME,
          Array[Oid](kerberosMechOid, spnegoMechOid),
          GSSCredential.ACCEPT_ONLY)

        // Create a GSS context
        gssContext = manager.createContext(serverCreds)
        // Get service ticket from the authorization header
        val serviceTicketBase64 = getAuthHeader(request, authFactory)
        val inToken = Base64.decodeBase64(serviceTicketBase64.getBytes)
        gssContext.acceptSecContext(inToken, 0, inToken.length)
        // Authenticate or deny based on its context completion
        if (!gssContext.isEstablished) {
          throw new AuthenticationException("Kerberos authentication failed: " +
            "unable to establish context with the service ticket " +
            "provided by the client.")
        } else getPrincipalWithoutRealmAndHost(gssContext.getSrcName.toString)
      } catch {
        case e: GSSException =>
          throw new AuthenticationException("Kerberos authentication failed: ", e)
      } finally {
        if (gssContext != null) {
          try gssContext.dispose()
          catch {
            case _: GSSException => // No-op
          }
        }
      }
    }

    @throws[AuthenticationException]
    private def getPrincipalWithoutRealm(fullPrincipal: String): String = {
      var fullKerberosName: KerberosNameShim = null
      try fullKerberosName = ShimLoader.getHadoopShims.getKerberosNameShim(fullPrincipal)
      catch {
        case e: IOException =>
          throw new AuthenticationException(e.getMessage, e)
      }
      val serviceName = fullKerberosName.getServiceName
      val hostName = fullKerberosName.getHostName
      var principalWithoutRealm = serviceName
      if (hostName != null) {
        principalWithoutRealm = serviceName + "/" + hostName
      }

      principalWithoutRealm
    }

    @throws[AuthenticationException]
    private def getPrincipalWithoutRealmAndHost(fullPrincipal: String): String = {
      var fullKerberosName: KerberosNameShim = null
      try {
        fullKerberosName = ShimLoader.getHadoopShims.getKerberosNameShim(fullPrincipal)
        fullKerberosName.getShortName
      } catch {
        case e: IOException =>
          throw new AuthenticationException(e.getMessage, e)
      }
    }
  }

  @throws[AuthenticationException]
  private def getUsername(
      request: HttpServletRequest,
      authFactory: KyuubiAuthenticationFactory): String = {
    val creds = getAuthHeaderTokens(request, authFactory)
    // Username must be present
    if (creds(0) == null || creds(0).isEmpty) {
      throw new AuthenticationException("Authorization header received " +
        "from the client does not contain username.")
    }
    creds(0)
  }

  @throws[AuthenticationException]
  private def getPassword(
      request: HttpServletRequest,
      authFactory: KyuubiAuthenticationFactory): String = {
    val creds = getAuthHeaderTokens(request, authFactory)
    // Password must be present
    if (creds(1) == null || creds(1).isEmpty) {
      throw new AuthenticationException("Authorization header received " +
        "from the client does not contain username.")
    }
    creds(1)
  }

  @throws[AuthenticationException]
  private def getAuthHeaderTokens(
      request: HttpServletRequest,
      authFactory: KyuubiAuthenticationFactory): Array[String] = {
    val authHeaderBase64 = getAuthHeader(request, authFactory)
    val authHeaderString = StringUtils.newStringUtf8(Base64.decodeBase64(authHeaderBase64.getBytes))
    authHeaderString.split(":")
  }

  /**
   * Returns the base64 encoded auth header payload
   *
   * @param request
   * @param authFactory
   * @return
   * @throws AuthenticationException
   */
  @throws[AuthenticationException]
  private def getAuthHeader(
      request: HttpServletRequest,
      authFactory: KyuubiAuthenticationFactory): String = {
    val authHeader = request.getHeader(HttpAuthUtils.AUTHORIZATION)
    // Each http request must have an Authorization header
    if (authHeader == null || authHeader.isEmpty) {
      throw new AuthenticationException("Authorization header received " +
        "from the client is empty.")
    }

    var authHeaderBase64String: String = null
    var beginIndex = 0
    if (isKerberosAuthMode(authFactory)) beginIndex = (HttpAuthUtils.NEGOTIATE + " ").length
    else beginIndex = (HttpAuthUtils.BASIC + " ").length
    authHeaderBase64String = authHeader.substring(beginIndex)
    // Authorization header must have a payload
    if (authHeaderBase64String == null || authHeaderBase64String.isEmpty) {
      throw new AuthenticationException("Authorization header received " +
        "from the client does not contain any data.")
    }
    authHeaderBase64String
  }

  private def isKerberosAuthMode(authFactory: KyuubiAuthenticationFactory) =
    authFactory.isKerberosEnabled

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
}
