package org.apache.kyuubi.service.authentication.utils

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util
import java.util.{Base64, StringTokenizer}

import scala.collection.mutable

import org.apache.kyuubi.Logging
import org.apache.kyuubi.service.authentication.{AuthenticationFilter, AuthSchemes, Credential}

object HttpAuthUtils extends Logging {
  // HTTP header used by the server endpoint during an authentication sequence.
  val WWW_AUTHENTICATE_HEADER = "WWW-Authenticate"
  // HTTP header used by the client endpoint during an authentication sequence.
  val AUTHORIZATION_HEADER = "Authorization"
  // HTTP header prefix used by the SPNEGO client/server endpoints during an
  // authentication sequence.
  val NEGOTIATE = "Negotiate"
  // HTTP header prefix used during the Basic authentication sequence.
  val BASIC = "Basic"
  // HTTP header prefix used during the Basic authentication sequence.
  val DIGEST = "Digest"

  // RFC 7617: The 'Basic' HTTP Authentication Scheme
  def basicAuthorizationHeader(userId: String, password: String = "none"): String =
    "BASIC " + new String(
      Base64.getEncoder.encode(s"$userId:$password".getBytes()),
      StandardCharsets.UTF_8)

  def bearerAuthorizationHeader(token: String): String = AuthSchemes.BEARER + " " + token

  private val COOKIE_ATTR_SEPARATOR = "&"
  private val COOKIE_CLIENT_USER_NAME = "cu"
  private val COOKIE_CLIENT_RAND_NUMBER = "rn"
  private val COOKIE_KEY_VALUE_SEPARATOR = "="
  private val COOKIE_ATTRIBUTES = new util.HashSet[String](
    util.Arrays.asList(COOKIE_CLIENT_USER_NAME, COOKIE_CLIENT_RAND_NUMBER))

  /**
   * Creates and returns a HS2 cookie token.
   *
   * @param clientUserName Client User name.
   * @return An unsigned cookie token generated from input parameters.
   *         The final cookie generated is of the following format :
   *         cu=<username>&rn=<randomNumber>&s=<cookieSignature>
   */
  def createCookieToken(clientUserName: String): String = {
    val sb = new mutable.StringBuilder
    sb.append(COOKIE_CLIENT_USER_NAME).append(COOKIE_KEY_VALUE_SEPARATOR)
      .append(clientUserName).append(COOKIE_ATTR_SEPARATOR).append(COOKIE_CLIENT_RAND_NUMBER)
      .append(COOKIE_KEY_VALUE_SEPARATOR).append((new SecureRandom).nextLong)
    sb.toString
  }

  /**
   * Parses a cookie token to retrieve client user name.
   *
   * @param tokenStr Token String.
   * @return A valid user name if input is of valid format, else returns null.
   */
  def getUserNameFromCookieToken(tokenStr: String): String = {
    val map = splitCookieToken(tokenStr)
    if (!(map.keySet == COOKIE_ATTRIBUTES)) {
      error("Invalid token with missing attributes " + tokenStr)
      return null
    }
    map.get(COOKIE_CLIENT_USER_NAME)
  }

  /**
   * Splits the cookie token into attributes pairs.
   *
   * @param tokenStr input token.
   * @return a map with the attribute pairs of the token if the input is valid.
   *         Else, returns null.
   */
  private def splitCookieToken(tokenStr: String): util.Map[String, String] = {
    val map = new util.HashMap[String, String]
    val st = new StringTokenizer(tokenStr, COOKIE_ATTR_SEPARATOR)
    while ({
      st.hasMoreTokens
    }) {
      val part = st.nextToken
      val separator = part.indexOf(COOKIE_KEY_VALUE_SEPARATOR)
      if (separator == -1) {
        error("Invalid token string " + tokenStr)
        return null
      }
      val key = part.substring(0, separator)
      val value = part.substring(separator + 1)
      map.put(key, value)
    }
    map
  }

  def getCredentialExtraInfo: Map[String, String] = {
    Map(Credential.CLIENT_IP_KEY ->
      Option(
        AuthenticationFilter.HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.get()).getOrElse(
        AuthenticationFilter.HTTP_CLIENT_IP_ADDRESS.get()))
  }
}
