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

package org.apache.kyuubi.server.flight

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import com.google.common.cache.CacheBuilder
import org.apache.arrow.flight.{CallHeaders, CallStatus}
import org.apache.arrow.flight.auth2.{Auth2Constants, CallHeaderAuthenticator, GeneratedBearerTokenAuthenticator}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{AUTHENTICATION_METHOD, FRONTEND_FLIGHT_SQL_TOKEN_TTL}
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.service.authentication.{AuthenticationProviderFactory, AuthMethods, AuthTypes, AuthUtils}

/**
 * Flight CallHeaderAuthenticator supporting:
 * - Basic username/password (LDAP/JDBC/CUSTOM/NONE providers)
 * - SPNEGO Negotiate bootstrap using [[FlightSqlKerberosValidator]]
 * - Bearer tokens issued after successful Basic/Negotiate authentication
 */
object KyuubiFlightAuthHandler {
  private val NEGOTIATE_PREFIX = "Negotiate "

  def create(conf: KyuubiConf): CallHeaderAuthenticator = {
    val delegate = new KyuubiFlightAuthHandler(conf)
    if (delegate.issuesBearerTokens) {
      val ttlMs = conf.get(FRONTEND_FLIGHT_SQL_TOKEN_TTL)
      val ttlMinutes = math.max(1L, TimeUnit.MILLISECONDS.toMinutes(ttlMs))
      new GeneratedBearerTokenAuthenticator(
        delegate,
        CacheBuilder.newBuilder().expireAfterAccess(ttlMinutes, TimeUnit.MINUTES))
    } else {
      delegate
    }
  }
}

class KyuubiFlightAuthHandler(conf: KyuubiConf)
  extends CallHeaderAuthenticator with Logging {

  private val authTypes =
    conf.get(AUTHENTICATION_METHOD).map(value => AuthTypes.withName(value))

  private val noAuthRequired =
    AuthUtils.saslDisabled(authTypes) ||
      AuthUtils.effectivePlainAuthType(authTypes).contains(AuthTypes.NONE)

  private val passwordProvider = AuthUtils.effectivePlainAuthType(authTypes)
    .filterNot(_ == AuthTypes.NONE)
    .map(authType =>
      AuthenticationProviderFactory.getAuthenticationProvider(
        AuthMethods.withName(authType.toString),
        conf,
        isServer = true))

  private val kerberosEnabled = AuthUtils.kerberosEnabled(authTypes)

  private lazy val kerberosValidator: Option[FlightSqlKerberosValidator] =
    if (kerberosEnabled) {
      try Some(new FlightSqlKerberosValidator(conf))
      catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            "Flight SQL Kerberos is enabled but SPNEGO principal/keytab are not usable",
            e)
      }
    } else {
      None
    }

  private[flight] def issuesBearerTokens: Boolean =
    passwordProvider.isDefined || kerberosValidator.isDefined

  override def authenticate(headers: CallHeaders): CallHeaderAuthenticator.AuthResult = {
    val authorization = Option(headers.get(Auth2Constants.AUTHORIZATION_HEADER))
    try {
      val result = authorization match {
        case Some(value) if value.startsWith(Auth2Constants.BASIC_PREFIX) =>
          authenticateBasic(value.stripPrefix(Auth2Constants.BASIC_PREFIX))
        case Some(value)
            if value.regionMatches(
              true,
              0,
              KyuubiFlightAuthHandler.NEGOTIATE_PREFIX,
              0,
              KyuubiFlightAuthHandler.NEGOTIATE_PREFIX.length) =>
          authenticateNegotiate(
            value.substring(KyuubiFlightAuthHandler.NEGOTIATE_PREFIX.length))
        case Some(value) if value.startsWith(Auth2Constants.BEARER_PREFIX) =>
          // Bearer validation is handled by GeneratedBearerTokenAuthenticator when enabled.
          throw CallStatus.UNAUTHENTICATED
            .withDescription("Bearer authentication requires a previously issued Flight token")
            .toRuntimeException
        case Some(_) =>
          throw CallStatus.UNAUTHENTICATED
            .withDescription("Unsupported Flight SQL authorization scheme")
            .toRuntimeException
        case None if noAuthRequired =>
          val user = Option(headers.get("x-user-name"))
            .filter(_.nonEmpty)
            .getOrElse("anonymous")
          authResult(user)
        case None =>
          throw CallStatus.UNAUTHENTICATED
            .withDescription("Missing Flight SQL authorization header")
            .toRuntimeException
      }
      MetricsSystem.tracing { ms =>
        ms.incCount(MetricsConstants.FLIGHT_SQL_CONN_TOTAL)
        ms.incCount(MetricsConstants.FLIGHT_SQL_CONN_OPEN)
      }
      result
    } catch {
      case e: RuntimeException =>
        MetricsSystem.tracing(_.incCount(MetricsConstants.FLIGHT_SQL_CONN_FAIL))
        throw e
    }
  }

  private def authenticateBasic(encoded: String): CallHeaderAuthenticator.AuthResult = {
    if (passwordProvider.isEmpty) {
      throw CallStatus.UNAUTHENTICATED
        .withDescription("Basic authentication is not configured for Flight SQL")
        .toRuntimeException
    }
    val decoded =
      try {
        new String(Base64.getDecoder.decode(encoded), StandardCharsets.UTF_8)
      } catch {
        case e: IllegalArgumentException =>
          throw CallStatus.UNAUTHENTICATED
            .withDescription("Malformed Flight SQL basic credentials")
            .withCause(e)
            .toRuntimeException
      }
    val separator = decoded.indexOf(':')
    if (separator <= 0) {
      throw CallStatus.UNAUTHENTICATED
        .withDescription("Malformed Flight SQL basic credentials")
        .toRuntimeException
    }
    val user = decoded.substring(0, separator)
    val password = decoded.substring(separator + 1)
    passwordProvider.get.authenticate(user, password)
    authResult(user)
  }

  private def authenticateNegotiate(token: String): CallHeaderAuthenticator.AuthResult = {
    val validator = kerberosValidator.getOrElse {
      throw CallStatus.UNAUTHENTICATED
        .withDescription("Kerberos authentication is not configured for Flight SQL")
        .toRuntimeException
    }
    try {
      authResult(validator.validate(token.trim))
    } catch {
      case NonFatal(e) =>
        throw CallStatus.UNAUTHENTICATED
          .withDescription("Flight SQL Kerberos authentication failed")
          .withCause(e)
          .toRuntimeException
    }
  }

  private def authResult(user: String): CallHeaderAuthenticator.AuthResult =
    new CallHeaderAuthenticator.AuthResult {
      override def getPeerIdentity: String = user
    }
}
