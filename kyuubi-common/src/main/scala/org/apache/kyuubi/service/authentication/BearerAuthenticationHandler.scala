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
