package org.apache.kyuubi.service.authentication

import java.io.{File, IOException}
import java.security.{PrivilegedActionException, PrivilegedExceptionAction}
import java.util.Base64
import javax.security.auth.Subject
import javax.security.auth.kerberos.{KerberosPrincipal, KeyTab}
import javax.security.sasl.AuthenticationException
import javax.servlet.ServletException
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.hadoop.security.authentication.util.KerberosName
import org.apache.hadoop.security.authentication.util.KerberosUtil._
import org.ietf.jgss.{GSSContext, GSSCredential, GSSManager, Oid}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthSchemes.AuthScheme
import org.apache.kyuubi.service.authentication.utils.HttpAuthUtils.{NEGOTIATE, WWW_AUTHENTICATE_HEADER}

class KerberosAuthenticationHandler extends AuthenticationHandler with Logging {

  private var gssManager: GSSManager = _
  private var conf: KyuubiConf = _
  private var serverSubject = new Subject()
  private var keytab: String = _
  private var principal: String = _

  override val authScheme: AuthScheme = AuthSchemes.NEGOTIATE

  override def authenticationSupported: Boolean = {
    keytab.nonEmpty && principal.nonEmpty
  }

  override def init(conf: KyuubiConf): Unit = {
    this.conf = conf
    keytab = conf.get(KyuubiConf.SERVER_SPNEGO_KEYTAB).getOrElse("")
    principal = conf.get(KyuubiConf.SERVER_SPNEGO_PRINCIPAL).getOrElse("")
    if (authenticationSupported) {
      val keytabFile = new File(keytab)
      if (!keytabFile.exists()) {
        throw new ServletException(s"Keytab[$keytab] does not exists")
      }
      if (!principal.startsWith("HTTP/")) {
        throw new ServletException(s"SPNEGO principal[$principal] does not start with HTTP/")
      }

      info(s"Using keytab $keytab, for principal $principal")
      serverSubject.getPrivateCredentials().add(KeyTab.getInstance(keytabFile))
      serverSubject.getPrincipals.add(new KerberosPrincipal(principal))

      // TODO: support to config kerberos.name.rules and kerberos.rule.mechanism
      // set default rules if no rules set, otherwise it will throw exception
      // when parse the kerberos name
      if (!KerberosName.hasRulesBeenSet) {
        KerberosName.setRules("DEFAULT")
      }

      try {
        gssManager = Subject.doAs(
          serverSubject,
          new PrivilegedExceptionAction[GSSManager] {
            override def run(): GSSManager = {
              GSSManager.getInstance()
            }
          })
      } catch {
        case e: PrivilegedActionException => throw e.getException
        case e: Exception => throw new ServletException(e)
      }
    }
  }

  override def destroy(): Unit = {
    keytab = null
    serverSubject = null
  }

  override def authenticate(
      request: HttpServletRequest,
      response: HttpServletResponse): String = {
    var authUser: String = null
    val authorization = getAuthorization(request)
    val clientToken = Base64.getDecoder.decode(authorization)
    try {
      val serverPrincipal = getTokenServerName(clientToken)
      if (!serverPrincipal.startsWith("HTTP/")) {
        throw new IllegalArgumentException(
          s"Invalid server principal $serverPrincipal decoded from client request")
      }
      authUser = Subject.doAs(
        serverSubject,
        new PrivilegedExceptionAction[String] {
          override def run(): String = {
            runWithPrincipal(serverPrincipal, clientToken, response)
          }
        })
    } catch {
      case ex: PrivilegedActionException =>
        ex.getException match {
          case ioe: IOException =>
            throw ioe
          case e: Exception => throw new AuthenticationException("SPNEGO authentication failed", e)
        }

      case e: Exception => throw new AuthenticationException("SPNEGO authentication failed", e)
    }
    authUser
  }

  def runWithPrincipal(
      serverPrincipal: String,
      clientToken: Array[Byte],
      response: HttpServletResponse): String = {
    var gssContext: GSSContext = null
    var gssCreds: GSSCredential = null
    var authUser: String = null
    try {
      debug(s"SPNEGO initialized with server principal $serverPrincipal")
      gssCreds = gssManager.createCredential(
        gssManager.createName(serverPrincipal, NT_GSS_KRB5_PRINCIPAL_OID),
        GSSCredential.INDEFINITE_LIFETIME,
        Array[Oid](GSS_SPNEGO_MECH_OID, GSS_KRB5_MECH_OID),
        GSSCredential.ACCEPT_ONLY)
      gssContext = gssManager.createContext(gssCreds)
      val serverToken = gssContext.acceptSecContext(clientToken, 0, clientToken.length)
      if (serverToken != null && serverToken.nonEmpty) {
        val authenticate = Base64.getEncoder.encodeToString(serverToken)
        response.setHeader(WWW_AUTHENTICATE_HEADER, s"$NEGOTIATE $authenticate")
      }
      if (!gssContext.isEstablished) {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
        debug("SPNEGO in progress")
      } else {
        val clientPrincipal = gssContext.getSrcName.toString
        val kerberosName = new KerberosName(clientPrincipal)
        val userName = kerberosName.getShortName
        authUser = userName
        response.setStatus(HttpServletResponse.SC_OK)
        debug(s"SPNEGO completed for client principal $clientPrincipal")
      }
    } finally {
      if (gssContext != null) {
        gssContext.dispose()
      }
      if (gssCreds != null) {
        gssCreds.dispose()
      }
    }
    authUser
  }
}
