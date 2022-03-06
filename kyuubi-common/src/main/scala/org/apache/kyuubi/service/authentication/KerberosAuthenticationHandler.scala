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
import java.security.{PrivilegedActionException, PrivilegedExceptionAction}
import java.util.HashMap
import javax.security.auth.Subject
import javax.security.auth.kerberos.{KerberosPrincipal, KerberosTicket}
import javax.security.auth.login.{AppConfigurationEntry, Configuration, LoginContext, LoginException}
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED
import javax.security.sasl.AuthenticationException
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._

import com.sun.security.auth.module.Krb5LoginModule
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.security.authentication.util.KerberosName
import org.ietf.jgss.{GSSCredential, GSSException, GSSManager}
import org.ietf.jgss.GSSCredential.ACCEPT_ONLY
import org.ietf.jgss.GSSCredential.INDEFINITE_LIFETIME

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthSchemes.AuthScheme
import org.apache.kyuubi.service.authentication.KerberosUtil._

class KerberosAuthenticationHandler extends AuthenticationHandler with Logging {
  import KerberosAuthenticationHandler._

  private val gssManager = GSSManager.getInstance()
  private var serverCredential: GSSCredential = _
  private var loginContext: LoginContext = _
  private var conf: KyuubiConf = _
  private var keytab: String = _
  private var principal: String = _

  override val authScheme: AuthScheme = AuthSchemes.NEGOTIATE

  override def init(conf: KyuubiConf): Unit = {
    this.conf = conf
    loginFromKeytab()
    // set default rules if no rules set, otherwise it will throw exception
    // when parse the kerberos name
    if (!KerberosName.hasRulesBeenSet) {
      KerberosName.setRules("DEFAULT")
    }
  }

  private def loginFromKeytab(): Unit = {
    try {
      keytab = conf.get(KyuubiConf.SERVER_SPNEGO_KEYTAB).getOrElse("")
      principal = conf.get(KyuubiConf.SERVER_PRINCIPAL).getOrElse("")
      if (keytab.isEmpty || principal.isEmpty) {
        throw new LoginException("Kyuubi spnego keytab or principal is null or empty")
      }
      doLogin(new Subject())
    } catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  private def checkTGTAndReLoginFromKeytab(): Unit = {
    val tgt = getTGT
    val now = System.currentTimeMillis()
    if (tgt == null && now >= getRefreshTime(tgt)) {
      reLoginFromKeytab()
    }
  }

  private def reLoginFromKeytab(): Unit = {
    try {
      loginContext.logout()
      doLogin(loginContext.getSubject)
    } catch {
      case e @ (_: LoginException | _: GSSException | _: PrivilegedActionException) =>
        throw new RuntimeException(e)
    }
  }

  private def getTGT(): KerberosTicket = {
    val subject = loginContext.getSubject
    val tickets = subject.getPrivateCredentials(classOf[KerberosTicket]).asScala
    var tgt: KerberosTicket = null
    for (ticket <- tickets if tgt == null) {
      val principal: KerberosPrincipal = ticket.getServer
      if (principal.getName == "krbtgt/" + principal.getRealm + "@" + principal.getRealm) {
        tgt = ticket
      }
    }
    tgt
  }

  private def getRefreshTime(ticket: KerberosTicket): Long = {
    val start = ticket.getStartTime.getTime
    val end = ticket.getEndTime.getTime
    start + ((end - start) * TICKET_RENEW_WINDOW).toLong
  }

  private def doLogin(subject: Subject): Unit = {
    info("login from keytab")
    loginContext = new LoginContext(
      "kyuubi-kerberos-handler",
      subject,
      null,
      new Configuration {
        override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = {
          val options = new HashMap[String, String]()
          options.put("refreshKrb5Config", "true")
          options.put("doNotPrompt", "true")
          if (logger.isDebugEnabled) {
            options.put("debug", "true")
          }
          options.put("keyTab", keytab)
          options.put("isInitiator", "false")
          options.put("useKeyTab", "true")
          options.put("principal", principal)
          options.put("storeKey", "true")
          Array[AppConfigurationEntry](
            new AppConfigurationEntry(classOf[Krb5LoginModule].getName, REQUIRED, options))
        }
      })
    loginContext.login()

    val serverName = gssManager.createName(principal, NT_GSS_KRB5_PRINCIPAL_OID)
    serverCredential = Subject.doAs(
      subject,
      new PrivilegedExceptionAction[GSSCredential] {
        override def run(): GSSCredential = {
          gssManager.createCredential(
            serverName,
            INDEFINITE_LIFETIME,
            Array(GSS_KRB5_MECH_OID, GSS_SPNEGO_MECH_OID),
            ACCEPT_ONLY)
        }
      })
  }

  override def authenticate(request: HttpServletRequest): AuthUser = {
    beforeAuth(request)
    checkTGTAndReLoginFromKeytab()
    val authorization = getAuthorization(request)
    val inputToken = Base64.decodeBase64(authorization.getBytes())

    try {
      val kerberosName = Subject.doAs(
        loginContext.getSubject,
        new PrivilegedExceptionAction[KerberosName] {
          override def run(): KerberosName = {
            val context = gssManager.createContext(serverCredential)
            context.acceptSecContext(inputToken, 0, inputToken.length)
            if (!context.isEstablished) {
              throw new AuthenticationException(
                "Kerberos authentication failed:" +
                  " unable to establish context with the service ticket provided by the client")
            }
            new KerberosName(context.getSrcName.toString)
          }
        })
      AuthUser(kerberosName.getShortName, "kerberos")
    } catch {
      case e: PrivilegedActionException =>
        throw new AuthenticationException("Kerberos authentication failed: ", e.getException)
      case e: IOException =>
        throw new AuthenticationException("Kerberos authentication failed: ", e)
    }
  }
}

object KerberosAuthenticationHandler {
  private val TICKET_RENEW_WINDOW = 0.80f
}
