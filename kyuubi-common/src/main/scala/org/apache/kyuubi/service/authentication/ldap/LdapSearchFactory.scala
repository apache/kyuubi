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

package org.apache.kyuubi.service.authentication.ldap

import java.io.IOException
import java.security.{GeneralSecurityException, KeyStore}
import java.util
import javax.naming.{Context, NamingException}
import javax.naming.directory.{DirContext, InitialDirContext}
import javax.net.ssl.SSLContext
import javax.security.sasl.AuthenticationException

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class LdapSearchFactory extends DirSearchFactory with Logging {
  @throws[AuthenticationException]
  override def getInstance(conf: KyuubiConf, principal: String, password: String): DirSearch = {
    try {
      val ctx = createDirContext(conf, principal, password)
      new LdapSearch(conf, ctx, conf.get(AUTHENTICATION_LDAP_SSL_ENABLE))
    } catch {
      case e: NamingException =>
        debug(s"Could not connect to the LDAP Server: Authentication failed for $principal")
        throw new AuthenticationException(s"Error validating LDAP user: $principal", e)
    }
  }

  @throws[NamingException]
  private def createDirContext(
      conf: KyuubiConf,
      principal: String,
      password: String): DirContext = {
    val env = createDirContextEnvironment(conf, principal, password)
    try {
      new InitialDirContext(env)
    } catch {
      case e: NamingException =>
        if (env.containsKey(LdapSearchFactory.LDAP_SOCKET_FACTORY)) {
          LdapSSLSocketFactory.clearSslContextForCurrentThread()
        }
        throw e
    }
  }

  private[ldap] def createDirContextEnvironment(
      conf: KyuubiConf,
      principal: String,
      password: String): util.Hashtable[String, AnyRef] = {
    val ldapUrl = conf.get(KyuubiConf.AUTHENTICATION_LDAP_URL)
    val env = new util.Hashtable[String, AnyRef]
    ldapUrl.foreach(env.put(Context.PROVIDER_URL, _))
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.SECURITY_AUTHENTICATION, "simple")
    env.put(Context.SECURITY_PRINCIPAL, principal)
    env.put(Context.SECURITY_CREDENTIALS, password)
    if (conf.get(AUTHENTICATION_LDAP_SSL_ENABLE)) {
      configureSSLSocketFactory(conf, env)
    }
    debug(s"Connecting using principal $principal to ldap server: ${ldapUrl.orNull}")
    env
  }

  @throws[NamingException]
  private def configureSSLSocketFactory(
      conf: KyuubiConf,
      env: util.Hashtable[String, AnyRef]): Unit = {
    val trustStorePath = conf.get(AUTHENTICATION_LDAP_SSL_TRUSTSTORE_PATH)
    val trustStorePassword = conf.get(AUTHENTICATION_LDAP_SSL_TRUSTSTORE_PASSWORD)
    val trustStoreType = conf.get(AUTHENTICATION_LDAP_SSL_TRUSTSTORE_TYPE)

    if (trustStorePath.isEmpty) {
      throw new IllegalArgumentException(
        s"${AUTHENTICATION_LDAP_SSL_TRUSTSTORE_PATH.key} not configured for SSL connection")
    }
    if (trustStorePassword.isEmpty) {
      throw new IllegalArgumentException(
        s"${AUTHENTICATION_LDAP_SSL_TRUSTSTORE_PASSWORD.key} not configured for SSL connection")
    }

    try {
      val sslContext = getSSLContext(
        trustStorePath.get,
        trustStorePassword.get,
        trustStoreType.getOrElse(KeyStore.getDefaultType))
      LdapSSLSocketFactory.setSSLContextForCurrentThread(sslContext)
      env.put(LdapSearchFactory.LDAP_SOCKET_FACTORY, classOf[LdapSSLSocketFactory].getName)
    } catch {
      case e @ (_: GeneralSecurityException | _: IOException) =>
        val namingException = new NamingException("Failed to configure LDAP SSL context")
        namingException.initCause(e)
        throw namingException
    }
  }

  private def getSSLContext(
      trustStorePath: String,
      trustStorePassword: String,
      trustStoreType: String): SSLContext = {
    LdapSSLUtils.createSSLContext(
      trustStorePath,
      trustStorePassword,
      trustStoreType)
  }
}

object LdapSearchFactory {
  private val LDAP_SOCKET_FACTORY = "java.naming.ldap.factory.socket"
}
