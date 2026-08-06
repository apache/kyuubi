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

import java.io.{ByteArrayInputStream, FileOutputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.KeyStore
import java.security.cert.CertificateFactory
import javax.naming.Context
import javax.naming.directory.DirContext

import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class LdapSearchFactorySuite extends KyuubiFunSuite {

  private val socketFactoryKey = "java.naming.ldap.factory.socket"
  private val ldapUrl = "ldaps://127.0.0.1:1"
  private val trustStorePassword = "password123"
  private val trustStoreType = "PKCS12"

  test("configure ldap ssl socket factory with PKCS12 truststore") {
    val trustStorePath = createTrustStore(trustStoreType, trustStorePassword)
    val conf = ldapSSLConf(trustStorePath)

    assertSSLSocketFactoryConfigured(conf)
  }

  test("configure ldap ssl socket factory with PEM certificate truststore") {
    val trustStorePath = createPemTrustStore()
    val conf = ldapSSLConf(trustStorePath)

    assertSSLSocketFactoryConfigured(conf)
  }

  test("do not configure ldap ssl socket factory when ldap ssl is disabled") {
    val conf = KyuubiConf(loadSysDefault = false)
      .set(AUTHENTICATION_LDAP_URL, ldapUrl)

    val env = new LdapSearchFactory()
      .createDirContextEnvironment(conf, "uid=user,ou=users", "password")

    assert(env.get(Context.PROVIDER_URL) === ldapUrl)
    assert(env.get(socketFactoryKey) === null)
  }

  test("require truststore path and password when ldap ssl is enabled") {
    Seq(
      KyuubiConf(loadSysDefault = false)
        .set(AUTHENTICATION_LDAP_URL, ldapUrl)
        .set(AUTHENTICATION_LDAP_SSL_ENABLE, true)
        .set(AUTHENTICATION_LDAP_SSL_TRUSTSTORE_PASSWORD, "password123") ->
        AUTHENTICATION_LDAP_SSL_TRUSTSTORE_PATH.key).foreach { case (conf, expectedKey) =>
      val e = intercept[IllegalArgumentException] {
        new LdapSearchFactory().createDirContextEnvironment(conf, "uid=user,ou=users", "password")
      }
      assert(e.getMessage.contains(expectedKey))
    }
  }

  test("clear ldap ssl context when ldap search is closed") {
    val trustStorePath = createTrustStore(trustStoreType, trustStorePassword)
    val conf = ldapSSLConf(trustStorePath)

    new LdapSearchFactory().createDirContextEnvironment(conf, "uid=user,ou=users", "password")
    assert(LdapSSLSocketFactory.getDefault().isInstanceOf[LdapSSLSocketFactory])

    new LdapSearch(conf, mock[DirContext], clearSslContextOnClose = true).close()

    intercept[IllegalStateException] {
      LdapSSLSocketFactory.getDefault()
    }
  }

  test("report truststore loading error when truststore password is wrong") {
    val trustStorePath = createTrustStore("PKCS12", "password123")

    val e = intercept[IOException] {
      LdapSSLUtils.createSSLContext(trustStorePath, "wrong-password", "PKCS12")
    }

    assert(e.getSuppressed.isEmpty)
  }

  private def createTrustStore(trustStoreType: String, password: String): String = {
    val path = Utils.createTempDir().resolve(s"ldap-truststore.$trustStoreType")
    val trustStore = KeyStore.getInstance(trustStoreType)
    trustStore.load(null, password.toCharArray)
    trustStore.setCertificateEntry("ldap", loadCertificate())
    val out = new FileOutputStream(path.toFile)
    try {
      trustStore.store(out, password.toCharArray)
    } finally {
      out.close()
    }
    path.toAbsolutePath.toString
  }

  private def createPemTrustStore(): String = {
    val path = Utils.createTempDir().resolve("ldap-truststore.pem")
    Files.write(path, testCertificatePem.getBytes(StandardCharsets.UTF_8))
    path.toAbsolutePath.toString
  }

  private def loadCertificate() = {
    val certificateFactory = CertificateFactory.getInstance("X.509")
    val in = new ByteArrayInputStream(testCertificatePem.getBytes(StandardCharsets.UTF_8))
    try {
      certificateFactory.generateCertificate(in)
    } finally {
      in.close()
    }
  }

  private def ldapSSLConf(trustStorePath: String): KyuubiConf = {
    KyuubiConf(loadSysDefault = false)
      .set(AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(AUTHENTICATION_LDAP_SSL_ENABLE, true)
      .set(AUTHENTICATION_LDAP_SSL_TRUSTSTORE_PATH, trustStorePath)
      .set(AUTHENTICATION_LDAP_SSL_TRUSTSTORE_PASSWORD, trustStorePassword)
      .set(AUTHENTICATION_LDAP_SSL_TRUSTSTORE_TYPE, trustStoreType)
  }

  private def assertSSLSocketFactoryConfigured(conf: KyuubiConf): Unit = {
    val env = new LdapSearchFactory()
      .createDirContextEnvironment(conf, "uid=user,ou=users", "password")

    assert(env.get(Context.PROVIDER_URL) === ldapUrl)
    assert(env.get(socketFactoryKey) === classOf[LdapSSLSocketFactory].getName)
    assert(LdapSSLSocketFactory.getDefault().isInstanceOf[LdapSSLSocketFactory])
  }

  private val testCertificatePem =
    """-----BEGIN CERTIFICATE-----
      |MIIDCTCCAfGgAwIBAgIUWpdT1drB26WnuUu6xQrRBRwRVWgwDQYJKoZIhvcNAQEL
      |BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDczMTA4MDIyMloXDTM2MDcy
      |ODA4MDIyMlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
      |AAOCAQ8AMIIBCgKCAQEA26bz4WtVhatDvNXzEkE9C5MpdKVop1lLtkFkgVcisdX4
      |HcIM0Lv/1ryrofxvIgJjt8frzC0awX7NPrN55fLyQ+NtIt3xwdcE8TkdwNuStQpi
      |R/TxpUtr+aZw40cDH35KeJNfEex6HKKbCWHTeItvwRhVt4vuATCavC/Tw9R5hF01
      |c42txratWk83xg83Scy6sZwIhXZ9kGmoTvidaAT212q+onwjW087axd3npZ5iyjV
      |pqkAZ4jzaqAlSuoEYHXsivqdAFIstx/BCqxPuIpfVh28fGBgovM0ROZFnCd/R0Pb
      |YAQwmypNjiBHMJjiWjexqm7nDA2Nj6fIWpDQmN0kMQIDAQABo1MwUTAdBgNVHQ4E
      |FgQUbtvmsQW8PbxcQ84f8SIMd9Ua7YswHwYDVR0jBBgwFoAUbtvmsQW8PbxcQ84f
      |8SIMd9Ua7YswDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAcs4I
      |fP9tkF8L2gXkNE5pH/c8+zvH3j/QFj0pAz12YR02yjJKBXw/JKTxLAbOKZuyY6bV
      |X4953ppMluvqm108IkOChkXPrD8nnF1WfgpZh61FXkboLWbZXS+BEb9j+5W+jeC3
      |xeVZZG1GP9CiL6F4MdDnQaOuyRMY+iCM+zYbDe0r56my1UNIEymdXiEriy17lllw
      |ho/IzPZKqt/JFGMQ8kkzWaYgF52cF6iuNdnYatd8HVcqYiyFG44XF5s3L9QHrZ5F
      |1364kBO0IXST4cFNbyQ+DHekbQoUxT2SdHW2JN8TMvr+mjwVWRoH6lg7+4Mb/6JB
      |36XlC2HY/TejOS5PyA==
      |-----END CERTIFICATE-----
      |""".stripMargin

  override protected def afterEach(): Unit = {
    LdapSSLSocketFactory.clearSslContextForCurrentThread()
    super.afterEach()
  }
}
