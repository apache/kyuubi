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

package org.apache.kyuubi

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.PrivilegedExceptionAction
import java.util.Base64
import javax.security.sasl.AuthenticationException

import scala.io.{Codec, Source}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.ietf.jgss.{GSSContext, GSSException, GSSManager, GSSName}
import org.scalatest.time.SpanSugar._

trait KerberizedTestHelper extends KyuubiFunSuite {
  val clientPrincipalUser = "client"
  val baseDir: File =
    Utils.createTempDir("kyuubi-kdc", Utils.getCodeSourceLocation(getClass)).toFile
  val kdcConf = MiniKdc.createConf()
  val hostName = "localhost"
  kdcConf.setProperty(MiniKdc.INSTANCE, this.getClass.getSimpleName)
  // MiniKdc keeps realm config in enum object `KdcServerOption.KDC_REALM`.
  // Multiple kdcs will be launched when testing if they are defined in the same package.
  // Using same realms here to avoid authentication failure.
  kdcConf.setProperty(MiniKdc.ORG_NAME, classOf[KerberizedTestHelper].getSimpleName)
  kdcConf.setProperty(MiniKdc.ORG_DOMAIN, "COM")
  kdcConf.setProperty(MiniKdc.KDC_BIND_ADDRESS, hostName)
  kdcConf.setProperty(MiniKdc.KDC_PORT, "0")
  // MiniKdc.DEBUG in kdcConf is set to false by default and will override JVM system property.
  // Remove it so we can turn on/off kerberos debug message by setting system property
  // `sun.security.krb5.debug`.
  kdcConf.remove(MiniKdc.DEBUG)

  private var kdc: MiniKdc = _
  protected var krb5ConfPath: String = _

  private val keytabFile = new File(baseDir, "kyuubi-test.keytab")
  protected val testKeytab: String = keytabFile.getAbsolutePath
  protected var testPrincipal: String = _
  protected var testSpnegoPrincipal: String = _

  override def beforeAll(): Unit = {
    eventually(timeout(60.seconds), interval(1.second)) {
      try {
        kdc = new MiniKdc(kdcConf, baseDir)
        kdc.start()
        krb5ConfPath = kdc.getKrb5conf.getAbsolutePath
      } catch {
        case NonFatal(e) =>
          if (kdc != null) {
            kdc.stop()
            kdc = null
          }
          throw e
      }
    }
    val tempTestPrincipal = s"$clientPrincipalUser/$hostName"
    val tempSpnegoPrincipal = s"HTTP/$hostName"
    kdc.createPrincipal(keytabFile, tempTestPrincipal, tempSpnegoPrincipal)
    rewriteKrb5Conf()
    testPrincipal = tempTestPrincipal + "@" + kdc.getRealm
    testSpnegoPrincipal = tempSpnegoPrincipal + "@" + kdc.getRealm
    info(s"KerberizedTest Principal: $testPrincipal")
    info(s"KerberizedTest SPNEGO Principal: $testSpnegoPrincipal")
    info(s"KerberizedTest Keytab: $testKeytab")
    super.beforeAll()
  }

  /**
   * Forked from Apache Spark
   * In this method we rewrite krb5.conf to make kdc and client use the same enctypes
   */
  private def rewriteKrb5Conf(): Unit = {
    val source = Source.fromFile(kdc.getKrb5conf)(Codec.UTF8)
    try {
      val krb5Conf = source.getLines
      var rewritten = false
      val addedConfig =
        addedKrb5Config("default_tkt_enctypes", "aes128-cts-hmac-sha1-96") +
          addedKrb5Config("default_tgs_enctypes", "aes128-cts-hmac-sha1-96") +
          addedKrb5Config("dns_lookup_realm", "true")
      val rewriteKrb5Conf = krb5Conf.map(s =>
        if (s.contains("libdefaults")) {
          rewritten = true
          s + addedConfig
        } else if (s.contains(hostName)) {
          s + "\n" + s.replace(hostName, s"tcp/$hostName")
        } else {
          s
        }).filter(!_.trim.startsWith("#")).mkString(System.lineSeparator())

      val krb5confStr =
        if (!rewritten) {
          "[libdefaults]" + addedConfig + System.lineSeparator() +
            System.lineSeparator() + rewriteKrb5Conf
        } else {
          rewriteKrb5Conf
        }

      kdc.getKrb5conf.delete()
      val writer = Files.newBufferedWriter(kdc.getKrb5conf.toPath, StandardCharsets.UTF_8)
      writer.write(krb5confStr)
      writer.close()
      info(s"krb5.conf file content: $krb5confStr")
    } finally {
      source.close
    }
  }

  private def addedKrb5Config(key: String, value: String): String = {
    System.lineSeparator() + s"    $key=$value"
  }

  override def afterAll(): Unit = {
    kdc.stop()
    super.afterAll()
  }

  def tryWithSecurityEnabled(block: => Unit): Unit = {
    val conf = new Configuration()
    assert(!UserGroupInformation.isSecurityEnabled)
    val currentUser = UserGroupInformation.getCurrentUser
    val authType = "hadoop.security.authentication"
    try {
      conf.set(authType, "KERBEROS")
      System.setProperty("java.security.krb5.conf", krb5ConfPath)
      UserGroupInformation.setConfiguration(conf)
      assert(UserGroupInformation.isSecurityEnabled)
      block
    } finally {
      conf.unset(authType)
      System.clearProperty("java.security.krb5.conf")
      UserGroupInformation.setLoginUser(currentUser)
      UserGroupInformation.setConfiguration(conf)
      assert(!UserGroupInformation.isSecurityEnabled)
    }
  }

  /**
   * Generate SPNEGO challenge request token.
   * Copy from Apache Hadoop YarnClientUtils::generateToken
   *
   * @param server - hostname to contact
   * @return SPNEGO token challenge
   */
  def generateToken(server: String): String = {
    val currentUser = UserGroupInformation.getCurrentUser
    currentUser.doAs(new PrivilegedExceptionAction[String] {
      override def run(): String =
        try {
          val manager = GSSManager.getInstance()
          val serverName = manager.createName("HTTP@" + server, GSSName.NT_HOSTBASED_SERVICE)
          val gssContext = manager.createContext(
            serverName.canonicalize(null),
            null,
            null,
            GSSContext.DEFAULT_LIFETIME)
          // Create a GSSContext for authentication with the service.
          // We're passing client credentials as null since we want them to
          // be read from the Subject.
          // We're passing Oid as null to use the default.
          gssContext.requestMutualAuth(true)
          gssContext.requestCredDeleg(true)
          // Establish context
          val inToken = Array.empty[Byte]
          val outToken = gssContext.initSecContext(inToken, 0, inToken.length)
          gssContext.dispose()
          new String(Base64.getEncoder.encode(outToken), StandardCharsets.UTF_8)
        } catch {
          case e: GSSException =>
            throw new AuthenticationException("Failed to generate token", e)
        }
    })
  }
}
