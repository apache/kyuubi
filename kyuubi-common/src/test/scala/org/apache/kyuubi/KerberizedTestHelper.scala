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

import scala.io.{Codec, Source}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.time.SpanSugar._

trait KerberizedTestHelper extends KyuubiFunSuite {
  val baseDir: File = Utils.createTempDir(
    this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath, "kyuubi-kdc").toFile
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
  private var krb5ConfPath: String = _

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

  private val keytabFile = new File(baseDir, "kyuubi-test.keytab")
  protected val testKeytab: String = keytabFile.getAbsolutePath
  protected var testPrincipal = s"client/$hostName"
  kdc.createPrincipal(keytabFile, testPrincipal)


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
        } else {
          s
        }).filter(!_.trim.startsWith("#")).mkString(System.lineSeparator())

      val krb5confStr = if (!rewritten) {
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

  rewriteKrb5Conf()

  testPrincipal = testPrincipal + "@" + kdc.getRealm

  info(s"KerberizedTest Principal: $testPrincipal")
  info(s"KerberizedTest Keytab: $testKeytab")

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
}
