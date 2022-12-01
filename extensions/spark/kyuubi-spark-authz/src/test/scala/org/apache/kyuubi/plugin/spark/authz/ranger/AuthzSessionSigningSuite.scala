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

package org.apache.kyuubi.plugin.spark.authz.ranger

import java.util.Base64

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSessionExtensions
import org.scalatest.BeforeAndAfterAll
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_SESSION_SIGN_PUBLICKEY, KYUUBI_SESSION_USER_KEY, KYUUBI_SESSION_USER_SIGN}
import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, SparkSessionProvider}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils
import org.apache.kyuubi.util.SignUtils

class AuthzSessionSigningSuite extends AnyFunSuite
  with SparkSessionProvider with BeforeAndAfterAll {
  // scalastyle:on
  override protected val extension: SparkSessionExtensions => Unit = new RangerSparkExtension
  override protected val catalogImpl: String = "in-memory"
  override protected val extraSparkConf: SparkConf = new SparkConf()
    .set("spark.kyuubi.session.user.sign.enabled", "true")

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("KYUUBI #3839 - signing and verifying kyuubi session user") {
    val kyuubiSessionUser = "bowenliang123"
    val sc = spark.sparkContext

    // preparation as in SparkOperation
    val (privateKey, publicKey) = SignUtils.generateKeyPair()
    val userSignature = SignUtils.signWithPrivateKey(kyuubiSessionUser, privateKey)
    val publicKeyBase64 = Base64.getEncoder.encodeToString(publicKey.getEncoded)
    sc.setLocalProperty(KYUUBI_SESSION_USER_KEY, kyuubiSessionUser)
    sc.setLocalProperty(KYUUBI_SESSION_SIGN_PUBLICKEY, publicKeyBase64)
    sc.setLocalProperty(KYUUBI_SESSION_USER_SIGN, userSignature)

    // pass session user verification
    val ugi = AuthZUtils.getAuthzUgi(sc)
    assertResult(kyuubiSessionUser)(ugi.getUserName)

    // fake session user name
    val fakeSessionUser = "faker"
    sc.setLocalProperty(KYUUBI_SESSION_USER_KEY, fakeSessionUser)
    val e1 = intercept[AccessControlException](AuthZUtils.getAuthzUgi(sc))
    assertResult(s"Invalid user identifier [$fakeSessionUser]")(e1.getMessage)
    sc.setLocalProperty(KYUUBI_SESSION_USER_KEY, kyuubiSessionUser)

    // invalid session user sign
    sc.setLocalProperty(KYUUBI_SESSION_USER_SIGN, "invalid_sign")
    val e2 = intercept[AccessControlException](AuthZUtils.getAuthzUgi(sc))
    assertResult(s"Invalid user identifier [$kyuubiSessionUser]")(e2.getMessage)
  }
}
