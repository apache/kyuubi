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

package org.apache.kyuubi.plugin.spark.authz.util

import java.nio.charset.StandardCharsets
import java.security.{KeyFactory, PublicKey, Signature}
import java.security.interfaces.ECPublicKey
import java.security.spec.X509EncodedKeySpec
import java.util.Base64

import scala.util.{Failure, Success, Try}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.spark.{SPARK_VERSION, SparkContext}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}

import org.apache.kyuubi.plugin.spark.authz.AccessControlException
import org.apache.kyuubi.plugin.spark.authz.util.ReservedKeys._

private[authz] object AuthZUtils {

  /**
   * fixme error handling need improve here
   */
  def getFieldVal[T](o: Any, name: String): T = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value.asInstanceOf[T]
      case Failure(e) =>
        val candidates = o.getClass.getDeclaredFields.map(_.getName).mkString("[", ",", "]")
        throw new RuntimeException(s"$name not in ${o.getClass} $candidates", e)
    }
  }

  def getFieldValOpt[T](o: Any, name: String): Option[T] = Try(getFieldVal[T](o, name)).toOption

  def invoke(
      obj: AnyRef,
      methodName: String,
      args: (Class[_], AnyRef)*): AnyRef = {
    try {
      val (types, values) = args.unzip
      val method = obj.getClass.getMethod(methodName, types: _*)
      method.setAccessible(true)
      method.invoke(obj, values: _*)
    } catch {
      case e: NoSuchMethodException =>
        val candidates = obj.getClass.getMethods.map(_.getName).mkString("[", ",", "]")
        throw new RuntimeException(s"$methodName not in ${obj.getClass} $candidates", e)
    }
  }

  def invokeAs[T](
      obj: AnyRef,
      methodName: String,
      args: (Class[_], AnyRef)*): T = {
    invoke(obj, methodName, args: _*).asInstanceOf[T]
  }

  def invokeStatic(
      obj: Class[_],
      methodName: String,
      args: (Class[_], AnyRef)*): AnyRef = {
    val (types, values) = args.unzip
    val method = obj.getMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values: _*)
  }

  def invokeStaticAs[T](
      obj: Class[_],
      methodName: String,
      args: (Class[_], AnyRef)*): T = {
    invokeStatic(obj, methodName, args: _*).asInstanceOf[T]
  }

  /**
   * Get the active session user
   * @param spark spark context instance
   * @return the user name
   */
  def getAuthzUgi(spark: SparkContext): UserGroupInformation = {
    val isSessionUserVerifyEnabled =
      spark.getConf.getBoolean(s"spark.$KYUUBI_SESSION_USER_SIGN_ENABLED", defaultValue = false)

    // kyuubi.session.user is only used by kyuubi
    val user = spark.getLocalProperty(KYUUBI_SESSION_USER)
    if (isSessionUserVerifyEnabled) {
      verifyKyuubiSessionUser(spark, user)
    }

    if (user != null && user != UserGroupInformation.getCurrentUser.getShortUserName) {
      UserGroupInformation.createRemoteUser(user)
    } else {
      UserGroupInformation.getCurrentUser
    }
  }

  def hasResolvedPermanentView(plan: LogicalPlan): Boolean = {
    plan match {
      case view: View if view.resolved && isSparkVersionAtLeast("3.1.0") =>
        !getFieldVal[Boolean](view, "isTempView")
      case _ =>
        false
    }
  }

  lazy val isRanger21orGreater: Boolean = {
    try {
      classOf[RangerBasePlugin].getConstructor(classOf[String], classOf[String], classOf[String])
      true
    } catch {
      case _: NoSuchMethodException =>
        false
    }
  }

  def isSparkVersionAtMost(targetVersionString: String): Boolean = {
    SemanticVersion(SPARK_VERSION).isVersionAtMost(targetVersionString)
  }

  def isSparkVersionAtLeast(targetVersionString: String): Boolean = {
    SemanticVersion(SPARK_VERSION).isVersionAtLeast(targetVersionString)
  }

  def isSparkVersionEqualTo(targetVersionString: String): Boolean = {
    SemanticVersion(SPARK_VERSION).isVersionEqualTo(targetVersionString)
  }

  /**
   * check if spark version satisfied
   * first param is option of supported most  spark version,
   * and secont param is option of supported least spark version
   *
   * @return
   */
  def passSparkVersionCheck: (Option[String], Option[String]) => Boolean =
    (mostSparkVersion, leastSparkVersion) => {
      mostSparkVersion.forall(isSparkVersionAtMost) &&
      leastSparkVersion.forall(isSparkVersionAtLeast)
    }

  def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }

  def quote(parts: Seq[String]): String = {
    parts.map(quoteIfNeeded).mkString(".")
  }

  private def verifyKyuubiSessionUser(spark: SparkContext, user: String): Unit = {
    def illegalAccessWithUnverifiedUser = {
      throw new AccessControlException(s"Invalid user identifier [$user]")
    }

    try {
      val userPubKeyBase64 = spark.getLocalProperty(KYUUBI_SESSION_SIGN_PUBLICKEY)
      val userSignBase64 = spark.getLocalProperty(KYUUBI_SESSION_USER_SIGN)
      if (StringUtils.isAnyBlank(user, userPubKeyBase64, userSignBase64)) {
        illegalAccessWithUnverifiedUser
      }
      if (!verifySignWithECDSA(user, userSignBase64, userPubKeyBase64)) {
        illegalAccessWithUnverifiedUser
      }
    } catch {
      case _: Exception =>
        illegalAccessWithUnverifiedUser
    }
  }

  private def verifySignWithECDSA(
      plainText: String,
      signatureBase64: String,
      publicKeyBase64: String): Boolean = {
    val pubKeyBytes = Base64.getDecoder.decode(publicKeyBase64)
    val publicKey: PublicKey = KeyFactory.getInstance("EC")
      .generatePublic(new X509EncodedKeySpec(pubKeyBytes)).asInstanceOf[ECPublicKey]
    val publicSignature = Signature.getInstance("SHA256withECDSA")
    publicSignature.initVerify(publicKey)
    publicSignature.update(plainText.getBytes(StandardCharsets.UTF_8))
    val signatureBytes = Base64.getDecoder.decode(signatureBase64)
    publicSignature.verify(signatureBytes)
  }
}
