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

package org.apache.kyuubi.spark.connector.hive

import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.{HiveMetaStoreClient, IMetaStoreClient}
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.security.token.Token
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.security.HadoopDelegationTokenProvider
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper._

import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorDelegationTokenProvider.metastoreTokenSignature

class KyuubiHiveConnectorDelegationTokenProvider
  extends HadoopDelegationTokenProvider with Logging {

  logInfo(s"Hadoop Delegation Token provider for service [$serviceName] is initialized.")

  override def serviceName: String = "kyuubi-hive-connector"

  private val classNotFoundErrorStr = "You are attempting to use the " +
    s"${getClass.getCanonicalName}, but your Spark distribution is not built with Hive libraries."

  private def hiveConf(
      sparkConf: SparkConf,
      hadoopConf: Configuration,
      hiveCatalogName: String): Option[HiveConf] = {
    try {
      val hiveConf = new HiveConf(hadoopConf, classOf[HiveConf])
      sparkConf.getAllWithPrefix(s"spark.sql.catalog.$hiveCatalogName.")
        .foreach { case (k, v) => hiveConf.set(k, v) }
      // The `RetryingHiveMetaStoreClient` may block the subsequent token obtaining,
      // and `obtainDelegationTokens` is scheduled frequently, it's fine to disable
      // the Hive retry mechanism.
      hiveConf.set("hive.metastore.fastpath", "false")
      Some(hiveConf)
    } catch {
      case NonFatal(e) =>
        logWarning("Fail to create Hive Configuration", e)
        None
      case e: NoClassDefFoundError =>
        logWarning(classNotFoundErrorStr, e)
        None
    }
  }

  private def extractHiveCatalogNames(sparkConf: SparkConf): Set[String] = sparkConf
    .getAllWithPrefix("spark.sql.catalog.")
    .filter { case (_, v) => v == classOf[HiveTableCatalog].getName }
    .flatMap { case (k, _) => k.stripPrefix("spark.sql.catalog.").split("\\.").headOption }
    .distinct.toSet

  // The current implementation has the following limitations:
  // the v2 catalog is kind of SQL API, and we can not get SQLConf here, thus only those
  // configurations present in the Spark application bootstrap will take effect, e.g.
  // `spark-defaults.conf` or `spark-submit --conf k=v` will take effect, but dynamically
  // register by SET statement will not.
  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    val hiveCatalogNames = extractHiveCatalogNames(sparkConf)
    logDebug(s"Recognized Kyuubi Hive Connector catalogs: $hiveCatalogNames")
    hiveCatalogNames.exists { hiveCatalogName =>
      hiveConf(sparkConf, hadoopConf, hiveCatalogName).exists { remoteHmsConf =>
        delegationTokensRequired(sparkConf, remoteHmsConf, hiveCatalogName)
      }
    }
  }

  private def delegationTokensRequired(
      sparkConf: SparkConf,
      hiveConf: HiveConf,
      hiveCatalogName: String): Boolean = {
    val tokenRenewalEnabled = sparkConf.getBoolean(
      s"spark.sql.catalog.$hiveCatalogName.delegation.token.renewal.enabled",
      true)
    val metastoreUris =
      sparkConf.get(s"spark.sql.catalog.$hiveCatalogName.hive.metastore.uris", "")
    val tokenAlias = new Text(metastoreUris)
    val currentToken = UserGroupInformation.getCurrentUser.getCredentials.getToken(tokenAlias)
    tokenRenewalEnabled && metastoreUris.nonEmpty && currentToken == null &&
    SecurityUtil.getAuthenticationMethod(hiveConf) != AuthenticationMethod.SIMPLE &&
    hiveConf.getBoolean("hive.metastore.sasl.enabled", false) &&
    (SparkHadoopUtil.get.isProxyUser(UserGroupInformation.getCurrentUser) ||
      (!Utils.isClientMode(sparkConf) && !sparkConf.contains("spark.kerberos.keytab")))
  }

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    val hmsClients: mutable.HashSet[IMetaStoreClient] = mutable.HashSet.empty
    try {
      val hiveCatalogNames = extractHiveCatalogNames(sparkConf)
      logDebug(s"Recognized Hive catalogs: $hiveCatalogNames")

      val requireTokenCatalogs = hiveCatalogNames.filter { hiveCatalogName =>
        hiveConf(sparkConf, hadoopConf, hiveCatalogName).exists { remoteHmsConf =>
          delegationTokensRequired(sparkConf, remoteHmsConf, hiveCatalogName)
        }
      }
      logDebug(s"Require token Hive catalogs: $requireTokenCatalogs")

      requireTokenCatalogs.foreach { hiveCatalogName =>
        // one token request failure should not block the subsequent token obtaining
        Utils.tryLogNonFatalError {
          hiveConf(sparkConf, hadoopConf, hiveCatalogName).foreach { remoteHmsConf =>
            val metastoreUrisKey = s"spark.sql.catalog.$hiveCatalogName.hive.metastore.uris"
            val metastoreUris = sparkConf.get(metastoreUrisKey, "")
            assert(metastoreUris.nonEmpty)

            val catalogOptions =
              sparkConf.getAllWithPrefix(s"spark.sql.catalog.$hiveCatalogName.").toMap
            val tokenSignature = metastoreTokenSignature(catalogOptions.asJava)

            val principalKey = "hive.metastore.kerberos.principal"
            val principal = remoteHmsConf.getTrimmed(principalKey, "")
            require(principal.nonEmpty, s"Hive principal $principalKey undefined")

            val currentUser = UserGroupInformation.getCurrentUser
            logInfo(s"Getting Hive delegation token for ${currentUser.getUserName} against " +
              s"$principal at $metastoreUris")

            doAsRealUser {
              val hmsClient = new HiveMetaStoreClient(remoteHmsConf, null, false)
              hmsClients += hmsClient
              val tokenStr = hmsClient.getDelegationToken(currentUser.getUserName, principal)
              val hive2Token = new Token[DelegationTokenIdentifier]()
              hive2Token.decodeFromUrlString(tokenStr)
              hive2Token.setService(new Text(tokenSignature))
              logDebug(s"Get Token from hive metastore: ${hive2Token.toString}")
              val tokenAlias = new Text(metastoreUris)
              creds.addToken(tokenAlias, hive2Token)
            }
          }
        }
      }
      None
    } catch {
      case _: NoClassDefFoundError =>
        logWarning(classNotFoundErrorStr)
        None
    } finally {
      hmsClients.foreach { hmsClient => Utils.tryLogNonFatalError { hmsClient.close() } }
    }
  }

  /**
   * Run some code as the real logged in user (which may differ from the current user, for
   * example, when using proxying).
   */
  private def doAsRealUser[T](fn: => T): T = {
    val currentUser = UserGroupInformation.getCurrentUser
    val realUser = Option(currentUser.getRealUser).getOrElse(currentUser)

    // For some reason the Scala-generated anonymous class ends up causing an
    // UndeclaredThrowableException, even if you annotate the method with @throws.
    try {
      realUser.doAs(new PrivilegedExceptionAction[T]() {
        override def run(): T = fn
      })
    } catch {
      case e: UndeclaredThrowableException => throw Option(e.getCause).getOrElse(e)
    }
  }
}

object KyuubiHiveConnectorDelegationTokenProvider {

  // fallback to `hive.metastore.uris` if `hive.metastore.token.signature` is absent
  def metastoreTokenSignature(catalogOptions: util.Map[String, String]): String = {
    assert(catalogOptions.containsKey("hive.metastore.uris"))
    val metastoreUris = catalogOptions.get("hive.metastore.uris")
    catalogOptions.getOrDefault("hive.metastore.token.signature", metastoreUris)
  }
}
