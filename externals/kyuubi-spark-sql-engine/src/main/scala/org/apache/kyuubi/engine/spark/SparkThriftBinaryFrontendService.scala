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

package org.apache.kyuubi.engine.spark

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hive.service.rpc.thrift.{TRenewDelegationTokenReq, TRenewDelegationTokenResp}
import org.apache.spark.kyuubi.SparkContextHelper

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf.ENGINE_CONNECTION_URL_USE_HOSTNAME
import org.apache.kyuubi.ha.client.{EngineServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.service.{Serverable, Service, ThriftBinaryFrontendService}
import org.apache.kyuubi.util.KyuubiHadoopUtils

class SparkThriftBinaryFrontendService(
    override val serverable: Serverable)
  extends ThriftBinaryFrontendService("SparkThriftBinaryFrontendService") {
  import SparkThriftBinaryFrontendService._

  private lazy val sc = be.asInstanceOf[SparkSQLBackendService].sparkSession.sparkContext

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    debug(req.toString)

    // We hacked `TCLIService.Iface.RenewDelegationToken` to transfer Credentials from Kyuubi
    // Server to Spark SQL engine
    val resp = new TRenewDelegationTokenResp()
    try {
      val newCreds = KyuubiHadoopUtils.decodeCredentials(req.getDelegationToken)
      val (hiveTokens, otherTokens) =
        KyuubiHadoopUtils.getTokenMap(newCreds).partition(_._2.getKind == HIVE_DELEGATION_TOKEN)

      val updateCreds = new Credentials()
      val oldCreds = UserGroupInformation.getCurrentUser.getCredentials
      addHiveToken(hiveTokens, oldCreds, updateCreds)
      addOtherTokens(otherTokens, oldCreds, updateCreds)
      if (updateCreds.numberOfTokens() > 0) {
        SparkContextHelper.updateDelegationTokens(sc, updateCreds)
      }

      resp.setStatus(ThriftBinaryFrontendService.OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error renew delegation tokens: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  private def addHiveToken(
      newTokens: Map[Text, Token[_ <: TokenIdentifier]],
      oldCreds: Credentials,
      updateCreds: Credentials): Unit = {
    val metastoreUris = sc.hadoopConfiguration.getTrimmed("hive.metastore.uris", "")

    // `HiveMetaStoreClient` selects the first token whose service is "" and kind is
    // "HIVE_DELEGATION_TOKEN" to authenticate.
    val oldAliasAndToken = KyuubiHadoopUtils.getTokenMap(oldCreds)
      .find { case (_, token) =>
        token.getKind == HIVE_DELEGATION_TOKEN && token.getService == new Text()
      }

    if (metastoreUris.nonEmpty && oldAliasAndToken.isDefined) {
      // Each entry of `newTokens` is a <uris, token> pair for a metastore cluster.
      // If entry's uris and engine's metastore uris have at least 1 same uri, we presume they
      // represent the same metastore cluster.
      val uriSet = metastoreUris.split(",").filter(_.nonEmpty).toSet
      val newToken = newTokens
        .find { case (uris, token) =>
          val matched = uris.toString.split(",").exists(uriSet.contains) &&
            token.getService == new Text()
          if (!matched) {
            debug(s"Filter out Hive token $token")
          }
          matched
        }
        .map(_._2)
      newToken.foreach { token =>
        if (KyuubiHadoopUtils.getTokenIssueDate(token) >
            KyuubiHadoopUtils.getTokenIssueDate(oldAliasAndToken.get._2)) {
          updateCreds.addToken(oldAliasAndToken.get._1, token)
        } else {
          warn(s"Ignore Hive token with earlier issue date: $token")
        }
      }
      if (newToken.isEmpty) {
        warn(s"No matching Hive token found for engine metastore uris $metastoreUris")
      }
    } else if (metastoreUris.isEmpty) {
      info(s"Ignore Hive token as hive.metastore.uris are empty")
    } else {
      // Either because Hive metastore is not secured or because engine is launched with keytab
      info(s"Ignore Hive token as engine does not need it")
    }
  }

  private def addOtherTokens(
      tokens: Map[Text, Token[_ <: TokenIdentifier]],
      oldCreds: Credentials,
      updateCreds: Credentials): Unit = {
    tokens.foreach { case (alias, newToken) =>
      val oldToken = oldCreds.getToken(alias)
      if (oldToken != null) {
        if (KyuubiHadoopUtils.getTokenIssueDate(newToken) >
            KyuubiHadoopUtils.getTokenIssueDate(oldToken)) {
          updateCreds.addToken(alias, newToken)
        } else {
          warn(s"Ignore token with earlier issue date: $newToken")
        }
      } else {
        info(s"Ignore unknown token $newToken")
      }
    }
  }

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new EngineServiceDiscovery(this))
    } else {
      None
    }
  }

  override def connectionUrl: String = {
    checkInitialized()
    if (conf.get(ENGINE_CONNECTION_URL_USE_HOSTNAME)) {
      s"${serverAddr.getCanonicalHostName}:$portNum"
    } else {
      // engine use address if run on k8s with cluster mode
      s"${serverAddr.getHostAddress}:$portNum"
    }
  }

  // When a OOM occurs, here we de-register the engine by stop its discoveryService.
  // Then the current engine will not be connected by new client anymore but keep the existing ones
  // alive. In this case we can reduce the engine's overhead and make it possible recover from that.
  // We shall not tear down the whole engine by serverable.stop to make the engine unreachable for
  // the exising clients which are still get statuses and report the end-users.
  override protected def oomHook: Runnable = {
    () => discoveryService.foreach(_.stop())
  }
}

object SparkThriftBinaryFrontendService {

  val HIVE_DELEGATION_TOKEN = new Text("HIVE_DELEGATION_TOKEN")
}
