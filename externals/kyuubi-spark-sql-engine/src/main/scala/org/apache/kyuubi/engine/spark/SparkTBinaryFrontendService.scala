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

import scala.collection.JavaConverters._

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hive.service.rpc.thrift.{TOpenSessionReq, TOpenSessionResp, TRenewDelegationTokenReq, TRenewDelegationTokenResp}
import org.apache.spark.SparkContext
import org.apache.spark.kyuubi.SparkContextHelper

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.ha.client.{EngineServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.service.{Serverable, Service, TBinaryFrontendService}
import org.apache.kyuubi.service.TFrontendService._
import org.apache.kyuubi.util.KyuubiHadoopUtils

class SparkTBinaryFrontendService(
    override val serverable: Serverable)
  extends TBinaryFrontendService("SparkTBinaryFrontend") {
  import SparkTBinaryFrontendService._

  private lazy val sc = be.asInstanceOf[SparkSQLBackendService].sparkSession.sparkContext

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    debug(req.toString)

    // We hacked `TCLIService.Iface.RenewDelegationToken` to transfer Credentials from Kyuubi
    // Server to Spark SQL engine
    val resp = new TRenewDelegationTokenResp()
    try {
      renewDelegationToken(sc, req.getDelegationToken)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error renew delegation tokens: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    debug(req.toString)
    info("Client protocol version: " + req.getClient_protocol)
    val resp = new TOpenSessionResp
    try {
      val respConfiguration = Map(
        KYUUBI_ENGINE_ID -> KyuubiSparkUtil.engineId,
        KYUUBI_ENGINE_NAME -> KyuubiSparkUtil.engineName,
        KYUUBI_ENGINE_URL -> KyuubiSparkUtil.engineUrl).asJava

      if (req.getConfiguration != null) {
        val credentials = req.getConfiguration.remove(KYUUBI_ENGINE_CREDENTIALS_KEY)
        Option(credentials).filter(_.nonEmpty).foreach(renewDelegationToken(sc, _))
      }

      val sessionHandle = getSessionHandle(req, resp)
      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      resp.setConfiguration(respConfiguration)
      resp.setStatus(OK_STATUS)
      Option(CURRENT_SERVER_CONTEXT.get()).foreach(_.setSessionHandle(sessionHandle))
    } catch {
      case e: Exception =>
        error("Error opening session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e, verbose = true))
    }
    resp
  }

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new EngineServiceDiscovery(this))
    } else {
      None
    }
  }

  override def attributes: Map[String, String] = {
    Map(KYUUBI_ENGINE_ID -> KyuubiSparkUtil.engineId)
  }
}

object SparkTBinaryFrontendService extends Logging {

  val HIVE_DELEGATION_TOKEN = new Text("HIVE_DELEGATION_TOKEN")

  private[spark] def renewDelegationToken(sc: SparkContext, delegationToken: String): Unit = {
    val newCreds = KyuubiHadoopUtils.decodeCredentials(delegationToken)
    val (hiveTokens, otherTokens) =
      KyuubiHadoopUtils.getTokenMap(newCreds).partition(_._2.getKind == HIVE_DELEGATION_TOKEN)

    val updateCreds = new Credentials()
    val oldCreds = UserGroupInformation.getCurrentUser.getCredentials
    addHiveToken(sc, hiveTokens, oldCreds, updateCreds)
    addOtherTokens(otherTokens, oldCreds, updateCreds)
    if (updateCreds.numberOfTokens() > 0) {
      info("Update delegation tokens. " +
        s"The number of tokens sent by the server is ${newCreds.numberOfTokens()}. " +
        s"The actual number of updated tokens is ${updateCreds.numberOfTokens()}.")
      SparkContextHelper.updateDelegationTokens(sc, updateCreds)
    }
  }

  private def addHiveToken(
      sc: SparkContext,
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
        if (compareIssueDate(token, oldAliasAndToken.get._2) > 0) {
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
        if (compareIssueDate(newToken, oldToken) > 0) {
          updateCreds.addToken(alias, newToken)
        } else {
          warn(s"Ignore token with earlier issue date: $newToken")
        }
      } else {
        info(s"Ignore unknown token $newToken")
      }
    }
  }

  private def compareIssueDate(
      newToken: Token[_ <: TokenIdentifier],
      oldToken: Token[_ <: TokenIdentifier]): Int = {
    val newDate = KyuubiHadoopUtils.getTokenIssueDate(newToken)
    val oldDate = KyuubiHadoopUtils.getTokenIssueDate(oldToken)
    if (newDate.isDefined && oldDate.isDefined && newDate.get <= oldDate.get) {
      -1
    } else {
      1
    }
  }
}
