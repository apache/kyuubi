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

package org.apache.kyuubi.engine.flink

import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.engine.flink.FlinkTBinaryFrontendService.renewDelegationToken
import org.apache.kyuubi.ha.client.{EngineServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.service.{Serverable, Service, TBinaryFrontendService}
import org.apache.kyuubi.service.TFrontendService.OK_STATUS
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TRenewDelegationTokenReq, TRenewDelegationTokenResp}
import org.apache.kyuubi.util.KyuubiHadoopUtils

class FlinkTBinaryFrontendService(
    override val serverable: Serverable)
  extends TBinaryFrontendService("FlinkThriftBinaryFrontendService") {

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    debug(req.toString)
    // We hacked `TCLIService.Iface.RenewDelegationToken` to transfer Credentials from Kyuubi
    // Server to Flink SQL engine
    val resp = new TRenewDelegationTokenResp()
    try {
      renewDelegationToken(req.getDelegationToken)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error renew delegation tokens: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
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

}

object FlinkTBinaryFrontendService extends Logging {
  private[flink] def renewDelegationToken(delegationToken: String): Unit = {
    val newCreds = KyuubiHadoopUtils.decodeCredentials(delegationToken)
    val newTokens = KyuubiHadoopUtils.getTokenMap(newCreds)

    val updateCreds = new Credentials()
    val oldCreds = UserGroupInformation.getCurrentUser.getCredentials
    newTokens.foreach { case (alias, newToken) =>
      val oldToken = oldCreds.getToken(alias)
      if (oldToken != null) {
        if (KyuubiHadoopUtils.compareIssueDate(newToken, oldToken) > 0) {
          updateCreds.addToken(alias, newToken)
        } else {
          warn(s"Ignore token with earlier issue date: $newToken")
        }
      } else {
        info(s"Add new unknown token $newToken")
        updateCreds.addToken(alias, newToken)
      }
    }

    if (updateCreds.numberOfTokens() > 0) {
      info("Update delegation tokens. " +
        s"The number of tokens sent by the server is ${newCreds.numberOfTokens()}. " +
        s"The actual number of updated tokens is ${updateCreds.numberOfTokens()}.")
      UserGroupInformation.getCurrentUser.addCredentials(updateCreds)
    }
  }
}
