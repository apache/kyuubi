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

package org.apache.kyuubi.engine.hive

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.ha.client.{EngineServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.service.{Serverable, Service, TBinaryFrontendService}
import org.apache.kyuubi.service.TFrontendService.OK_STATUS
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TRenewDelegationTokenReq, TRenewDelegationTokenResp}
import org.apache.kyuubi.util.KyuubiHadoopUtils

class HiveTBinaryFrontendService(override val serverable: Serverable)
  extends TBinaryFrontendService("HiveTBinaryFrontend") {
  import HiveTBinaryFrontendService._

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new EngineServiceDiscovery(this))
    } else {
      None
    }
  }

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    debug(req.toString)

    // We hacked `TCLIService.Iface.RenewDelegationToken` to transfer Credentials from Kyuubi
    // Server to Hive SQL engine
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
}

object HiveTBinaryFrontendService {

  def renewDelegationToken(tokenStr: String): Unit = {
    val currentUser = UserGroupInformation.getCurrentUser
    // `currentUser` is either `UserGroupInformation.getLoginUser` or a proxy user.
    // If `currentUser` is a proxy user, it needs a HIVE_DELEGATION_TOKEN to pass
    // HiveMetastoreClient authentication.
    if (currentUser.getAuthenticationMethod == UserGroupInformation.AuthenticationMethod.PROXY) {
      val newCreds = KyuubiHadoopUtils.decodeCredentials(tokenStr)
      KyuubiHadoopUtils.getTokenMap(newCreds).values
        .find(_.getKind == new Text("HIVE_DELEGATION_TOKEN"))
        .foreach { token =>
          UserGroupInformation.getCurrentUser.addToken(token)
        }
    }
  }
}
