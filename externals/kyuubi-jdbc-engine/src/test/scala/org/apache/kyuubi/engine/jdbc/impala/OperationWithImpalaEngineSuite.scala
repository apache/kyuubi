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
package org.apache.kyuubi.engine.jdbc.impala

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.jdbc.connection.ConnectionProvider
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoReq, TGetInfoType}

class OperationWithImpalaEngineSuite extends ImpalaOperationSuite with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = jdbcConnectionUrl

  test("impala - test for Jdbc engine getInfo") {
    val metaData = ConnectionProvider.create(kyuubiConf).getMetaData

    withSessionConf(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "ENGINE"))()() {
      withSessionHandle { (client, handle) =>
        val req = new TGetInfoReq()
        req.setSessionHandle(handle)
        req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
        assert(client.GetInfo(req).getInfoValue.getStringValue == metaData.getDatabaseProductName)

        val req2 = new TGetInfoReq()
        req2.setSessionHandle(handle)
        req2.setInfoType(TGetInfoType.CLI_DBMS_VER)
        assert(
          client.GetInfo(req2).getInfoValue.getStringValue == metaData.getDatabaseProductVersion)

        val req3 = new TGetInfoReq()
        req3.setSessionHandle(handle)
        req3.setInfoType(TGetInfoType.CLI_MAX_COLUMN_NAME_LEN)
        assert(client.GetInfo(req3).getInfoValue.getLenValue == metaData.getMaxColumnNameLength)
      }
    }
  }
}
