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

package org.apache.kyuubi.operation

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoReq, TGetInfoType}

class KyuubiServerInfoProviderSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl

  // `kyuubi.server.info.provider` has audience(SERVER), mimicking a
  // kyuubi-defaults.conf entry, so the test exercises the audience(SERVER) fallback path.
  override protected val conf: KyuubiConf =
    KyuubiConf().set(KyuubiConf.SERVER_INFO_PROVIDER, "SERVER")

  test("KYUUBI #7435: honor server-level kyuubi.server.info.provider in getInfo") {
    withSessionHandle { (client, handle) =>
      val req = new TGetInfoReq()
      req.setSessionHandle(handle)
      req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
      // Without the fix the audience(SERVER) value is stripped from the session conf and getInfo
      // falls back to ENGINE, which would return the engine name ("Spark SQL") instead.
      assert(client.GetInfo(req).getInfoValue.getStringValue === "Apache Kyuubi")
    }
  }
}
