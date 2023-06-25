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

package org.apache.kyuubi.it.hive.operation

import org.apache.hive.service.rpc.thrift.{TGetInfoReq, TGetInfoType}

import org.apache.kyuubi.{HiveEngineTests, Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class KyuubiOperationHiveEnginePerUserSuite extends WithKyuubiServer with HiveEngineTests {

  val kyuubiHome: String = Utils.getCodeSourceLocation(getClass).split("integration-tests").head

  override protected val conf: KyuubiConf = {
    val metastore = Utils.createTempDir(prefix = getClass.getSimpleName)
    metastore.toFile.delete()
    KyuubiConf()
      .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME", kyuubiHome)
      .set(ENGINE_TYPE, "HIVE_SQL")
      // increase this to 30s as hive session state and metastore client is slow initializing
      .setIfMissing(ENGINE_IDLE_TIMEOUT, 30000L)
      .set("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$metastore;create=true")
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("server info provider - server") {
    withSessionConf(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "SERVER"))()() {
      withSessionHandle { (client, handle) =>
        val req = new TGetInfoReq()
        req.setSessionHandle(handle)
        req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
        assert(client.GetInfo(req).getInfoValue.getStringValue === "Apache Kyuubi")
      }
    }
  }

  test("server info provider - engine") {
    withSessionConf(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "ENGINE"))()() {
      withSessionHandle { (client, handle) =>
        val req = new TGetInfoReq()
        req.setSessionHandle(handle)
        req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
        assert(client.GetInfo(req).getInfoValue.getStringValue === "Apache Hive")
      }
    }
  }
}
