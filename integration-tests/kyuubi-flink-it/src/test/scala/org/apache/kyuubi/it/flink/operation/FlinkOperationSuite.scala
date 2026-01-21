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

package org.apache.kyuubi.it.flink.operation

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.it.flink.WithKyuubiServerAndFlinkMiniCluster
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.TABLE_CAT
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TGetInfoReq, TGetInfoType}

class FlinkOperationSuite extends WithKyuubiServerAndFlinkMiniCluster
  with HiveJDBCTestHelper {

  override val conf: KyuubiConf = KyuubiConf()
    .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME_ENV_VAR_NAME", kyuubiHome)
    .set(ENGINE_TYPE, "FLINK_SQL")
    .set("flink.parallelism.default", "2")

  override protected def jdbcUrl: String = getJdbcUrl

  test("get catalogs for flink sql") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val catalogs = meta.getCatalogs
      val expected = Set("default_catalog").toIterator
      while (catalogs.next()) {
        assert(catalogs.getString(TABLE_CAT) === expected.next())
      }
      assert(!expected.hasNext)
      assert(!catalogs.next())
    }
  }

  test("execute statement - create/alter/drop table") {
    withJdbcStatement()({ statement =>
      statement.executeQuery("create table tbl_a (a string) with ('connector' = 'blackhole')")
      assert(statement.execute("alter table tbl_a rename to tbl_b"))
      assert(statement.execute("drop table tbl_b"))
    })
  }

  test("execute statement - select column name with dots") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select 'tmp.hello'")
      assert(resultSet.next())
      assert(resultSet.getString(1) === "tmp.hello")
    }
  }

  test("set kyuubi conf into flink conf") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET")
      // Flink does not support set key without value currently,
      // thus read all rows to find the desired one
      var success = false
      while (resultSet.next() && !success) {
        if (resultSet.getString(1) == "parallelism.default" &&
          resultSet.getString(2) == "2") {
          success = true
        }
      }
      assert(success)
    }
  }

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
        assert(client.GetInfo(req).getInfoValue.getStringValue === "Apache Flink")
        req.setInfoType(TGetInfoType.CLI_ODBC_KEYWORDS)
        assert(client.GetInfo(req).getInfoValue.getStringValue === "Unimplemented")
      }
    }
  }
}
