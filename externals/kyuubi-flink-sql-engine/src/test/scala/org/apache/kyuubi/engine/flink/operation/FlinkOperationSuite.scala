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

package org.apache.kyuubi.engine.flink.operation

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.engine.flink.WithFlinkSQLEngine
import org.apache.kyuubi.engine.flink.result.Constants
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.TABLE_TYPE
import org.apache.kyuubi.service.ServiceState._

class FlinkOperationSuite extends WithFlinkSQLEngine with HiveJDBCTestHelper {
  override def withKyuubiConf: Map[String, String] = Map()

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/"

  ignore("release session if shared level is CONNECTION") {
    logger.info(s"jdbc url is $jdbcUrl")
    assert(engine.getServiceState == STARTED)
    withJdbcStatement() { _ => }
    eventually(Timeout(20.seconds)) {
      assert(engine.getServiceState == STOPPED)
    }
  }

  test("get catalogs for flink sql") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val catalogs = meta.getCatalogs
      val expected = Set("default_catalog").toIterator
      while (catalogs.next()) {
        assert(catalogs.getString("catalogs") === expected.next())
      }
      assert(!expected.hasNext)
      assert(!catalogs.next())
    }
  }

  test("get table type for flink sql") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val types = meta.getTableTypes
      val expected = Constants.SUPPORTED_TABLE_TYPES.toIterator
      while (types.next()) {
        assert(types.getString(TABLE_TYPE) === expected.next())
      }
      assert(!expected.hasNext)
      assert(!types.next())
    }
  }

  test("execute statement - select column name with dots") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select 'tmp.hello'")
      assert(resultSet.next())
      assert(resultSet.getString(1) === "tmp.hello")
    }
  }
}
