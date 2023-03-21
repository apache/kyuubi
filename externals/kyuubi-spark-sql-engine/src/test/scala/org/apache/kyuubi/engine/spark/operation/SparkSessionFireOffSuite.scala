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

package org.apache.kyuubi.engine.spark.operation

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.config.KyuubiConf.SESSION_CLOSE_GRACEFULLY
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

abstract class SparkSessionFireOffSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl

  test("close engine session test") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("kentyao")
      req.setPassword("anonymous")
      val conf = Map(
        "use:database" -> "default")
      req.setConfiguration(conf.asJava)

      val tOpenSessionResp = client.OpenSession(req)
      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setStatement("create table kyuubi_test(id string) stored as parquet")
      client.ExecuteStatement(tExecuteStatementReq)

      val tExecuteStatementReq1 = new TExecuteStatementReq()
      tExecuteStatementReq1.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq1.setRunAsync(true)
      tExecuteStatementReq1.setStatement("insert into kyuubi_test select " +
        "java_method('java.lang.Thread', 'sleep', 5000L);")
      client.ExecuteStatement(tExecuteStatementReq1)

      val tCloseSessionReq = new TCloseSessionReq()
      tCloseSessionReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      val tCloseSessionResp = client.CloseSession(tCloseSessionReq)
      assert(tCloseSessionResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    }
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select * from kyuubi_test;")
      val engineSessionCloseGracefully = kyuubiConf.get(SESSION_CLOSE_GRACEFULLY)
      if (engineSessionCloseGracefully) {
        assert(resultSet.next())
        assert(resultSet.getString("id") == "null")
      } else {
        assert(resultSet.next() == false)
      }
    }
  }

}

class SparkSessionCloseDirectly extends SparkSessionFireOffSuite {
  override def withKyuubiConf: Map[String, String] = Map.empty
}

class SparkSessionCloseGracefully extends SparkSessionFireOffSuite {
  override def withKyuubiConf: Map[String, String] = Map(
    "kyuubi.session.engine.spark.close.gracefully" -> "true")
}
