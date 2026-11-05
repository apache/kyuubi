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

package org.apache.kyuubi.engine.spark.session

import java.sql.DriverManager

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class ServerLocalIsolatedSparkSessionSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override def withKyuubiConf: Map[String, String] = {
    Map(
      ENGINE_SHARE_LEVEL.key -> "SERVER_LOCAL",
      ENGINE_SINGLE_SPARK_SESSION.key -> "false",
      ENGINE_USER_ISOLATED_SPARK_SESSION.key -> "true")
  }

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/;#spark.ui.enabled=false"

  private def queryString(stmt: java.sql.Statement, sql: String): String = {
    val rs = stmt.executeQuery(sql)
    assert(rs.next())
    rs.getString(2)
  }

  test("same user connections keep isolated spark sessions in SERVER_LOCAL share level") {
    val conn1 = DriverManager.getConnection(jdbcUrlWithConf, user, password)
    val conn2 = DriverManager.getConnection(jdbcUrlWithConf, user, password)
    try {
      val stmt1 = conn1.createStatement()
      val stmt2 = conn2.createStatement()

      stmt1.execute("set a=1")
      assert(queryString(stmt1, "set a") == "1")
      assert(queryString(stmt2, "set a") == "<undefined>")
      stmt2.execute("set a=2")
      assert(queryString(stmt1, "set a") == "1")
      assert(queryString(stmt2, "set a") == "2")

      stmt1.execute("create temporary view tv_isolated as select 1 as id")
      assert(stmt1.executeQuery("select * from tv_isolated").next())
      intercept[java.sql.SQLException](stmt2.executeQuery("select * from tv_isolated"))

      conn1.close()
      assert(queryString(stmt2, "set a") == "2")
      val rs = stmt2.executeQuery("select 1")
      assert(rs.next() && rs.getInt(1) == 1)
    } finally {
      conn1.close()
      conn2.close()
    }
  }
}
