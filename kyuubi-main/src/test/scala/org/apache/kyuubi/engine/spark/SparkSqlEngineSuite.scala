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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_INITIALIZE_SQL, SESSION_CONF_IGNORE_LIST, SESSION_CONF_RESTRICT_LIST}
import org.apache.kyuubi.operation.{JDBCTestUtils, WithKyuubiServer}

class SparkSqlEngineSuite extends WithKyuubiServer with JDBCTestUtils {
  override protected val conf: KyuubiConf = {
    KyuubiConf().set(ENGINE_INITIALIZE_SQL,
      "CREATE DATABASE IF NOT EXISTS INIT_DB;" +
        "CREATE TABLE IF NOT EXISTS INIT_DB.test(a int);" +
        "INSERT OVERWRITE TABLE INIT_DB.test SELECT 1;")
      .set(SESSION_CONF_IGNORE_LIST.key, "kyuubi.abc.xyz,spark.sql.abc.xyz,spark.sql.abc.var")
      .set(SESSION_CONF_RESTRICT_LIST.key, "kyuubi.xyz.abc,spark.sql.xyz.abc,spark.sql.xyz.abc.var")
  }

  private var _sessionConfs: Map[String, String] = Map.empty
  private var _sparkHiveConfs: Map[String, String] = Map.empty
  private var _sparkHiveVars: Map[String, String] = Map.empty

  private def withSessionConf[T](
      sessionConfs: Map[String, String])(
      sparkHiveConfs: Map[String, String])(
      sparkHiveVars: Map[String, String])(f: => T): T = {
    this._sessionConfs = sessionConfs
    this._sparkHiveConfs = sparkHiveConfs
    this._sparkHiveVars = sparkHiveVars
    try f finally {
      _sparkHiveVars = Map.empty
      _sparkHiveConfs = Map.empty
      _sessionConfs = Map.empty
    }
  }

  override def afterAll(): Unit = {
    withJdbcStatement() { statement =>
      statement.executeQuery("DROP TABLE IF EXISTS INIT_DB.test")
      statement.executeQuery("DROP DATABASE IF EXISTS INIT_DB")
    }
    super.afterAll()
  }

  test("KYUUBI-457: Support configurable initialize sql statement for engine startup") {
    withJdbcStatement() { statement =>
      val result = statement.executeQuery("SELECT * FROM INIT_DB.test")
      assert(result.next())
      assert(result.getInt(1) == 1)
      assert(!result.next())
    }
  }

  test("ignore config via system settings") {
    val sessionConf = Map("kyuubi.abc.xyz" -> "123", "kyuubi.abc.xyz0" -> "123")
    val sparkHiveConfs = Map("spark.sql.abc.xyz" -> "123", "spark.sql.abc.xyz0" -> "123")
    val sparkHiveVars = Map("spark.sql.abc.var" -> "123", "spark.sql.abc.var0" -> "123")
    withSessionConf(sessionConf)(sparkHiveConfs)(sparkHiveVars) {
     withJdbcStatement() { statement =>
       Seq("spark.sql.abc.xyz", "spark.sql.abc.var").foreach { key =>
         val rs1 = statement.executeQuery(s"SET ${key}0")
         assert(rs1.next())
         assert(rs1.getString("value") === "123")
         val rs2 = statement.executeQuery(s"SET $key")
         assert(rs2.next())
         assert(rs2.getString("value") === "<undefined>", "ignored")
       }
     }
   }
  }

  test("restricted config via system settings") {
    val sessionConfMap = Map("kyuubi.xyz.abc" -> "123", "kyuubi.abc.xyz" -> "123")
    withSessionConf(sessionConfMap)(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        sessionConfMap.keys.foreach { key =>
          val rs = statement.executeQuery(s"SET $key")
          assert(rs.next())
          assert(rs.getString("value") === "<undefined>",
            "session configs do not reach on server-side")
        }

      }
    }

    withSessionConf(Map.empty)(Map("spark.sql.xyz.abc" -> "123"))(Map.empty) {
      assertJDBCConnectionFail()
    }

    withSessionConf(Map.empty)(Map.empty)(Map("spark.sql.xyz.abc.var" -> "123")) {
      assertJDBCConnectionFail()
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl

  override protected def sessionConfigs: Map[String, String] = {
    super.sessionConfigs ++: _sessionConfs
  }

  override protected def sparkHiveConfigs: Map[String, String] = {
    super.sparkHiveConfigs ++: _sparkHiveConfs
  }

  override protected def sparkHiveVars: Map[String, String] = {
    super.sparkHiveVars ++: _sparkHiveVars
  }
}
