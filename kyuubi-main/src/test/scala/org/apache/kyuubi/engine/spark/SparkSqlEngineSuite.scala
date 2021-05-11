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

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.JDBCTestUtils

class SparkSqlEngineSuite extends WithKyuubiServer with JDBCTestUtils {
  override protected val conf: KyuubiConf = {
    KyuubiConf()
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


  test("Fail connections on invalid sub domains") {
    Seq("1", ",", "", "a" * 11, "abc.xyz").foreach { invalid =>
      val sparkHiveConfigs = Map(
        ENGINE_SHARE_LEVEL.key -> "USER",
        ENGINE_SHARE_LEVEL_SUB_DOMAIN.key -> invalid)
      withSessionConf(Map.empty)(sparkHiveConfigs)(Map.empty) {
        assertJDBCConnectionFail()
      }
    }
  }

  test("Engine isolation with sub domain configurations") {
    val sparkHiveConfigs = Map(
      ENGINE_SHARE_LEVEL.key -> "USER",
      ENGINE_SHARE_LEVEL_SUB_DOMAIN.key -> "spark",
      "spark.driver.memory" -> "1000M")
    var mem: String = null
    withSessionConf(Map.empty)(sparkHiveConfigs)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET spark.driver.memory")
        assert(rs.next())
        mem = rs.getString(2)
        assert(mem === "1000M")
      }
    }

    val sparkHiveConfigs2 = Map(
      ENGINE_SHARE_LEVEL.key -> "USER",
      ENGINE_SHARE_LEVEL_SUB_DOMAIN.key -> "spark",
      "spark.driver.memory" -> "1001M")
    withSessionConf(Map.empty)(sparkHiveConfigs2)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET spark.driver.memory")
        assert(rs.next())
        mem = rs.getString(2)
        assert(mem === "1000M", "The sub-domain is same, so the engine reused")
      }
    }

    val sparkHiveConfigs3 = Map(
      ENGINE_SHARE_LEVEL.key -> "USER",
      ENGINE_SHARE_LEVEL_SUB_DOMAIN.key -> "kyuubi",
      "spark.driver.memory" -> "1002M")
    withSessionConf(Map.empty)(sparkHiveConfigs3)(Map.empty) {
      withJdbcStatement() { statement =>
        val rs = statement.executeQuery(s"SET spark.driver.memory")
        assert(rs.next())
        mem = rs.getString(2)
        assert(mem === "1002M", "The sub-domain is changed, so the engine recreated")
      }
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
