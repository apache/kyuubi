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

class SessionIdleTimeoutSuite extends WithSparkSQLEngine {

  // Load JDBC driver
  Class.forName("org.apache.kyuubi.jdbc.KyuubiHiveDriver")

  override def withKyuubiConf: Map[String, String] = {
    Map(
      ENGINE_SHARE_LEVEL.key -> "SERVER",
      ENGINE_SPARK_MAX_INITIAL_WAIT.key -> "0")
  }

  private def baseJdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/default"

  test("KYUUBI #7304: session idle timeout supports ISO 8601 duration format") {
    // Test with ISO 8601 duration format (PT40M = 40 minutes = 2400000 ms)
    val jdbcUrlWithIso8601Timeout =
      s"$baseJdbcUrl;#${SESSION_IDLE_TIMEOUT.key}=PT40M"
    val conn = DriverManager.getConnection(jdbcUrlWithIso8601Timeout, "anonymous", "")
    try {
      val statement = conn.createStatement()
      try {
        val resultSet = statement.executeQuery("SELECT 1")
        assert(resultSet.next())
        assert(resultSet.getInt(1) == 1)
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  test("KYUUBI #7304: session idle timeout supports plain milliseconds") {
    // Test with plain milliseconds (2400000 ms = 40 minutes)
    val jdbcUrlWithMillisTimeout =
      s"$baseJdbcUrl;#${SESSION_IDLE_TIMEOUT.key}=2400000"
    val conn = DriverManager.getConnection(jdbcUrlWithMillisTimeout, "anonymous", "")
    try {
      val statement = conn.createStatement()
      try {
        val resultSet = statement.executeQuery("SELECT 1")
        assert(resultSet.next())
        assert(resultSet.getInt(1) == 1)
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  test("KYUUBI #7304: session idle timeout supports various ISO 8601 formats") {
    // Test with different ISO 8601 duration formats
    val testCases = Seq("PT1H", "PT30M", "PT1H30M", "PT90S")

    testCases.foreach { durationStr =>
      val jdbcUrlWithTimeout =
        s"$baseJdbcUrl;#${SESSION_IDLE_TIMEOUT.key}=$durationStr"
      val conn = DriverManager.getConnection(jdbcUrlWithTimeout, "anonymous", "")
      try {
        val statement = conn.createStatement()
        try {
          val resultSet = statement.executeQuery("SELECT 1")
          assert(resultSet.next())
          assert(resultSet.getInt(1) == 1)
        } finally {
          statement.close()
        }
      } finally {
        conn.close()
      }
    }
  }
}
