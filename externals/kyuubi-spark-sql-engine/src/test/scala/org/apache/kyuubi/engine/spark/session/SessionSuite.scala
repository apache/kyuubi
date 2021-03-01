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

import java.sql.{DriverManager, Statement}
import java.util.Locale

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.SparkSQLEngine

class SessionSuite extends KyuubiFunSuite {
  var spark: SparkSession = _
  var engine: SparkSQLEngine = _
  protected val user: String = Utils.currentUser

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    spark = SparkSQLEngine.createSpark(
      (CONNECTION_RELEASE_ON_CLOSE.key -> "true"), (ENGINE_SHARED_LEVEL.key -> "CONNECTION"))
    engine = SparkSQLEngine.startEngine(spark)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    if (engine != null) {
      engine.stop()
      engine = null
    }
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  test("release session if shared level is CONNECTION") {
    assert(engine.isAlive())
    withJdbcStatement(engine.connectionUrl) {_ => }
    assert(!engine.isAlive())
  }

  test("don't release session if kyuubi.connection.release.onClose is false") {
    assert(engine.isAlive())
    engine.getConf.set(CONNECTION_RELEASE_ON_CLOSE.key, "false")
    assert(engine.isAlive())
  }

  protected def withJdbcStatement(
    jdbcUrl: String, tableNames: String*)(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(jdbcUrl, tableNames: _*)(f)
  }

  protected def withMultipleConnectionJdbcStatement(
    jdbcUrl: String, tableNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUrl, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      tableNames.foreach { name =>
        if (name.toUpperCase(Locale.ROOT).startsWith("VIEW")) {
          statements.head.execute(s"DROP VIEW IF EXISTS $name")
        } else {
          statements.head.execute(s"DROP TABLE IF EXISTS $name")
        }
      }
      info("Closing statements")
      statements.foreach(_.close())
      info("Closed statements")
      connections.foreach(_.close())
      info("Closing connections")
    }
  }
}
