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

import java.sql.{DriverManager, Statement}
import java.util.Locale

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.KyuubiFunSuite

trait WithSparkSQLEngine extends KyuubiFunSuite {

  protected val spark: SparkSession = SparkSQLEngine.createSpark()

  protected var engine: SparkSQLEngine = _

  protected var connectionUrl: String = _

  override def beforeAll(): Unit = {
    engine = SparkSQLEngine.startEngine(spark)
    connectionUrl = engine.connectionUrl
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (engine != null) {
      engine.stop()
    }
    SessionState.detachSession()
    Hive.closeCurrent()
  }

  protected def jdbcUrl: String = s"jdbc:hive2://$connectionUrl/"


  protected def withMultipleConnectionJdbcStatement(
      tableNames: String*)(fs: (Statement => Unit)*): Unit = {
    val user = System.getProperty("user.name")
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
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  protected def withDatabases(dbNames: String*)(fs: (Statement => Unit)*): Unit = {
    val user = System.getProperty("user.name")
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUrl, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      dbNames.foreach { name =>
        statements.head.execute(s"DROP DATABASE IF EXISTS $name")
      }
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  protected def withJdbcStatement(tableNames: String*)(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(tableNames: _*)(f)
  }
}
