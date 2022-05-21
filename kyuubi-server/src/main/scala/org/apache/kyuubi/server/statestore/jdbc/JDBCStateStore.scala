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

package org.apache.kyuubi.server.statestore.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Statement}
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.statestore.StateStore
import org.apache.kyuubi.server.statestore.api.BatchState

import scala.collection.mutable.ListBuffer

class JDBCStateStore(conf: KyuubiConf) extends StateStore with Logging {
  private var hikariDataSource: HikariDataSource = _

  override def initialize(): Unit = {
    val hikariConfig = new HikariConfig()
    hikariConfig.setDriverClassName(conf.get(SERVER_STATE_STORE_JDBC_DRIVER))
    hikariConfig.setJdbcUrl(conf.get(SERVER_STATE_STORE_JDBC_URL))
    hikariConfig.setUsername(conf.get(SERVER_STATE_STORE_JDBC_USER))
    hikariConfig.setPassword(conf.get(SERVER_STATE_STORE_JDBC_PASSWORD))
    conf.getStateStoreJDBCDataSourceProperties.foreach { case (key, value) =>
      hikariConfig.addDataSourceProperty(key, value)
    }
    hikariDataSource = new HikariDataSource(hikariConfig)
  }

  override def shutdown(): Unit = {
    hikariDataSource.close()
  }

  override def createBatch(batch: BatchState): Unit = {
    val connection = getConnection()
    execute(connection, "")
  }

  override def getBatch(batchId: String): BatchState = {
    val connection = getConnection()
    val rs = executeQuery(connection, "")
    buildBatches(rs).headOption.orNull
  }

  private def buildBatches(resultSet: ResultSet): Seq[BatchState] = {
    val batches = ListBuffer[BatchState]()
    while (resultSet.next()) {
      val id = resultSet.getString("id")
      val batchType = resultSet.getString("batch_type")
      val batchOwner = resultSet.getString("batch_owner")
      val kyuubiInstance = resultSet.getString("kyuubi_instance")
      val state = resultSet.getString("state")
      val createTime = resultSet.getLong("create_time")
      val appId = resultSet.getString("app_id")
      val appName = resultSet.getString("app_name")
      val appUrl = resultSet.getString("app_url")
      val appState = resultSet.getString("app_state")
      val appError = Option(resultSet.getString("app_error"))
      val endTime = resultSet.getLong("end_time")
      val batch = BatchState(
        id,
        batchType,
        batchOwner,
        kyuubiInstance,
        state,
        createTime,
        appId,
        appName,
        appUrl,
        appState,
        appError,
        endTime)
      batches += batch
    }
    batches
  }

  private def getConnection(autoCommit: Boolean = true): Connection = {
    try {
      val connection = hikariDataSource.getConnection
      connection.setAutoCommit(autoCommit)
      connection
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    }
  }

  private def execute(conn: Connection, sql: String): Unit = {
    debug(s"executing sql $sql")
    var statement: Statement = null
    try {
      statement = conn.createStatement()
      statement.execute(sql)
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    } finally {
      if (statement != null) {
        statement.close()
      }
    }
  }

  private def executeUpdate(conn: Connection, sql: String, params: _*): Int = {
    debug(s"executing update $sql")
    var result: Int = 0
    var statement: PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParam(statement, params)
      result = statement.executeUpdate()
      statement.closeOnCompletion()
    } catch onStatementError(statement)
    result
  }

  private def executeQuery(conn: Connection, sql: String, params: _*): ResultSet = {
    debug(s"executing update $sql")
    var rs: ResultSet = null
    var statement: PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParam(statement, params)
      rs = statement.executeQuery()
      statement.closeOnCompletion()
    } catch onStatementError(statement)
    rs
  }

  private def onStatementError(statement: Statement): PartialFunction[Throwable, Unit] = {
    case e: SQLException =>
      if (statement != null) {
        Utils.tryLogNonFatalError(statement.close())
      }
      throw new KyuubiException(e.getMessage, e)
  }

  private def setStatementParam(statement: PreparedStatement, params: _*): Unit = {
    if (params != null) {
      params.zipWithIndex.map { case (param, index) =>
        param match {
          case s: String => statement.setString(index + 1, s)
          case i: Int => statement.setInt(index + 1, i)
          case d: Double => statement.setDouble(index + 1, d)
          case l: Long => statement.setLong(index + 1, l)
          case _ => throw new KyuubiException(s"Unsupported date type:${param.getClass.getName}")
        }

      }
    }
  }
}
