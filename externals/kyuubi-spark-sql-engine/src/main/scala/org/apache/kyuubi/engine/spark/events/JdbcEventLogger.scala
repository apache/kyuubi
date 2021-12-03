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

package org.apache.kyuubi.engine.spark.events

import java.io._
import java.sql.{PreparedStatement, SQLException, SQLIntegrityConstraintViolationException, Statement}
import java.util.concurrent.TimeUnit

import com.alibaba.druid.pool.DruidPooledConnection
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.FSDataOutputStream

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SERVER_CLUSTER
import org.apache.kyuubi.engine.spark.events.JdbcEventLogger._
import org.apache.kyuubi.events.{EventLogger, KyuubiEvent}
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.InitMySQLDatabase

/**
 * This event logger insert kyuubi engine events into databases.
 * When we initialize this class, we will load the file: engine-event.sql first.
 * @param jdbcUrl jdbcUrl
 */
class JdbcEventLogger[T <: KyuubiEvent]()
  extends AbstractService("JdbcEventLogger") with EventLogger[T] with Logging {

  type Logger = (PrintWriter, Option[FSDataOutputStream])

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    InitMySQLDatabase.initialize(conf)
    loadSql(
      getClass.getClassLoader.getResourceAsStream("engine-event.sql"))
    super.initialize(conf)
  }

  private def loadSql(in: InputStream): Unit = {
    val sqlStringBuffer = new StringBuffer()
    var reader: BufferedReader = null
    try {
      reader = new BufferedReader(
        new InputStreamReader(in, "UTF-8"))
      var tmp: String = null
      while ({tmp = reader.readLine(); tmp} != null) {
        sqlStringBuffer.append(" ")
          .append(tmp)
      }
    } catch {
      case e: Exception =>
        error(s"Loading the file named engine-event failed. $e")
    } finally {
      in.close()
      reader.close()
    }
    val connection = InitMySQLDatabase.getConnection()
    val statement: Statement = connection.createStatement()
    val arr = sqlStringBuffer.toString.split(";")
    for ( i <- 0 until arr.length) {
      statement.addBatch(arr.apply(i))
    }
    try {
      statement.executeBatch()
    } catch {
      case e: SQLException =>
        error(s"Loading the file named engine-event failed. $e")
    } finally {
      InitMySQLDatabase.close(connection, null, statement)
    }
  }

  // Insert or update data into database by different eventType
  // todo: insert into DB should be batch not single
  override def logEvent(kyuubiEvent: T): Unit = {
    kyuubiEvent match {
      case engineEvent: EngineEvent =>
        val appId = engineEvent.applicationId
        val completeTime = engineEvent.endTime
        val connection: DruidPooledConnection = InitMySQLDatabase.getConnection()
        var engineSummaryStatement: PreparedStatement = null
        var engineDetailStatement: PreparedStatement = null
        if (checkExist(appId)
          && completeTime.compareTo(0) <= 0
          && StringUtils.isEmpty(engineEvent.diagnostic)) {
          // If the cache has this appId but endTime is less than 0 and diagnostic is null,
          // only need to insert data into detail table.
          val detailSQL = InitMySQLDatabase.insertSQL("engine_event_detail",
            ENGINE_EVENT_DETAIL_INSTER_FIELDS)
          engineDetailStatement = connection.prepareStatement(detailSQL.toString())
          engineDetailStatement.setString(1, appId)
          engineDetailStatement.setInt(2, engineEvent.state)
          engineDetailStatement.setLong(3, System.currentTimeMillis())
        } else {
          // There have two different result:
          // 1. AppId does not exist in cache
          // 2. The cache has this appId and endTime is greater than 0 or diagnostic is not null
          // Above results need to updateOrInsert summary table and insert data into detail table.
          val summarySQL = InitMySQLDatabase.insertOrUpdateSQL("engine_event_summary",
            ENGINE_EVENT_SUMMARY_INSERT_FIELDS, ENGINE_EVENT_SUMMARY_UPDATE_FIELDS)
          val detailSQL = InitMySQLDatabase.insertSQL("engine_event_detail",
            ENGINE_EVENT_DETAIL_INSTER_FIELDS)
          engineSummaryStatement = connection.prepareStatement(summarySQL.toString())
          engineSummaryStatement.setString(1, conf.get(SERVER_CLUSTER))
          engineSummaryStatement.setString(2, appId)
          engineSummaryStatement.setString(3, engineEvent.applicationName)
          engineSummaryStatement.setString(4, engineEvent.owner)
          engineSummaryStatement.setString(5, engineEvent.shareLevel)
          engineSummaryStatement.setString(6, engineEvent.connectionUrl)
          engineSummaryStatement.setString(7, engineEvent.master)
          engineSummaryStatement.setString(8, engineEvent.sparkVersion)
          engineSummaryStatement.setString(9, engineEvent.webUrl)
          engineSummaryStatement.setLong(10, engineEvent.startTime)
          engineSummaryStatement.setLong(11, completeTime)
          engineSummaryStatement.setString(12, engineEvent.diagnostic)
          engineSummaryStatement.setString(13, engineEvent.settings.toString())
          engineSummaryStatement.setLong(14, completeTime)
          engineSummaryStatement.setString(15, engineEvent.diagnostic)

          engineDetailStatement = connection.prepareStatement(detailSQL.toString())
          engineDetailStatement.setString(1, appId)
          engineDetailStatement.setInt(2, engineEvent.state)
          engineDetailStatement.setLong(3, System.currentTimeMillis())
        }
        try {
          if (engineSummaryStatement != null) {
            engineSummaryStatement.executeUpdate()
            CACHE.put(appId, appId)
          }
          if (engineDetailStatement != null) {
            engineDetailStatement.executeUpdate()
          }
        } catch {
          case e: SQLException =>
            error(s"Insert engine data into table failed, $e")
        } finally {
          InitMySQLDatabase.close(connection, engineSummaryStatement, null)
          InitMySQLDatabase.close(connection, engineDetailStatement, null)
        }
      case sessionEvent: SessionEvent =>
        val sessionId = sessionEvent.sessionId
        val connection: DruidPooledConnection = InitMySQLDatabase.getConnection()
        var sessionSummaryStatement: PreparedStatement = null
        var summarySQL: StringBuilder = null
        if (!checkExist(sessionId)) {
          // SessionId does not exist in cache, then should insert data into session summary table
          summarySQL = InitMySQLDatabase.insertSQL("session_event_summary",
            SESSION_EVENT_SUMMARY_INSERT_FIELDS)
          sessionSummaryStatement = connection.prepareStatement(summarySQL.toString())
          sessionSummaryStatement.setString(1, conf.get(SERVER_CLUSTER))
          sessionSummaryStatement.setString(2, sessionEvent.engineId)
          sessionSummaryStatement.setString(3, sessionId)
          sessionSummaryStatement.setString(4, sessionEvent.username)
          sessionSummaryStatement.setString(5, sessionEvent.ip)
          sessionSummaryStatement.setLong(6, sessionEvent.startTime)
          sessionSummaryStatement.setLong(7, sessionEvent.endTime)
          sessionSummaryStatement.setInt(8, sessionEvent.totalOperations)
        } else {
          summarySQL = InitMySQLDatabase.updateSessionSQL()
          sessionSummaryStatement = connection.prepareStatement(summarySQL.toString())
          sessionSummaryStatement.setLong(1, sessionEvent.endTime)
          sessionSummaryStatement.setInt(2, sessionEvent.totalOperations)
          sessionSummaryStatement.setInt(3, sessionEvent.totalOperations)
          sessionSummaryStatement.setString(4, sessionId)
        }
        try {
          sessionSummaryStatement.executeUpdate()
          CACHE.put(sessionId, sessionId)
        } catch {
          case _: SQLIntegrityConstraintViolationException =>
            warn(s"SessionId existed")
            CACHE.put(sessionId, sessionId)
          case e: SQLException =>
            error(s"Insert session data into table failed, $e")
        } finally {
          InitMySQLDatabase.close(connection, sessionSummaryStatement, null)
        }
      case sparkStatementEvent: SparkStatementEvent =>
        val statementId = sparkStatementEvent.statementId
        val completeTime = sparkStatementEvent.completeTime
        val connection: DruidPooledConnection = InitMySQLDatabase.getConnection()
        var statementSummaryStatement: PreparedStatement = null
        var statementDetailStatement: PreparedStatement = null
        if (checkExist(statementId)
          && completeTime.compareTo(0) <= 0
          && StringUtils.isEmpty(sparkStatementEvent.exception)) {
          // If the cache has this statementId but completeTime is less than 0,
          // and exception is null,
          // only need to insert data into detail table.
          val detailSQL = InitMySQLDatabase.insertSQL("spark_statement_event_detail", STATEMENT_EVENT_DETAIL_INSERT_FIELDS)
          statementDetailStatement = connection.prepareStatement(detailSQL.toString())
          statementDetailStatement.setString(1, statementId)
          statementDetailStatement.setString(2, sparkStatementEvent.state)
          statementDetailStatement.setLong(3, System.currentTimeMillis())
        } else {
          // There have two different result:
          // 1. StatementId does not exist in cache
          // 2. The cache has this statementId and completeTime is greater than 0
          // or diagnostic is not null
          // Above results need to updateOrInsert summary table and insert data into detail table.
          val summarySQL = InitMySQLDatabase.insertOrUpdateSQL("spark_statement_event_summary",
            STATEMENT_EVENT_SUMMARY_INSERT_FIELDS, STATEMENT_EVENT_SUMMARY_UPDATE_FIELDS)
          val detailSQL = InitMySQLDatabase.insertSQL("spark_statement_event_detail",
            STATEMENT_EVENT_DETAIL_INSERT_FIELDS)
          statementSummaryStatement = connection.prepareStatement(summarySQL.toString())
          statementSummaryStatement.setString(1, conf.get(SERVER_CLUSTER))
          statementSummaryStatement.setString(2, sparkStatementEvent.appId)
          statementSummaryStatement.setString(3, sparkStatementEvent.sessionId)
          statementSummaryStatement.setString(4, statementId)
          statementSummaryStatement.setString(5, sparkStatementEvent.statement)
          statementSummaryStatement.setString(6, sparkStatementEvent.username)
          statementSummaryStatement.setLong(7, sparkStatementEvent.createTime )
          statementSummaryStatement.setLong(8, sparkStatementEvent.completeTime)
          // todo: need to get exceptionType from exception
          statementSummaryStatement.setString(9, "exceptionType")
          statementSummaryStatement.setString(10, sparkStatementEvent.exception)
          statementSummaryStatement.setLong(11, sparkStatementEvent.completeTime)
          statementSummaryStatement.setString(12, "exceptionType")
          statementSummaryStatement.setString(13, sparkStatementEvent.exception)

          statementDetailStatement = connection.prepareStatement(detailSQL.toString())
          statementDetailStatement.setString(1, statementId)
          statementDetailStatement.setString(2, sparkStatementEvent.state)
          statementDetailStatement.setLong(3, System.currentTimeMillis())
        }
        try {
          if (statementSummaryStatement != null) {
            statementSummaryStatement.executeUpdate()
            CACHE.put(statementId, statementId)
          }
          if (statementDetailStatement != null) {
            statementDetailStatement.executeUpdate()
          }
        } catch {
          case e: SQLException =>
            error(s"Insert statement data into table failed, $e")
        } finally {
          InitMySQLDatabase.close(connection, statementSummaryStatement, null)
          InitMySQLDatabase.close(connection, statementDetailStatement, null)
        }
      case _ =>
    }
  }

  private def checkExist(id: String): Boolean = {
    if (StringUtils.isEmpty(CACHE.getIfPresent(id))) {
      return false;
    }
    return true
  }
}

object JdbcEventLogger {
  val CACHE: Cache[String, String] = CacheBuilder.newBuilder()
    .expireAfterWrite(24, TimeUnit.HOURS).build()

  val ENGINE_EVENT_SUMMARY_INSERT_FIELDS = "cluster,app_id,app_name,owner,share_level," +
    "connection_url,master,spark_version,web_url,start_time,complete_time,diagnostic,settings"
  val ENGINE_EVENT_SUMMARY_UPDATE_FIELDS = Array("complete_time", "diagnostic")
  val ENGINE_EVENT_DETAIL_INSTER_FIELDS = "app_id,state,event_time"

  val SESSION_EVENT_SUMMARY_INSERT_FIELDS = "cluster,app_id,session_id,user_name," +
    "ip,start_time,complete_time,total_operations"

  val STATEMENT_EVENT_SUMMARY_INSERT_FIELDS = "cluster,app_id,session_id,statement_id,statement," +
    "user_name,create_time,complete_time,exception_type,exception"
  val STATEMENT_EVENT_SUMMARY_UPDATE_FIELDS = Array("complete_time", "exception_type", "exception")
  val STATEMENT_EVENT_DETAIL_INSERT_FIELDS = "statement_id,state,event_time"
}
