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

import java.net.URL
import java.sql.{Connection, ResultSet, SQLException, Statement}
import java.util.Locale

import scala.collection.mutable.ListBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.hsqldb.cmdline.SqlFile

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.statestore.StateStore
import org.apache.kyuubi.server.statestore.api.BatchMetadata
import org.apache.kyuubi.server.statestore.jdbc.DatabaseType._

class JDBCStateStore(conf: KyuubiConf) extends StateStore with Logging {
  import JDBCStateStore._

  private val dbType = DatabaseType.withName(conf.get(SERVER_STATE_STORE_JDBC_DB_TYPE))
  private val driverClassOpt = conf.get(SERVER_STATE_STORE_JDBC_DRIVER)
  private val driverClass = dbType match {
    case DERBY => driverClassOpt.getOrElse("org.apache.derby.jdbc.AutoloadedDriver")
    case MYSQL => driverClassOpt.getOrElse("com.mysql.jdbc.Driver")
    case CUSTOM => driverClassOpt.getOrElse(
        throw new IllegalArgumentException("No jdbc driver defined"))
  }

  private val hikariConfig = new HikariConfig()
  hikariConfig.setDriverClassName(driverClass)
  hikariConfig.setJdbcUrl(conf.get(SERVER_STATE_STORE_JDBC_URL))
  hikariConfig.setUsername(conf.get(SERVER_STATE_STORE_JDBC_USER))
  hikariConfig.setPassword(conf.get(SERVER_STATE_STORE_JDBC_PASSWORD))
  hikariConfig.setPoolName("kyuubi-state-store-pool")
  conf.getStateStoreJDBCDataSourceProperties.foreach { case (key, value) =>
    hikariConfig.addDataSourceProperty(key, value)
  }
  private val hikariDataSource = new HikariDataSource(hikariConfig)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val stateMaxAge = conf.get(SERVER_STATE_STORE_MAX_AGE)

  if (conf.get(SERVER_STATE_STORE_JDBC_DB_SCHEMA_INIT)) {
    initSchema()
  }

  private def initSchema(): Unit = {
    val classLoader = getClass.getClassLoader
    val initScriptUrl: Option[URL] = dbType match {
      case DERBY =>
        Option(classLoader.getResource("sql/derby/statestore-schema.derby.sql"))
      case MYSQL =>
        Option(classLoader.getResource("sql/mysql/statestore-schema.mysql.sql"))
      case CUSTOM => None
    }
    initScriptUrl.foreach { url =>
      val sf = new SqlFile(url)
      withConnection() { connection =>
        // for derby, it does not support `create table if not exists` syntax
        Utils.tryLogNonFatalError {
          sf.setConnection(connection)
          sf.execute()
        }
      }
    }
  }

  override def shutdown(): Unit = {
    hikariDataSource.close()
  }

  override def createBatch(batch: BatchMetadata): Unit = {
    val query =
      s"""
         |INSERT INTO $BATCH_METADATA_TABLE(
         |BATCH_ID,
         |BATCH_OWNER,
         |IP_ADDRESS,
         |SESSION_CONF,
         |KYUUBI_INSTANCE,
         |BATCH_TYPE,
         |RESOURCE,
         |CLASS_NAME,
         |NAME,
         |CONF,
         |ARGS,
         |STATE,
         |CREATE_TIME
         |)
         |VALUES
         |(
         |${sqlColValue(batch.batchId)},
         |${sqlColValue(batch.batchOwner)},
         |${sqlColValue(batch.ipAddress)},
         |${sqlColValue(valueAsString(batch.sessionConf))},
         |${sqlColValue(batch.kyuubiInstance)},
         |${sqlColValue(batch.batchType)}
         |${sqlColValue(batch.resource)},
         |${sqlColValue(batch.className)},
         |${sqlColValue(batch.name)},
         |${sqlColValue(valueAsString(batch.conf))},
         |${sqlColValue(valueAsString(batch.args))},
         |${sqlColValue(batch.state)},
         |${sqlColValue(batch.createTime)}
         |)""".stripMargin
    executeQuery(query)
  }

  override def updateBatchAppInfo(
      batchId: String,
      appId: String,
      appName: String,
      appUrl: String,
      appState: String,
      appError: Option[String]): Unit = {
    val query =
      s"""
         |UPDATE $BATCH_METADATA_TABLE
         |SET
         |APP_ID=${sqlColValue(appId)},
         |APP_NAME=${sqlColValue(appName)},
         |APP_URL=${sqlColValue(appUrl)},
         |APP_STATE=${sqlColValue(appState)},
         |APP_ERROR=${sqlColValue(appError.orNull)}
         |WHERE BATCH_ID=${sqlColValue(batchId)}
        """.stripMargin
    executeQuery(query)
  }

  override def closeBatch(batchId: String, state: String, endTime: Long): Unit = {
    val query =
      s"""
         |UPDATE $BATCH_METADATA_TABLE
         |SET
         |STATE=${sqlColValue(state)},
         |END_TIME=${sqlColValue(endTime)}
         |WHERE BATCH_ID=${sqlColValue(batchId)}
        """.stripMargin
    executeQuery(query)
  }

  override def getBatches(
      batchType: String,
      batchOwner: String,
      batchState: String,
      from: Int,
      size: Int): Seq[BatchMetadata] = {
    val queryBuilder = new StringBuilder
    queryBuilder.append(s"SELECT $STATE_ONLY_COLUMNS FROM $BATCH_METADATA_TABLE")
    val whereConditions = ListBuffer[String]()
    Option(batchType).filter(_.nonEmpty).foreach { _ =>
      whereConditions += s" UPPER(BATCH_TYPE)=${sqlColValue(batchType.toUpperCase(Locale.ROOT))} "
    }
    Option(batchOwner).filter(_.nonEmpty).foreach { _ =>
      whereConditions += s" BATCH_OWNER=${sqlColValue(batchOwner)} "
    }
    Option(batchState).filter(_.nonEmpty).foreach { _ =>
      whereConditions += s" STATE=${sqlColValue(batchState)} "
    }
    if (whereConditions.nonEmpty) {
      queryBuilder.append(whereConditions.mkString(" WHERE ", " AND ", " "))
    }
    queryBuilder.append(" ORDER BY KEY_ID ")
    queryBuilder.append(s" {LIMIT $size OFFSET $from} ")
    withConnection() { connection =>
      val rs = execute(connection, queryBuilder.toString())
      buildBatches(rs)
    }
  }

  override def getBatchesToRecover(
      kyuubiInstance: String,
      from: Int,
      size: Int): Seq[BatchMetadata] = {
    val query =
      s"""
         |SELECT * FROM $BATCH_METADATA_TABLE
         |WHERE
         |KYUUBI_INSTANCE=${sqlColValue(kyuubiInstance)}
         |AND END_TIME IS NULL
         |ORDER BY KEY_ID
         |{LIMIT $size OFFSET $from}
         |""".stripMargin
    withConnection() { connection =>
      val rs = execute(connection, query)
      buildBatches(rs)
    }
  }

  override def getBatch(batchId: String): BatchMetadata = {
    val query =
      s"""SELECT $STATE_ONLY_COLUMNS
         |FROM $BATCH_METADATA_TABLE
         |WHERE
         |BATCH_ID=${sqlColValue(batchId)}""".stripMargin
    withConnection() { connection =>
      val rs = execute(connection, query)
      buildBatches(rs).headOption.orNull
    }
  }

  override def cleanupBatch(batchId: String): Unit = {
    val query = s"DELETE FROM $BATCH_METADATA_TABLE WHERE BATCH_ID=${sqlColValue(batchId)}"
    executeQuery(query)
  }

  override def checkAndCleanupBatches(): Unit = {
    val minEndTime = System.currentTimeMillis() - stateMaxAge
    val query =
      s"""
         |DELETE FROM $BATCH_METADATA_TABLE
         |WHERE
         |END_TIME IS NOT NULL AND END_TIME < ${sqlColValue(minEndTime)}
         |""".stripMargin
    executeQuery(query)
  }

  private def buildBatches(resultSet: ResultSet): Seq[BatchMetadata] = {
    try {
      val batches = ListBuffer[BatchMetadata]()
      while (resultSet.next()) {
        val batchId = resultSet.getString("BATCH_ID")
        val batchOwner = resultSet.getString("BATCH_OWNER")
        val ipAddress = resultSet.getString("IP_ADDRESS")
        val sessionConf = string2Map(resultSet.getString("SESSION_CONF"))
        val kyuubiInstance = resultSet.getString("KYUUBI_INSTANCE")
        val batchType = resultSet.getString("BATCH_TYPE")
        val resource = resultSet.getString("RESOURCE")
        val className = resultSet.getString("CLASS_NAME")
        val name = resultSet.getString("NAME")
        val conf = string2Map(resultSet.getString("CONF"))
        val args = string2Seq(resultSet.getString("ARGS"))
        val state = resultSet.getString("STATE")
        val createTime = resultSet.getLong("CREATE_TIME")
        val appId = resultSet.getString("APP_ID")
        val appName = resultSet.getString("APP_NAME")
        val appUrl = resultSet.getString("APP_URL")
        val appState = resultSet.getString("APP_STATE")
        val appError = Option(resultSet.getString("APP_ERROR"))
        val endTime = resultSet.getLong("END_TIME")
        val batch = BatchMetadata(
          batchId,
          batchOwner,
          ipAddress,
          sessionConf,
          kyuubiInstance,
          batchType,
          resource,
          className,
          name,
          conf,
          args,
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
    } finally {
      Utils.tryLogNonFatalError(resultSet.close())
    }
  }

  private def execute(conn: Connection, sql: String): ResultSet = {
    debug(s"executing sql $sql")
    var statement: Statement = null
    var hasResult: Boolean = false
    var resultSet: ResultSet = null
    try {
      statement = conn.createStatement()
      hasResult = statement.execute(sql)
      if (hasResult) {
        statement.closeOnCompletion()
        resultSet = statement.getResultSet
      }
    } catch {
      case e: SQLException =>
        if (statement != null) {
          Utils.tryLogNonFatalError(statement.close())
        }
        throw new KyuubiException(e.getMessage, e)
    } finally {
      if (!hasResult && statement != null) {
        Utils.tryLogNonFatalError(statement.close())
      }
    }
    resultSet
  }

  private def withConnection[T](autoCommit: Boolean = true)(f: Connection => T): T = {
    var connection: Connection = null
    try {
      connection = hikariDataSource.getConnection
      connection.setAutoCommit(autoCommit)
      f(connection)
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  private def executeQuery(query: String): Unit = {
    withConnection() { connection =>
      execute(connection, query)
    }
  }

  private def sqlColValue(obj: Any): String = {
    Option(obj).map {
      case str: String => s"'$str'"
      case _ => obj.toString
    }.getOrElse("null")
  }

  private def valueAsString(obj: Any): String = {
    mapper.writeValueAsString(obj)
  }

  private def string2Map(str: String): Map[String, String] = {
    if (str == null || str.isEmpty) {
      Map.empty
    } else {
      mapper.readValue(str, classOf[Map[String, String]])
    }
  }

  private def string2Seq(str: String): Seq[String] = {
    if (str == null || str.isEmpty) {
      Seq.empty
    } else {
      mapper.readValue(str, classOf[Seq[String]])
    }
  }
}

object JDBCStateStore {
  private val BATCH_METADATA_TABLE = "BATCH_METADATA"
  private val STATE_ONLY_COLUMNS = Seq(
    "BATCH_ID",
    "BATCH_OWNER",
    "KYUUBI_INSTANCE",
    "BATCH_TYPE",
    "STATE",
    "APP_ID",
    "APP_NAME",
    "APP_URL",
    "APP_STATE",
    "APP_ERROR").mkString(",")
}
