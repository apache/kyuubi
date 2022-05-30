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

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.{Locale, Properties}
import java.util.stream.Collectors

import scala.collection.mutable.ListBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.annotations.VisibleForTesting
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.server.statestore.StateStore
import org.apache.kyuubi.server.statestore.api.Metadata
import org.apache.kyuubi.server.statestore.jdbc.DatabaseType._

class JDBCStateStore(conf: KyuubiConf) extends StateStore with Logging {
  import JDBCStateStore._

  private val dbType = DatabaseType.withName(conf.get(SERVER_STATE_STORE_JDBC_DATABASE_TYPE))
  private val driverClassOpt = conf.get(SERVER_STATE_STORE_JDBC_DRIVER)
  private val driverClass = dbType match {
    case DERBY => driverClassOpt.getOrElse("org.apache.derby.jdbc.AutoloadedDriver")
    case MYSQL => driverClassOpt.getOrElse("com.mysql.jdbc.Driver")
    case CUSTOM => driverClassOpt.getOrElse(
        throw new IllegalArgumentException("No jdbc driver defined"))
  }

  private val databaseAdaptor = dbType match {
    case DERBY => new DerbyDatabaseDialect
    case MYSQL => new MysqlDatabaseDialect
    case CUSTOM => new GenericDatabaseDialect
  }

  private val datasourceProperties = new Properties()
  conf.getStateStoreJDBCDataSourceProperties.foreach { case (key, value) =>
    datasourceProperties.put(key, value)
  }

  private val hikariConfig = new HikariConfig(datasourceProperties)
  hikariConfig.setDriverClassName(driverClass)
  hikariConfig.setJdbcUrl(conf.get(SERVER_STATE_STORE_JDBC_URL))
  hikariConfig.setUsername(conf.get(SERVER_STATE_STORE_JDBC_USER))
  hikariConfig.setPassword(conf.get(SERVER_STATE_STORE_JDBC_PASSWORD))
  hikariConfig.setPoolName("kyuubi-state-store-pool")

  @VisibleForTesting
  private[kyuubi] val hikariDataSource = new HikariDataSource(hikariConfig)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  if (conf.get(SERVER_STATE_STORE_JDBC_DATABASE_SCHEMA_INIT)) {
    initSchema()
  }

  private def initSchema(): Unit = {
    val classLoader = getClass.getClassLoader
    val initSchemaStream: Option[InputStream] = dbType match {
      case DERBY =>
        Option(classLoader.getResourceAsStream("sql/derby/statestore-schema-derby.sql"))
      case MYSQL =>
        Option(classLoader.getResourceAsStream("sql/mysql/statestore-schema-mysql.sql"))
      case CUSTOM => None
    }
    initSchemaStream.foreach { inputStream =>
      try {
        val ddlStatements = new BufferedReader(new InputStreamReader(inputStream)).lines()
          .collect(Collectors.joining("\n")).trim.split(";")
        withConnection() { connection =>
          Utils.tryLogNonFatalError {
            ddlStatements.foreach { ddlStatement =>
              execute(connection, ddlStatement)
              info(s"Execute init schema ddl: $ddlStatement successfully.")
            }
          }
        }
      } finally {
        inputStream.close()
      }
    }
  }

  override def close(): Unit = {
    hikariDataSource.close()
  }

  override def insertMetadata(metadata: Metadata): Unit = {
    val query =
      s"""
         |INSERT INTO $METADATA_TABLE(
         |identifier,
         |real_user,
         |user_name,
         |ip_address,
         |kyuubi_instance,
         |state,
         |resource,
         |class_name,
         |request_name,
         |request_conf,
         |request_args,
         |create_time,
         |engine_type
         |)
         |VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         |""".stripMargin

    withConnection() { connection =>
      execute(
        connection,
        query,
        metadata.identifier,
        metadata.realUser,
        metadata.username,
        metadata.ipAddress,
        metadata.kyuubiInstance,
        metadata.state,
        metadata.resource,
        metadata.className,
        metadata.requestName,
        valueAsString(metadata.requestConf),
        valueAsString(metadata.requestArgs),
        metadata.createTime,
        Option(metadata.engineType).map(_.toUpperCase(Locale.ROOT)).orNull)
    }
  }

  override def getMetadata(identifier: String, stateOnly: Boolean): Metadata = {
    val query =
      if (stateOnly) {
        s"SELECT $METADATA_STATE_ONLY_COLUMNS FROM $METADATA_TABLE WHERE identifier = ?"
      } else {
        s"SELECT $METADATA_ALL_COLUMNS FROM $METADATA_TABLE WHERE identifier = ?"
      }

    withConnection() { connection =>
      withResultSet(connection, query, identifier) { rs =>
        buildMetadata(rs, stateOnly).headOption.orNull
      }
    }
  }

  override def getMetadataList(
      engineType: String,
      userName: String,
      state: String,
      kyuubiInstance: String,
      from: Int,
      size: Int,
      stateOnly: Boolean): Seq[Metadata] = {
    val queryBuilder = new StringBuilder
    val params = ListBuffer[Any]()
    if (stateOnly) {
      queryBuilder.append(s"SELECT $METADATA_STATE_ONLY_COLUMNS FROM $METADATA_TABLE")
    } else {
      queryBuilder.append(s"SELECT $METADATA_ALL_COLUMNS FROM $METADATA_TABLE")
    }
    val whereConditions = ListBuffer[String]()
    Option(engineType).filter(_.nonEmpty).foreach { _ =>
      whereConditions += " UPPER(engine_type) = ? "
      params += engineType.toUpperCase(Locale.ROOT)
    }
    Option(userName).filter(_.nonEmpty).foreach { _ =>
      whereConditions += " user_name = ? "
      params += userName
    }
    Option(state).filter(_.nonEmpty).foreach { _ =>
      whereConditions += " STATE = ? "
      params += state
    }
    Option(kyuubiInstance).filter(_.nonEmpty).foreach { _ =>
      whereConditions += " KYUUBI_INSTANCE = ? "
      params += kyuubiInstance
    }
    if (whereConditions.nonEmpty) {
      queryBuilder.append(whereConditions.mkString(" WHERE ", " AND ", " "))
    }
    queryBuilder.append(" ORDER BY KEY_ID ")
    val query = databaseAdaptor.addLimitAndOffsetToQuery(queryBuilder.toString(), size, from)
    withConnection() { connection =>
      withResultSet(connection, query, params: _*) { rs =>
        buildMetadata(rs, stateOnly)
      }
    }
  }

  override def updateMetadata(metadata: Metadata): Unit = {
    val queryBuilder = new StringBuilder
    val params = ListBuffer[Any]()

    queryBuilder.append(s"UPDATE $METADATA_TABLE")
    val setClauses = ListBuffer[String]()
    Option(metadata.state).foreach { _ =>
      setClauses += " state = ? "
      params += metadata.state
    }
    if (metadata.endTime > 0) {
      setClauses += " end_time = ? "
      params += metadata.endTime
    }
    Option(metadata.engineId).foreach { _ =>
      setClauses += " engine_id = ? "
      params += metadata.engineId
    }
    Option(metadata.engineName).foreach { _ =>
      setClauses += " engine_name = ? "
      params += metadata.engineName
    }
    Option(metadata.engineUrl).foreach { _ =>
      setClauses += " engine_url = ? "
      params += metadata.engineUrl
    }
    Option(metadata.engineState).foreach { _ =>
      setClauses += " engine_state = ? "
      params += metadata.engineState
    }
    metadata.engineError.foreach { error =>
      setClauses += " engine_error = ? "
      params += error
    }
    if (setClauses.nonEmpty) {
      queryBuilder.append(setClauses.mkString(" SET ", " , ", " "))
    }
    queryBuilder.append(" WHERE identifier = ? ")
    params += metadata.identifier

    withConnection() { connection =>
      execute(connection, queryBuilder.toString(), params: _*)

    }
  }

  override def cleanupMetadataByIdentifier(identifier: String): Unit = {
    val query = s"DELETE FROM $METADATA_TABLE WHERE identifier = ?"
    withConnection() { connection =>
      execute(connection, query, identifier)
    }
  }

  override def cleanupMetadataByAge(maxAge: Long): Unit = {
    val minEndTime = System.currentTimeMillis() - maxAge
    val query = s"DELETE FROM $METADATA_TABLE WHERE end_time IS NOT NULL AND end_time < ?"
    withConnection() { connection =>
      execute(connection, query, minEndTime)
    }
  }

  private def buildMetadata(resultSet: ResultSet, stateOnly: Boolean): Seq[Metadata] = {
    try {
      val metadataList = ListBuffer[Metadata]()
      while (resultSet.next()) {
        val identifier = resultSet.getString("identifier")
        val realUser = resultSet.getString("real_user")
        val userName = resultSet.getString("user_name")
        val ipAddress = resultSet.getString("ip_address")
        val kyuubiInstance = resultSet.getString("kyuubi_instance")
        val state = resultSet.getString("state")
        val requestName = resultSet.getString("request_name")
        val createTime = resultSet.getLong("create_time")
        val engineType = resultSet.getString("engine_type")
        val engineId = resultSet.getString("engine_id")
        val engineName = resultSet.getString("engine_name")
        val engineUrl = resultSet.getString("engine_url")
        val engineState = resultSet.getString("engine_state")
        val engineError = Option(resultSet.getString("engine_error"))
        val endTime = resultSet.getLong("end_time")

        var resource: String = null
        var className: String = null
        var requestConf: Map[String, String] = Map.empty
        var requestArgs: Seq[String] = Seq.empty

        if (!stateOnly) {
          resource = resultSet.getString("resource")
          className = resultSet.getString("class_name")
          requestConf = string2Map(resultSet.getString("request_conf"))
          requestArgs = string2Seq(resultSet.getString("request_args"))
        }
        val metadata = Metadata(
          identifier = identifier,
          realUser = realUser,
          username = userName,
          ipAddress = ipAddress,
          kyuubiInstance = kyuubiInstance,
          state = state,
          resource = resource,
          className = className,
          requestName = requestName,
          requestConf = requestConf,
          requestArgs = requestArgs,
          createTime = createTime,
          engineType = engineType,
          engineId = engineId,
          engineName = engineName,
          engineUrl = engineUrl,
          engineState = engineState,
          engineError = engineError,
          endTime = endTime)
        metadataList += metadata
      }
      metadataList
    } finally {
      Utils.tryLogNonFatalError(resultSet.close())
    }
  }

  private def execute(conn: Connection, sql: String, params: Any*): Unit = {
    debug(s"executing sql $sql")
    var statement: PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParams(statement, params: _*)
      statement.execute()
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    } finally {
      if (statement != null) {
        Utils.tryLogNonFatalError(statement.close())
      }
    }
  }

  def withResultSet[T](conn: Connection, sql: String, params: Any*)(f: ResultSet => T): T = {
    debug(s"executing sql $sql with result set")
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParams(statement, params: _*)
      resultSet = statement.executeQuery()
      f(resultSet)
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    } finally {
      if (resultSet != null) {
        Utils.tryLogNonFatalError(resultSet.close())
      }
      if (statement != null) {
        Utils.tryLogNonFatalError(statement.close())
      }
    }
  }

  private def setStatementParams(statement: PreparedStatement, params: Any*): Unit = {
    params.zipWithIndex.foreach { case (param, index) =>
      param match {
        case null => statement.setObject(index + 1, null)
        case s: String => statement.setString(index + 1, s)
        case i: Int => statement.setInt(index + 1, i)
        case l: Long => statement.setLong(index + 1, l)
        case d: Double => statement.setDouble(index + 1, d)
        case f: Float => statement.setFloat(index + 1, f)
        case _ => throw new KyuubiException(s"Unsupported param type ${param.getClass.getName}")
      }
    }
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
  private val METADATA_TABLE = "metadata"
  private val METADATA_STATE_ONLY_COLUMNS = Seq(
    "identifier",
    "real_user",
    "user_name",
    "ip_address",
    "kyuubi_instance",
    "state",
    "request_name",
    "create_time",
    "engine_type",
    "engine_id",
    "engine_name",
    "engine_url",
    "engine_state",
    "engine_error",
    "end_time").mkString(",")
  private val METADATA_ALL_COLUMNS = Seq(
    "identifier",
    "real_user",
    "user_name",
    "ip_address",
    "kyuubi_instance",
    "state",
    "resource",
    "class_name",
    "request_name",
    "request_conf",
    "request_args",
    "create_time",
    "engine_type",
    "engine_id",
    "engine_name",
    "engine_url",
    "engine_state",
    "engine_error",
    "end_time").mkString(",")
}
