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

package org.apache.kyuubi.server.metadata.jdbc

import java.io.{BufferedReader, InputStreamReader}
import java.nio.file.{Files, FileSystems, Paths}
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.Locale
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.annotations.VisibleForTesting
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.server.metadata.MetadataStore
import org.apache.kyuubi.server.metadata.api.{Metadata, MetadataFilter}
import org.apache.kyuubi.server.metadata.jdbc.DatabaseType._
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._
import org.apache.kyuubi.session.SessionType

class JDBCMetadataStore(conf: KyuubiConf) extends MetadataStore with Logging {
  import JDBCMetadataStore._

  private val dbType = DatabaseType.withName(conf.get(METADATA_STORE_JDBC_DATABASE_TYPE))
  private val driverClassOpt = conf.get(METADATA_STORE_JDBC_DRIVER)
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

  private val datasourceProperties =
    JDBCMetadataStoreConf.getMetadataStoreJDBCDataSourceProperties(conf)
  private val hikariConfig = new HikariConfig(datasourceProperties)
  hikariConfig.setDriverClassName(driverClass)
  hikariConfig.setJdbcUrl(conf.get(METADATA_STORE_JDBC_URL))
  hikariConfig.setUsername(conf.get(METADATA_STORE_JDBC_USER))
  hikariConfig.setPassword(conf.get(METADATA_STORE_JDBC_PASSWORD))
  hikariConfig.setPoolName("jdbc-metadata-store-pool")

  @VisibleForTesting
  private[kyuubi] val hikariDataSource = new HikariDataSource(hikariConfig)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private val terminalStates =
    OperationState.terminalStates.map(x => s"'${x.toString}'").mkString(", ")

  if (conf.get(METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT)) {
    initSchema()
  }

  private def initSchema(): Unit = {
    getInitSchema(dbType).foreach { schema =>
      val ddlStatements = schema.trim.split(";")
      withConnection() { connection =>
        Utils.tryLogNonFatalError {
          ddlStatements.foreach { ddlStatement =>
            execute(connection, ddlStatement)
            info(s"Execute init schema ddl: $ddlStatement successfully.")
          }
        }
      }
    }
  }

  // Visible for testing.
  private[jdbc] def getInitSchema(dbType: DatabaseType): Option[String] = {
    val classLoader = Utils.getContextOrKyuubiClassLoader
    val schemaPackage = s"sql/${dbType.toString.toLowerCase}"

    Option(classLoader.getResource(schemaPackage)).map(_.toURI).flatMap { uri =>
      val pathNames = if (uri.getScheme == "jar") {
        val fs = FileSystems.newFileSystem(uri, Map.empty[String, AnyRef].asJava)
        try {
          Files.walk(fs.getPath(schemaPackage), 1).iterator().asScala.map(
            _.getFileName.toString).filter { name =>
            SCHEMA_URL_PATTERN.findFirstMatchIn(name).isDefined
          }.toArray
        } finally {
          fs.close()
        }
      } else {
        Paths.get(uri).toFile.listFiles((_, name) => {
          SCHEMA_URL_PATTERN.findFirstMatchIn(name).isDefined
        }).map(_.getName)
      }
      getLatestSchemaUrl(pathNames).map(name => s"$schemaPackage/$name").map { schemaUrl =>
        val inputStream = classLoader.getResourceAsStream(schemaUrl)
        try {
          new BufferedReader(new InputStreamReader(inputStream)).lines()
            .collect(Collectors.joining("\n"))
        } finally {
          inputStream.close()
        }
      }
    }.headOption
  }

  def getSchemaVersion(schemaUrl: String): (Int, Int, Int) =
    SCHEMA_URL_PATTERN.findFirstMatchIn(schemaUrl) match {
      case Some(m) => (m.group(1).toInt, m.group(2).toInt, m.group(3).toInt)
      case _ => throw new KyuubiException(s"Invalid schema url: $schemaUrl")
    }

  def getLatestSchemaUrl(schemaUrls: Seq[String]): Option[String] = {
    schemaUrls.sortWith { (u1, u2) =>
      val v1 = getSchemaVersion(u1)
      val v2 = getSchemaVersion(u2)
      v1._1 > v2._1 ||
      (v1._1 == v2._1 && v1._2 > v2._2) ||
      (v1._1 == v2._1 && v1._2 == v2._2 && v1._3 > v2._3)
    }.headOption
  }

  override def close(): Unit = {
    hikariDataSource.close()
  }

  override def insertMetadata(metadata: Metadata): Unit = {
    val query =
      s"""
         |INSERT INTO $METADATA_TABLE(
         |identifier,
         |session_type,
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
         |engine_type,
         |cluster_manager
         |)
         |VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         |""".stripMargin

    withConnection() { connection =>
      execute(
        connection,
        query,
        metadata.identifier,
        metadata.sessionType.toString,
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
        Option(metadata.engineType).map(_.toUpperCase(Locale.ROOT)).orNull,
        metadata.clusterManager.orNull)
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
      filter: MetadataFilter,
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
    Option(filter.sessionType).foreach { sessionType =>
      whereConditions += " session_type = ?"
      params += sessionType.toString
    }
    Option(filter.engineType).filter(_.nonEmpty).foreach { engineType =>
      whereConditions += " UPPER(engine_type) = ? "
      params += engineType.toUpperCase(Locale.ROOT)
    }
    Option(filter.username).filter(_.nonEmpty).foreach { username =>
      whereConditions += " user_name = ? "
      params += username
    }
    Option(filter.state).filter(_.nonEmpty).foreach { state =>
      whereConditions += " state = ? "
      params += state.toUpperCase(Locale.ROOT)
    }
    Option(filter.kyuubiInstance).filter(_.nonEmpty).foreach { kyuubiInstance =>
      whereConditions += " kyuubi_instance = ? "
      params += kyuubiInstance
    }
    if (filter.createTime > 0) {
      whereConditions += " create_time >= ? "
      params += filter.createTime
    }
    if (filter.endTime > 0) {
      whereConditions += " end_time > 0 "
      whereConditions += " end_time <= ? "
      params += filter.endTime
    }
    if (filter.peerInstanceClosed) {
      whereConditions += " peer_instance_closed = ? "
      params += filter.peerInstanceClosed
    }
    if (whereConditions.nonEmpty) {
      queryBuilder.append(whereConditions.mkString(" WHERE ", " AND ", " "))
    }
    queryBuilder.append(" ORDER BY key_id ")
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
    if (metadata.engineOpenTime > 0) {
      setClauses += " engine_open_time = ? "
      params += metadata.engineOpenTime
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
    if (metadata.peerInstanceClosed) {
      setClauses += " peer_instance_closed = ? "
      params += metadata.peerInstanceClosed
    }
    if (setClauses.nonEmpty) {
      queryBuilder.append(setClauses.mkString(" SET ", " , ", " "))
    }
    queryBuilder.append(" WHERE identifier = ? ")
    params += metadata.identifier

    val query = queryBuilder.toString()
    withConnection() { connection =>
      withUpdateCount(connection, query, params: _*) { updateCount =>
        if (updateCount == 0) {
          throw new KyuubiException(
            s"Error updating metadata for ${metadata.identifier} with $query")
        }
      }
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
    val query = s"DELETE FROM $METADATA_TABLE WHERE state IN ($terminalStates) AND end_time < ?"
    withConnection() { connection =>
      execute(connection, query, minEndTime)
    }
  }

  private def buildMetadata(resultSet: ResultSet, stateOnly: Boolean): Seq[Metadata] = {
    try {
      val metadataList = ListBuffer[Metadata]()
      while (resultSet.next()) {
        val identifier = resultSet.getString("identifier")
        val sessionType = SessionType.withName(resultSet.getString("session_type"))
        val realUser = resultSet.getString("real_user")
        val userName = resultSet.getString("user_name")
        val ipAddress = resultSet.getString("ip_address")
        val kyuubiInstance = resultSet.getString("kyuubi_instance")
        val state = resultSet.getString("state")
        val requestName = resultSet.getString("request_name")
        val createTime = resultSet.getLong("create_time")
        val engineType = resultSet.getString("engine_type")
        val clusterManager = Option(resultSet.getString("cluster_manager"))
        val engineId = resultSet.getString("engine_id")
        val engineName = resultSet.getString("engine_name")
        val engineUrl = resultSet.getString("engine_url")
        val engineState = resultSet.getString("engine_state")
        val engineError = Option(resultSet.getString("engine_error"))
        val endTime = resultSet.getLong("end_time")
        val peerInstanceClosed = resultSet.getBoolean("peer_instance_closed")

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
          sessionType = sessionType,
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
          clusterManager = clusterManager,
          engineId = engineId,
          engineName = engineName,
          engineUrl = engineUrl,
          engineState = engineState,
          engineError = engineError,
          endTime = endTime,
          peerInstanceClosed = peerInstanceClosed)
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
        throw new KyuubiException(s"Error executing $sql:" + e.getMessage, e)
    } finally {
      if (statement != null) {
        Utils.tryLogNonFatalError(statement.close())
      }
    }
  }

  private def withResultSet[T](
      conn: Connection,
      sql: String,
      params: Any*)(f: ResultSet => T): T = {
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

  private def withUpdateCount[T](
      conn: Connection,
      sql: String,
      params: Any*)(f: Int => T): T = {
    debug(s"executing sql $sql with update count")
    var statement: PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParams(statement, params: _*)
      f(statement.executeUpdate())
    } catch {
      case e: SQLException =>
        throw new KyuubiException(e.getMessage, e)
    } finally {
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
        case b: Boolean => statement.setBoolean(index + 1, b)
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
        Utils.tryLogNonFatalError(connection.close())
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

object JDBCMetadataStore {
  private val SCHEMA_URL_PATTERN = """^metadata-store-schema-(\d+)\.(\d+)\.(\d+)\.(.*)\.sql$""".r
  private val METADATA_TABLE = "metadata"
  private val METADATA_STATE_ONLY_COLUMNS = Seq(
    "identifier",
    "session_type",
    "real_user",
    "user_name",
    "ip_address",
    "kyuubi_instance",
    "state",
    "request_name",
    "create_time",
    "engine_type",
    "cluster_manager",
    "engine_id",
    "engine_name",
    "engine_url",
    "engine_state",
    "engine_error",
    "end_time",
    "peer_instance_closed").mkString(",")
  private val METADATA_ALL_COLUMNS = Seq(
    METADATA_STATE_ONLY_COLUMNS,
    "resource",
    "class_name",
    "request_conf",
    "request_args").mkString(",")
}
