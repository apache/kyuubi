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
import org.apache.kyuubi.server.metadata.api.{KubernetesEngineInfo, Metadata, MetadataFilter}
import org.apache.kyuubi.server.metadata.jdbc.DatabaseType._
import org.apache.kyuubi.server.metadata.jdbc.JDBCMetadataStoreConf._
import org.apache.kyuubi.session.SessionType
import org.apache.kyuubi.util.JdbcUtils
import org.apache.kyuubi.util.reflect.ReflectUtils

class JDBCMetadataStore(conf: KyuubiConf) extends MetadataStore with Logging {
  import JDBCMetadataStore._

  private val dbType = DatabaseType.withName(conf.get(METADATA_STORE_JDBC_DATABASE_TYPE))
  private val driverClassOpt = conf.get(METADATA_STORE_JDBC_DRIVER)
  private lazy val mysqlDriverClass =
    if (ReflectUtils.isClassLoadable("com.mysql.cj.jdbc.Driver")) {
      "com.mysql.cj.jdbc.Driver"
    } else {
      "com.mysql.jdbc.Driver"
    }
  private val driverClass = dbType match {
    case SQLITE => driverClassOpt.getOrElse("org.sqlite.JDBC")
    case MYSQL => driverClassOpt.getOrElse(mysqlDriverClass)
    case POSTGRESQL => driverClassOpt.getOrElse("org.postgresql.Driver")
    case CUSTOM => driverClassOpt.getOrElse(
        throw new IllegalArgumentException("No jdbc driver defined"))
  }

  private val dialect = dbType match {
    case SQLITE => new SQLiteDatabaseDialect
    case MYSQL => new MySQLDatabaseDialect
    case POSTGRESQL => new PostgreSQLDatabaseDialect
    case CUSTOM => new GenericDatabaseDialect
  }

  private val priorityEnabled = conf.get(METADATA_STORE_JDBC_PRIORITY_ENABLED)

  private val datasourceProperties =
    JDBCMetadataStoreConf.getMetadataStoreJDBCDataSourceProperties(conf)
  private val hikariConfig = new HikariConfig(datasourceProperties)
  hikariConfig.setDriverClassName(driverClass)
  hikariConfig.setJdbcUrl(getMetadataStoreJdbcUrl(conf))
  hikariConfig.setUsername(conf.get(METADATA_STORE_JDBC_USER))
  hikariConfig.setPassword(conf.get(METADATA_STORE_JDBC_PASSWORD))
  hikariConfig.setPoolName("jdbc-metadata-store-pool")

  @VisibleForTesting
  implicit private[kyuubi] val hikariDataSource: HikariDataSource =
    new HikariDataSource(hikariConfig)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  if (conf.get(METADATA_STORE_JDBC_DATABASE_SCHEMA_INIT)) {
    initSchema()
  }

  private def initSchema(): Unit = {
    getInitSchema(dbType).foreach { schema =>
      val ddlStatements = schema.trim.split(";").map(_.trim)
      JdbcUtils.withConnection { connection =>
        Utils.tryLogNonFatalError {
          ddlStatements.foreach { ddlStatement =>
            execute(connection, ddlStatement)
            info(s"""Execute init schema ddl successfully.
                    |$ddlStatement
                    |""".stripMargin)
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
    }
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
         |cluster_manager,
         |priority
         |)
         |VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         |""".stripMargin

    JdbcUtils.withConnection { connection =>
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
        metadata.clusterManager.orNull,
        metadata.priority)
    }
  }

  override def pickMetadata(kyuubiInstance: String): Option[Metadata] = synchronized {
    JdbcUtils.executeQueryWithRowMapper(
      s"""SELECT identifier FROM $METADATA_TABLE
         |WHERE state=?
         |ORDER BY ${if (priorityEnabled) "priority DESC, " else ""}create_time ASC LIMIT 1
         |""".stripMargin) { stmt =>
      stmt.setString(1, OperationState.INITIALIZED.toString)
    } { resultSet =>
      resultSet.getString(1)
    }.headOption.filter { preSelectedBatchId =>
      JdbcUtils.executeUpdate(
        s"""UPDATE $METADATA_TABLE
           |SET kyuubi_instance=?, state=?
           |WHERE identifier=? AND state=?
           |""".stripMargin) { stmt =>
        stmt.setString(1, kyuubiInstance)
        stmt.setString(2, OperationState.PENDING.toString)
        stmt.setString(3, preSelectedBatchId)
        stmt.setString(4, OperationState.INITIALIZED.toString)
      } == 1
    }.map { pickedBatchId =>
      getMetadata(pickedBatchId)
    }
  }

  override def transformMetadataState(
      identifier: String,
      fromState: String,
      targetState: String): Boolean = {
    val query = s"UPDATE $METADATA_TABLE SET state = ? WHERE identifier = ? AND state = ?"
    JdbcUtils.withConnection { connection =>
      withUpdateCount(connection, query, fromState, identifier, targetState) { updateCount =>
        updateCount == 1
      }
    }
  }

  override def getMetadata(identifier: String): Metadata = {
    val query = s"SELECT $METADATA_COLUMNS FROM $METADATA_TABLE WHERE identifier = ?"

    JdbcUtils.withConnection { connection =>
      withResultSet(connection, query, identifier) { rs =>
        buildMetadata(rs).headOption.orNull
      }
    }
  }

  override def getMetadataList(
      filter: MetadataFilter,
      from: Int,
      size: Int,
      orderBy: Option[String] = Some("key_id"),
      direction: String = "ASC"): Seq[Metadata] = {
    val queryBuilder = new StringBuilder
    val params = ListBuffer[Any]()
    queryBuilder.append("SELECT ")
    queryBuilder.append(METADATA_COLUMNS)
    queryBuilder.append(s" FROM $METADATA_TABLE")
    queryBuilder.append(s" ${assembleWhereClause(filter, params)}")
    orderBy.foreach(o => queryBuilder.append(s" ORDER BY $o $direction "))
    queryBuilder.append(dialect.limitClause(size, from))
    val query = queryBuilder.toString
    JdbcUtils.withConnection { connection =>
      withResultSet(connection, query, params.toSeq: _*) { rs =>
        buildMetadata(rs)
      }
    }
  }

  override def countMetadata(filter: MetadataFilter): Int = {
    val queryBuilder = new StringBuilder
    val params = ListBuffer[Any]()
    queryBuilder.append(s"SELECT COUNT(1) FROM $METADATA_TABLE")
    queryBuilder.append(s" ${assembleWhereClause(filter, params)}")
    val query = queryBuilder.toString
    JdbcUtils.executeQueryWithRowMapper(query) { stmt =>
      setStatementParams(stmt, params)
    } { resultSet =>
      resultSet.getInt(1)
    }.head
  }

  private def assembleWhereClause(
      filter: MetadataFilter,
      params: ListBuffer[Any]): String = {
    val whereConditions = ListBuffer[String]("1 = 1")
    Option(filter.sessionType).foreach { sessionType =>
      whereConditions += "session_type = ?"
      params += sessionType.toString
    }
    Option(filter.engineType).filter(_.nonEmpty).foreach { engineType =>
      whereConditions += "UPPER(engine_type) = ?"
      params += engineType.toUpperCase(Locale.ROOT)
    }
    Option(filter.username).filter(_.nonEmpty).foreach { username =>
      whereConditions += "user_name = ?"
      params += username
    }
    Option(filter.state).filter(_.nonEmpty).foreach { state =>
      whereConditions += "state = ?"
      params += state.toUpperCase(Locale.ROOT)
    }
    Option(filter.requestName).filter(_.nonEmpty).foreach { requestName =>
      whereConditions += "request_name = ?"
      params += requestName
    }
    Option(filter.kyuubiInstance).filter(_.nonEmpty).foreach { kyuubiInstance =>
      whereConditions += "kyuubi_instance = ?"
      params += kyuubiInstance
    }
    if (filter.createTime > 0) {
      whereConditions += "create_time >= ?"
      params += filter.createTime
    }
    if (filter.endTime > 0) {
      whereConditions += "end_time > 0"
      whereConditions += "end_time <= ?"
      params += filter.endTime
    }
    if (filter.peerInstanceClosed) {
      whereConditions += "peer_instance_closed = ?"
      params += filter.peerInstanceClosed
    }
    whereConditions.mkString("WHERE ", " AND ", "")
  }

  override def updateMetadata(metadata: Metadata): Unit = {
    val queryBuilder = new StringBuilder
    val params = ListBuffer[Any]()

    queryBuilder.append(s"UPDATE $METADATA_TABLE")
    val setClauses = ListBuffer[String]()
    Option(metadata.kyuubiInstance).foreach { _ =>
      setClauses += "kyuubi_instance = ?"
      params += metadata.kyuubiInstance
    }
    Option(metadata.state).foreach { _ =>
      setClauses += "state = ?"
      params += metadata.state
    }
    Option(metadata.requestConf).filter(_.nonEmpty).foreach { _ =>
      setClauses += "request_conf =?"
      params += valueAsString(metadata.requestConf)
    }
    metadata.clusterManager.foreach { cm =>
      setClauses += "cluster_manager = ?"
      params += cm
    }
    if (metadata.endTime > 0) {
      setClauses += "end_time = ?"
      params += metadata.endTime
    }
    if (metadata.engineOpenTime > 0) {
      setClauses += "engine_open_time = ?"
      params += metadata.engineOpenTime
    }
    Option(metadata.engineId).foreach { _ =>
      setClauses += "engine_id = ?"
      params += metadata.engineId
    }
    Option(metadata.engineName).foreach { _ =>
      setClauses += "engine_name = ?"
      params += metadata.engineName
    }
    Option(metadata.engineUrl).foreach { _ =>
      setClauses += "engine_url = ?"
      params += metadata.engineUrl
    }
    Option(metadata.engineState).foreach { _ =>
      setClauses += "engine_state = ?"
      params += metadata.engineState
    }
    metadata.engineError.foreach { error =>
      setClauses += "engine_error = ?"
      params += error
    }
    if (metadata.peerInstanceClosed) {
      setClauses += "peer_instance_closed = ?"
      params += metadata.peerInstanceClosed
    }
    if (setClauses.nonEmpty) {
      queryBuilder.append(setClauses.mkString(" SET ", ", ", ""))
    }
    queryBuilder.append(" WHERE identifier = ?")
    params += metadata.identifier

    val query = queryBuilder.toString()
    JdbcUtils.withConnection { connection =>
      withUpdateCount(connection, query, params.toSeq: _*) { updateCount =>
        if (updateCount == 0) {
          throw new KyuubiException(
            s"Error updating metadata for ${metadata.identifier} by SQL: $query, " +
              s"with params: ${params.mkString(", ")}")
        }
      }
    }
  }

  override def cleanupMetadataByIdentifier(identifier: String): Unit = {
    val query = s"DELETE FROM $METADATA_TABLE WHERE identifier = ?"
    JdbcUtils.withConnection { connection =>
      execute(connection, query, identifier)
    }
  }

  override def cleanupMetadataByAge(maxAge: Long): Unit = {
    val minEndTime = System.currentTimeMillis() - maxAge
    val query =
      s"DELETE FROM $METADATA_TABLE WHERE end_time > 0 AND end_time < ? AND create_time < ?"
    JdbcUtils.withConnection { connection =>
      withUpdateCount(connection, query, minEndTime, minEndTime) { count =>
        info(s"Cleaned up $count records older than $maxAge ms from $METADATA_TABLE.")
      }
    }
  }

  override def upsertKubernetesEngineInfo(engineInfo: KubernetesEngineInfo): Unit = {
    val query = dialect.insertOrReplace(
      KUBERNETES_ENGINE_INFO_TABLE,
      KUBERNETES_ENGINE_INFO_COLUMNS,
      KUBERNETES_ENGINE_INFO_KEY_COLUMN,
      engineInfo.identifier)
    JdbcUtils.withConnection { connection =>
      execute(
        connection,
        query,
        engineInfo.identifier,
        engineInfo.context.orNull,
        engineInfo.namespace.orNull,
        engineInfo.podName,
        engineInfo.podState,
        engineInfo.containerState,
        engineInfo.engineId,
        engineInfo.engineName,
        engineInfo.engineState,
        engineInfo.engineError.orNull,
        System.currentTimeMillis())
    }
  }

  override def getKubernetesMetaEngineInfo(identifier: String): KubernetesEngineInfo = {
    val query =
      s"SELECT $KUBERNETES_ENGINE_INFO_COLUMNS_STR FROM" +
        s" $KUBERNETES_ENGINE_INFO_TABLE WHERE identifier = ?"
    JdbcUtils.withConnection { connection =>
      withResultSet(connection, query, identifier) { rs =>
        buildKubernetesMetadata(rs).headOption.orNull
      }
    }
  }

  override def cleanupKubernetesEngineInfoByIdentifier(identifier: String): Unit = {
    val query = s"DELETE FROM $KUBERNETES_ENGINE_INFO_TABLE WHERE identifier = ?"
    JdbcUtils.withConnection { connection =>
      execute(connection, query, identifier)
    }
  }

  override def cleanupKubernetesEngineInfoByAge(maxAge: Long): Unit = {
    val minUpdateTime = System.currentTimeMillis() - maxAge
    val query = s"DELETE FROM $KUBERNETES_ENGINE_INFO_TABLE WHERE update_time < ?"
    JdbcUtils.withConnection { connection =>
      withUpdateCount(connection, query, minUpdateTime) { count =>
        info(s"Cleaned up $count records older than $maxAge ms from $KUBERNETES_ENGINE_INFO_TABLE.")
      }
    }
  }

  private def buildMetadata(resultSet: ResultSet): Seq[Metadata] = {
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
        val resource = resultSet.getString("resource")
        val className = resultSet.getString("class_name")
        val requestName = resultSet.getString("request_name")
        val requestConf = string2Map(resultSet.getString("request_conf"))
        val requestArgs = string2Seq(resultSet.getString("request_args"))
        val createTime = resultSet.getLong("create_time")
        val engineType = resultSet.getString("engine_type")
        val clusterManager = Option(resultSet.getString("cluster_manager"))
        val engineOpenTime = resultSet.getLong("engine_open_time")
        val engineId = resultSet.getString("engine_id")
        val engineName = resultSet.getString("engine_name")
        val engineUrl = resultSet.getString("engine_url")
        val engineState = resultSet.getString("engine_state")
        val engineError = Option(resultSet.getString("engine_error"))
        val endTime = resultSet.getLong("end_time")
        val peerInstanceClosed = resultSet.getBoolean("peer_instance_closed")

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
          engineOpenTime = engineOpenTime,
          engineId = engineId,
          engineName = engineName,
          engineUrl = engineUrl,
          engineState = engineState,
          engineError = engineError,
          endTime = endTime,
          peerInstanceClosed = peerInstanceClosed)
        metadataList += metadata
      }
      metadataList.toSeq
    } finally {
      Utils.tryLogNonFatalError(resultSet.close())
    }
  }

  private def buildKubernetesMetadata(resultSet: ResultSet): Seq[KubernetesEngineInfo] = {
    try {
      val metadataList = ListBuffer[KubernetesEngineInfo]()
      while (resultSet.next()) {
        val identifier = resultSet.getString("identifier")
        val context = Option(resultSet.getString("context"))
        val namespace = Option(resultSet.getString("namespace"))
        val pod = resultSet.getString("pod_name")
        val podState = resultSet.getString("pod_state")
        val containerState = resultSet.getString("container_state")
        val appId = resultSet.getString("engine_id")
        val appName = resultSet.getString("engine_name")
        val appState = resultSet.getString("engine_state")
        val appError = Option(resultSet.getString("engine_error"))
        val updateTime = resultSet.getLong("update_time")

        val metadata = KubernetesEngineInfo(
          identifier = identifier,
          context = context,
          namespace = namespace,
          podName = pod,
          podState = podState,
          containerState = containerState,
          engineId = appId,
          engineName = appName,
          engineState = appState,
          engineError = appError,
          updateTime = updateTime)
        metadataList += metadata
      }
      metadataList.toSeq
    } finally {
      Utils.tryLogNonFatalError(resultSet.close())
    }
  }

  private def execute(conn: Connection, sql: String, params: Any*): Unit = {
    debug(s"execute sql: $sql, with params: ${params.mkString(", ")}")
    var statement: PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParams(statement, params: _*)
      statement.execute()
    } catch {
      case e: SQLException =>
        throw new KyuubiException(
          s"Error executing sql: $sql, with params: ${params.mkString(", ")}. ${e.getMessage}",
          e)
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
    debug(s"executeQuery sql: $sql, with params: ${params.mkString(", ")}")
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParams(statement, params: _*)
      resultSet = statement.executeQuery()
      f(resultSet)
    } catch {
      case e: SQLException =>
        throw new KyuubiException(
          s"Error executing sql: $sql, with params: ${params.mkString(", ")}. ${e.getMessage}",
          e)
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
    debug(s"executeUpdate sql: $sql, with params: ${params.mkString(", ")}")
    var statement: PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql)
      setStatementParams(statement, params: _*)
      f(statement.executeUpdate())
    } catch {
      case e: SQLException =>
        throw new KyuubiException(
          s"Error executing sql: $sql, with params: ${params.mkString(", ")}. ${e.getMessage}",
          e)
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
  private val METADATA_COLUMNS = Seq(
    "identifier",
    "session_type",
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
    "cluster_manager",
    "engine_open_time",
    "engine_id",
    "engine_name",
    "engine_url",
    "engine_state",
    "engine_error",
    "end_time",
    "peer_instance_closed").mkString(",")
  private val KUBERNETES_ENGINE_INFO_TABLE = "k8s_engine_info"
  private val KUBERNETES_ENGINE_INFO_KEY_COLUMN = "identifier"
  private val KUBERNETES_ENGINE_INFO_COLUMNS = Seq(
    KUBERNETES_ENGINE_INFO_KEY_COLUMN,
    "context",
    "namespace",
    "pod_name",
    "pod_state",
    "container_state",
    "engine_id",
    "engine_name",
    "engine_state",
    "engine_error",
    "update_time")
  private val KUBERNETES_ENGINE_INFO_COLUMNS_STR = KUBERNETES_ENGINE_INFO_COLUMNS.mkString(",")
}
