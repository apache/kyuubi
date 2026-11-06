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
package org.apache.kyuubi.engine.jdbc.dialect

import java.sql.{Connection, ResultSet, Statement}

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_CONNECTION_URL, ENGINE_JDBC_SHORT_NAME}
import org.apache.kyuubi.engine.jdbc.schema.{JdbcTRowSetGenerator, SchemaHelper}
import org.apache.kyuubi.engine.jdbc.util.SupportServiceLoader
import org.apache.kyuubi.util.reflect.ReflectUtils._

abstract class JdbcDialect extends SupportServiceLoader with Logging {

  def createStatement(connection: Connection, fetchSize: Int = 1000): Statement = {
    val statement =
      connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    statement.setFetchSize(fetchSize)
    statement
  }

  // --- Connection-level current catalog/schema hooks ---

  def getCatalog(conn: Connection): String = conn.getCatalog

  def setCatalog(conn: Connection, catalog: String): Unit = conn.setCatalog(catalog)

  def getCurrentSchema(conn: Connection): String = conn.getSchema

  def setSchema(conn: Connection, schema: String): Unit = conn.setSchema(schema)

  // --- Metadata hooks; default impl forwards to standard JDBC DatabaseMetaData ---

  def getTypeInfo(conn: Connection): ResultSet =
    conn.getMetaData.getTypeInfo

  def getCatalogs(conn: Connection): ResultSet =
    conn.getMetaData.getCatalogs

  def getSchemas(conn: Connection, catalog: String, schemaPattern: String): ResultSet =
    conn.getMetaData.getSchemas(catalog, schemaPattern)

  def getTableTypes(conn: Connection): ResultSet =
    conn.getMetaData.getTableTypes

  def getTables(
      conn: Connection,
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      types: Array[String]): ResultSet =
    conn.getMetaData.getTables(catalog, schemaPattern, tableNamePattern, types)

  def getColumns(
      conn: Connection,
      catalog: String,
      schemaPattern: String,
      tableNamePattern: String,
      columnNamePattern: String): ResultSet =
    conn.getMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern)

  def getFunctions(
      conn: Connection,
      catalog: String,
      schemaPattern: String,
      functionNamePattern: String): ResultSet =
    conn.getMetaData.getFunctions(catalog, schemaPattern, functionNamePattern)

  def getPrimaryKeys(
      conn: Connection,
      catalog: String,
      schema: String,
      table: String): ResultSet =
    conn.getMetaData.getPrimaryKeys(catalog, schema, table)

  def getCrossReference(
      conn: Connection,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): ResultSet =
    conn.getMetaData.getCrossReference(
      primaryCatalog,
      primarySchema,
      primaryTable,
      foreignCatalog,
      foreignSchema,
      foreignTable)

  def getTRowSetGenerator(): JdbcTRowSetGenerator

  def getSchemaHelper(): SchemaHelper

  def cancelStatement(jdbcStatement: Statement): Unit = {
    if (jdbcStatement != null) {
      jdbcStatement.cancel()
      jdbcStatement.close()
    }
  }

  def closeStatement(jdbcStatement: Statement): Unit = {
    if (jdbcStatement != null) {
      jdbcStatement.close()
    }
  }
}

object JdbcDialects extends Logging {

  def get(conf: KyuubiConf): JdbcDialect = {
    val shortName: String = conf.get(ENGINE_JDBC_SHORT_NAME).getOrElse {
      val url = conf.get(ENGINE_JDBC_CONNECTION_URL).get
      assert(url.length > 5 && url.substring(5).contains(":"))
      url.substring(5, url.indexOf(":", 5))
    }
    loadFromServiceLoader[JdbcDialect]()
      .filter(_.name().equalsIgnoreCase(shortName)).toList match {
      case Nil =>
        throw new KyuubiException(s"Don't find jdbc dialect implement for jdbc engine: $shortName.")
      case head :: Nil =>
        head
      case managers =>
        warn(s"Found multiple jdbc dialect implements for jdbc engine: $shortName")
        managers
          .filter(_.getClass.getCanonicalName.startsWith("org.apache.kyuubi"))
          .head
    }
  }
}
