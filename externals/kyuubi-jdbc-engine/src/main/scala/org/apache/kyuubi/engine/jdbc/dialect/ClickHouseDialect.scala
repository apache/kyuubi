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

import java.sql.{Connection, Statement}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.jdbc.clickhouse.{ClickHouseRowSetHelper, ClickHouseSchemaHelper}
import org.apache.kyuubi.engine.jdbc.schema.{RowSetHelper, SchemaHelper}
import org.apache.kyuubi.operation.Operation
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class ClickHouseDialect extends JdbcDialect {

  override def createStatement(connection: Connection, fetchSize: Int): Statement = {
    val statement = super.createStatement(connection, fetchSize)
    statement.setFetchSize(Integer.MIN_VALUE)
    statement
  }

  override def getTypeInfoOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getCatalogsOperation(): String = "SELECT CATALOG_NAME " +
    "FROM INFORMATION_SCHEMA.INFORMATION_SCHEMA_CATALOG_NAME"

  override def getSchemasOperation(
      catalog: String,
      schema: String): String = {
    val query = new StringBuilder(
      s"""
         |SELECT CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER,
         |DEFAULT_CHARACTER_SET_CATALOG, DEFAULT_CHARACTER_SET_SCHEMA,
         |DEFAULT_CHARACTER_SET_NAME, SQL_PATH
         |FROM INFORMATION_SCHEMA.SCHEMATA
         |""".stripMargin)

    val filters = ArrayBuffer[String]()
    if (StringUtils.isNotBlank(catalog)) {
      filters += s"catalog_name LIKE '$catalog'"
    }

    if (StringUtils.isNotBlank(schema)) {
      filters += s"schema_name LIKE '$schema'"
    }

    if (filters.nonEmpty) {
      query.append(" WHERE ")
      query.append(filters.mkString(" AND "))
    }

    query.toString()
  }

  override def getTablesQuery(
                               catalog: String,
                               schema: String,
                               tableName: String,
                               tableTypes: util.List[String]): String = {
    val tTypes =
      if (tableTypes == null || tableTypes.isEmpty) {
        Set("BASE TABLE", "VIEW", "FOREIGN TABLE", "LOCAL TEMPORARY", "SYSTEM VIEW")
      } else {
        tableTypes.asScala.toSet
      }
    val query = new StringBuilder(
      s"""
         |SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
         |FROM INFORMATION_SCHEMA.TABLES
         |""".stripMargin)

    val filters = ArrayBuffer[String]()
    if (StringUtils.isNotBlank(catalog)) {
      filters += s"$TABLE_CATALOG = '$catalog'"
    }

    if (StringUtils.isNotBlank(schema)) {
      filters += s"$TABLE_SCHEMA LIKE '$schema'"
    }

    if (StringUtils.isNotBlank(tableName)) {
      filters += s"$TABLE_NAME LIKE '$tableName'"
    }

    if (tTypes.nonEmpty) {
      filters += s"(${tTypes.map { tableType => s"$TABLE_TYPE = '$tableType'" }
        .mkString(" OR ")})"
    }

    if (filters.nonEmpty) {
      query.append(" WHERE ")
      query.append(filters.mkString(" AND "))
    }

    query.toString()
  }

  override def getTableTypesOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }
  override def getColumnsQuery(
    session: Session,
    catalogName: String,
    schemaName: String,
    tableName: String,
    columnName: String): String = {
    val query = new StringBuilder(
      s"""
        |SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION,
        |COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
        |CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX,
        |NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_CATALOG, CHARACTER_SET_SCHEMA,
        |CHARACTER_SET_NAME, COLLATION_CATALOG, COLLATION_SCHEMA, COLLATION_NAME,
        |DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME, COLUMN_COMMENT, COLUMN_TYPE
        |FROM INFORMATION_SCHEMA.COLUMNS
        |""".stripMargin)

    val filters = ArrayBuffer[String]()
    if (StringUtils.isNotEmpty(catalogName)) {
      filters += s"$TABLE_CATALOG = '$catalogName'"
    }
    if (StringUtils.isNotEmpty(schemaName)) {
      filters += s"$TABLE_SCHEMA LIKE '$schemaName'"
    }
    if (StringUtils.isNotEmpty(tableName)) {
      filters += s"$TABLE_NAME LIKE '$tableName'"
    }
    if (StringUtils.isNotEmpty(columnName)) {
      filters += s"$COLUMN_NAME LIKE '$columnName'"
    }

    if (filters.nonEmpty) {
      query.append(" WHERE ")
      query.append(filters.mkString(" AND "))
    }

    query.toString()
  }

  override def getFunctionsOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getPrimaryKeysOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getCrossReferenceOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getRowSetHelper(): RowSetHelper = {
    new ClickHouseRowSetHelper
  }

  override def getSchemaHelper(): SchemaHelper = {
    new ClickHouseSchemaHelper
  }

  override def name(): String = {
    "clickhouse"
  }
}
