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
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.jdbc.doris.{DorisRowSetHelper, DorisSchemaHelper}
import org.apache.kyuubi.engine.jdbc.schema.{RowSetHelper, SchemaHelper}
import org.apache.kyuubi.operation.Operation
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class DorisDialect extends JdbcDialect {

  override def createStatement(connection: Connection, fetchSize: Int): Statement = {
    val statement =
      connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    statement.setFetchSize(Integer.MIN_VALUE)
    statement
  }

  override def getTypeInfoOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getCatalogsOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getSchemasOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getTablesQuery(
      catalog: String,
      schema: String,
      tableName: String,
      tableTypes: util.List[String]): String = {
    val tTypes =
      if (tableTypes == null || tableTypes.isEmpty) {
        Set("BASE TABLE", "SYSTEM VIEW", "VIEW")
      } else {
        tableTypes.asScala.toSet
      }
    val query = new StringBuilder(
      s"""
         |SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE,
         |TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH,
         |CREATE_TIME, UPDATE_TIME, TABLE_COLLATION, TABLE_COMMENT
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
      """
        |SELECT
        |`TABLE_CATALOG`,`TABLE_SCHEMA`,`TABLE_NAME`, `COLUMN_NAME`,`ORDINAL_POSITION`,
        |`COLUMN_DEFAULT`,`IS_NULLABLE`,`DATA_TYPE`,`CHARACTER_MAXIMUM_LENGTH`,
        |`CHARACTER_OCTET_LENGTH`,`NUMERIC_PRECISION`,`NUMERIC_SCALE`,`DATETIME_PRECISION`,
        |`CHARACTER_SET_NAME`,`COLLATION_NAME`,`COLUMN_TYPE`,`COLUMN_KEY`,`EXTRA`,`PRIVILEGES`,
        |`COLUMN_COMMENT`,`COLUMN_SIZE`,`DECIMAL_DIGITS`,`GENERATION_EXPRESSION`,`SRS_ID`
        |FROM information_schema.columns
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
    new DorisRowSetHelper
  }

  override def getSchemaHelper(): SchemaHelper = {
    new DorisSchemaHelper
  }

  override def name(): String = {
    "doris"
  }
}
