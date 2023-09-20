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

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.jdbc.hive.{HiveRowSetHelper, HiveSchemaHelper}
import org.apache.kyuubi.engine.jdbc.operation.ExecuteStatement
import org.apache.kyuubi.engine.jdbc.schema.{RowSetHelper, SchemaHelper}
import org.apache.kyuubi.operation.Operation
import org.apache.kyuubi.session.Session

class HiveDialect extends JdbcDialect {

  override def createStatement(connection: Connection, fetchSize: Int): Statement = {
    val statement =
      connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    statement.setFetchSize(fetchSize)
    statement
  }

  override def getTypeInfoOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getCatalogsOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getSchemasOperation(session: Session): Operation = {
    val query = new StringBuilder(
      s"""
         |SHOW DATABASES
         |""".stripMargin).toString()
    val executeStatement = {
      new ExecuteStatement(session, query, false, 0L, false)
    }
    executeStatement
  }

  override def getTablesQuery(
      session: Session,
      catalog: String,
      schema: String,
      tableName: String,
      tableTypes: util.List[String]): Operation = {
    val query = new StringBuilder(
      s"""
         |SHOW TABLES
         |""".stripMargin)
    if (StringUtils.isNotBlank(schema) && !schema.equals("%")) {
      query.append("IN ").append(schema)
    }

    if (StringUtils.isNotBlank(tableName)) {
      query.append("'").append(tableName).append("'")
    }

    val executeStatement = {
      new ExecuteStatement(session, query.toString(), false, 0L, false)
    }
    executeStatement
  }

  override def getTableTypesOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getColumnsQuery(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = {
    val query = new StringBuilder(
      s"""
         |SHOW COLUMNS
         |""".stripMargin)

    if (StringUtils.isNotBlank(tableName)) {
      query.append(" IN ").append(tableName)
    }
    if (StringUtils.isNotBlank(schemaName)) {
      query.append(" IN ").append(schemaName)
    }
    val executeStatement = {
      new ExecuteStatement(session, query.toString(), false, 0L, false)
    }
    executeStatement
  }

  override def getFunctionsOperation(session: Session): Operation = {
    val query = new StringBuilder(
      s"""
         |SHOW FUNCTIONS
         |""".stripMargin).toString()
    val executeStatement = {
      new ExecuteStatement(session, query, false, 0L, false)
    }
    executeStatement
  }

  override def getPrimaryKeysOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getCrossReferenceOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getCurrentDatabaseOperation(session: Session): Operation = {
    val query = new StringBuilder(
      s"""
         |SELECT CURRENT_DATABASE()
         |""".stripMargin).toString()
    val executeStatement = {
      new ExecuteStatement(session, query, false, 0L, false)
    }
    executeStatement
  }

  override def getRowSetHelper(): RowSetHelper = {
    new HiveRowSetHelper
  }

  override def getSchemaHelper(): SchemaHelper = {
    new HiveSchemaHelper
  }

  override def name(): String = {
    "hive"
  }
}
