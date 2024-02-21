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

import java.util

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.jdbc.impala.{ImpalaSchemaHelper, ImpalaTRowSetGenerator}
import org.apache.kyuubi.engine.jdbc.schema.{JdbcTRowSetGenerator, SchemaHelper}
import org.apache.kyuubi.session.Session

class ImpalaDialect extends JdbcDialect {

  override def getTablesQuery(
      catalog: String,
      schema: String,
      tableName: String,
      tableTypes: util.List[String]): String = {
    if (isPattern(schema)) {
      throw KyuubiSQLException.featureNotSupported("Pattern-like schema names not supported")
    }

    val query = new StringBuilder("show tables ")

    if (StringUtils.isNotEmpty(schema) && !isWildcardSetByKyuubi(schema)) {
      query.append(s"in $schema ")
    }

    if (StringUtils.isNotEmpty(tableName)) {
      query.append(s"like '${toImpalaRegex(tableName)}'")
    }

    query.toString()
  }

  override def getColumnsQuery(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): String = {
    if (isPattern(schemaName)) {
      throw KyuubiSQLException.featureNotSupported("Pattern-like schema names not supported")
    }

    if (isPattern(tableName)) {
      throw KyuubiSQLException.featureNotSupported("Pattern-like table names not supported")
    }

    val query = new StringBuilder("show column stats ")

    if (StringUtils.isNotEmpty(schemaName) && !isWildcardSetByKyuubi(schemaName)) {
      query.append(s"$schemaName.")
    }

    query.append(tableName)
    query.toString()
  }

  override def getTRowSetGenerator(): JdbcTRowSetGenerator = new ImpalaTRowSetGenerator

  override def getSchemaHelper(): SchemaHelper = new ImpalaSchemaHelper

  override def name(): String = "impala"

  private def isPattern(value: String): Boolean = {
    value != null && !isWildcardSetByKyuubi(value) && value.contains("*")
  }

  private def isWildcardSetByKyuubi(pattern: String): Boolean = pattern == "%"

  private def toImpalaRegex(pattern: String): String = {
    pattern.replace("%", "*")
  }
}
