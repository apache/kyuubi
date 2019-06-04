/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}

case class KyuubiShowTablesCommand(
    databaseName: String,
    tableIdentifierPattern: String,
    tableTypes: Seq[String]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val databases = catalog.listDatabases(databaseName)
    val tableIdentifiers = databases.flatMap { db => catalog.listTables(db, tableIdentifierPattern)}
    val types = tableTypes.map(_.toUpperCase)

    val tables = tableIdentifiers.map(catalog.getTempViewOrPermanentTableMetadata)
    tables.flatMap { table =>
      val tableType = table.tableType.name
      if (types.contains(tableType)) {
        Some(
          Row("", table.database, table.identifier.table, tableType, table.comment.getOrElse(""))
        )
      } else {
        None
      }
    }
  }
}
