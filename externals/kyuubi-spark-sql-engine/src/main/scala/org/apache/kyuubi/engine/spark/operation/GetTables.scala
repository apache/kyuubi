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

package org.apache.kyuubi.engine.spark.operation

import java.util.regex.Pattern

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.operation.OperationType
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.session.Session

class GetTables(
    spark: SparkSession,
    session: Session,
    catalog: String,
    schema: String,
    tableName: String,
    tableTypes: java.util.List[String])
  extends SparkOperation(spark, OperationType.GET_TABLES, session) {

  override def statement: String = {
    super.statement +
      s" [catalog: $catalog," +
      s" schemaPattern: $schema," +
      s" tablePattern: $tableName," +
      s" tableTypes: ${Option(tableTypes).map(_.asScala.mkString("(", ", ", ")")).orNull}]"
  }

  private def matched(tableType: CatalogTableType): Boolean = {
    val commonTableType = tableType match {
      case CatalogTableType.VIEW => "VIEW"
      case _ => "TABLE"
    }
    tableTypes == null || tableTypes.isEmpty || tableTypes.contains(commonTableType)
  }

  override protected def resultSchema: StructType = {
    new StructType()
      .add(TABLE_CAT, "string", nullable = true, "Catalog name. NULL if not applicable.")
      .add(TABLE_SCHEM, "string", nullable = true, "Schema name.")
      .add(TABLE_NAME, "string", nullable = true, "Table name.")
      .add(TABLE_TYPE, "string", nullable = true, "The table type, e.g. \"TABLE\", \"VIEW\"")
      .add(REMARKS, "string", nullable = true, "Comments about the table.")
      .add("TYPE_CAT", "string", nullable = true, "The types catalog.")
      .add("TYPE_SCHEM", "string", nullable = true, "the types schema (may be null)")
      .add("TYPE_NAME", "string", nullable = true, "Type name.")
      .add("SELF_REFERENCING_COL_NAME", "string", nullable = true,
        "Name of the designated \"identifier\" column of a typed table.")
      .add("REF_GENERATION", "string", nullable = true,
        "Specifies how values in SELF_REFERENCING_COL_NAME are created.")
  }

  override protected def runInternal(): Unit = {
    try {
      val catalog = spark.sessionState.catalog
      val schemaPattern = convertSchemaPattern(schema)
      val tablePattern = convertIdentifierPattern(tableName, datanucleusFormat = true)
      val databases = catalog.listDatabases(schemaPattern)

      val tables = databases.flatMap { db =>
        val identifiers = catalog.listTables(db, tablePattern, includeLocalTempViews = false)
        catalog.getTablesByName(identifiers).filter(t => matched(t.tableType)).map { t =>
          Row(
            "",
            t.database,
            t.identifier.table,
            t.tableType.name,
            t.comment.getOrElse(""),
            null, null, null, null, null)
        }
      }

      val views = if (matched(CatalogTableType.VIEW)) {
        val globalTempViewDb = catalog.globalTempViewManager.database
        (if (StringUtils.isEmpty(schema) || schema == "*"
          || Pattern.compile(convertSchemaPattern(schema, datanucleusFormat = false))
          .matcher(globalTempViewDb).matches()) {
          catalog.listTables(globalTempViewDb, tablePattern, includeLocalTempViews = true)
        } else {
          catalog.listLocalTempViews(tablePattern)
        }).map { v =>
          Row(
            "",
            v.database.orNull,
            v.table,
            CatalogTableType.VIEW.name,
            "",
            null, null, null, null, null)
        }
      } else {
        Seq.empty[Row]
      }
      iter = (tables ++ views).toList.iterator
    } catch {
      onError()
    }
  }

}
