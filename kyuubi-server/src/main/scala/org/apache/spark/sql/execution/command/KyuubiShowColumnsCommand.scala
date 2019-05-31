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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types._

import yaooqinn.kyuubi.schema.SchemaMapper

case class KyuubiShowColumnsCommand(
    databasePattern: String,
    tableIdentifierPattern: String,
    columnPattern: String) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    AttributeReference("TABLE_CAT", StringType, nullable = false)() ::
      AttributeReference("TABLE_SCHEM", StringType, nullable = false)() ::
      AttributeReference("TABLE_NAME", StringType, nullable = false)() ::
      AttributeReference("COLUMN_NAME", StringType, nullable = false)() ::
      AttributeReference("DATA_TYPE", IntegerType, nullable = false)() ::
      AttributeReference("TYPE_NAME", StringType, nullable = false)() ::
      AttributeReference("COLUMN_SIZE", IntegerType, nullable = false)() ::
      AttributeReference("BUFFER_LENGTH", ByteType, nullable = true)() ::
      AttributeReference("DECIMAL_DIGITS", IntegerType, nullable = true)() ::
      AttributeReference("NUM_PREC_RADIX", IntegerType, nullable = true)() ::
      AttributeReference("NULLABLE", IntegerType, nullable = false)() ::
      AttributeReference("REMARKS", StringType, nullable = true)() ::
      AttributeReference("COLUMN_DEF", StringType, nullable = true)() ::
      AttributeReference("SQL_DATA_TYPE", IntegerType, nullable = true)() ::
      AttributeReference("SQL_DATETIME_SUB", IntegerType, nullable = true)() ::
      AttributeReference("CHAR_OCTET_LENGTH", IntegerType, nullable = true)() ::
      AttributeReference("ORDINAL_POSITION", IntegerType, nullable = false)() ::
      AttributeReference("IS_NULLABLE", StringType, nullable = false)() ::
      AttributeReference("SCOPE_CATALOG", StringType, nullable = true)() ::
      AttributeReference("SCOPE_SCHEMA", StringType, nullable = true)() ::
      AttributeReference("SCOPE_TABLE", StringType, nullable = true)() ::
      AttributeReference("SOURCE_DATA_TYPE", ShortType, nullable = true)() ::
      AttributeReference("IS_AUTO_INCREMENT", StringType, nullable = false)() ::Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val pattern = if (columnPattern != null) columnPattern.r.pattern else ".*".r.pattern
    val databases = catalog.listDatabases(databasePattern)
    val tableIdentifiers = databases.flatMap { db =>
      catalog.listTables(db, tableIdentifierPattern)
    }
    val tables = tableIdentifiers.map(catalog.getTempViewOrPermanentTableMetadata)
    val result = tables.flatMap { table =>
      table.schema
        .filter(f => pattern.matcher(f.name).matches())
        .zipWithIndex.map { case (f, i) =>
        Row(
          "", // TABLE_CAT
          table.database, // TABLE_SCHEM
          table.identifier.table, // TABLE_NAME
          f.name, // COLUMN_NAME
          SchemaMapper.toJavaSQLType(f.dataType), // DATA_TYPE
          f.dataType.typeName, // TYPE_NAME
          SchemaMapper.getColumnSize(f.dataType).orNull, // COLUMN_SIZE
          null, // BUFFER_LENGTH, unused
          SchemaMapper.getDecimalDigits(f.dataType).orNull, // DECIMAL_DIGITS
          SchemaMapper.getNumPrecRadix(f.dataType).orNull, // NUM_PREC_RADIX
          if (f.nullable) 1 else 0, // NULLABLE
          f.getComment().getOrElse(""), //
          null, // COLUMN_DEF
          null, // SQL_DATA_TYPE
          null, // SQL_DATETIME_SUB
          null, // CHAR_OCTET_LENGTH
          i, // ORDINAL_POSITION
          if (f.nullable) "YES" else "NO", // IS_NULLABLE
          null, // SCOPE_CATALOG
          null, // SCOPE_SCHEMA
          null, // SCOPE_TABLE
          null, // SOURCE_DATA_TYPE
          "NO" // IS_AUTO_INCREMENT
        )
      }
    }
    result
  }

}
