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

package org.apache.kyuubi.engine.spark.shim

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructField

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.sparkMajorMinorVersion
import org.apache.kyuubi.schema.SchemaHelper

/**
 * A shim that defines the interface interact with Spark's catalogs
 */
trait SparkCatalogShim extends Logging {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                          Catalog                                            //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Get all register catalogs in Spark's `CatalogManager`
   */
  def getCatalogs(spark: SparkSession): Seq[Row]

  protected def catalogExists(spark: SparkSession, catalog: String): Boolean

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                           Schema                                            //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * a list of [[Row]]s, with 2 fields `schemaName: String, catalogName: String`
   */
  def getSchemas(spark: SparkSession, catalogName: String, schemaPattern: String): Seq[Row]

  protected def getGlobalTempViewManager(spark: SparkSession, schemaPattern: String): Seq[String]

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                        Table & View                                         //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  def getCatalogTablesOrViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      tableTypes: Set[String]): Seq[Row]

  def getTempViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String): Seq[Row]

  protected def getViews(
      spark: SparkSession,
      schemaPattern: String,
      tablePattern: String): Seq[TableIdentifier]


  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                          Columns                                            //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  def getColumns(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: String): Seq[Row]

  protected def toColumnResult(
      catalog: String, db: String, table: String, col: StructField, pos: Int): Row = {
    Row(
      catalog,                                              // TABLE_CAT
      db,                                                   // TABLE_SCHEM
      table,                                                // TABLE_NAME
      col.name,                                             // COLUMN_NAME
      SchemaHelper.toJavaSQLType(col.dataType),             // DATA_TYPE
      col.dataType.sql,                                     // TYPE_NAME
      SchemaHelper.getColumnSize(col.dataType).orNull,      // COLUMN_SIZE
      null,                                                 // BUFFER_LENGTH
      SchemaHelper.getDecimalDigits(col.dataType).orNull,   // DECIMAL_DIGITS
      SchemaHelper.getNumPrecRadix(col.dataType).orNull,    // NUM_PREC_RADIX
      if (col.nullable) 1 else 0,                           // NULLABLE
      col.getComment().getOrElse(""),                       // REMARKS
      null,                                                 // COLUMN_DEF
      null,                                                 // SQL_DATA_TYPE
      null,                                                 // SQL_DATETIME_SUB
      null,                                                 // CHAR_OCTET_LENGTH
      pos,                                                  // ORDINAL_POSITION
      "YES",                                                // IS_NULLABLE
      null,                                                 // SCOPE_CATALOG
      null,                                                 // SCOPE_SCHEMA
      null,                                                 // SCOPE_TABLE
      null,                                                 // SOURCE_DATA_TYPE
      "NO"                                                  // IS_AUTO_INCREMENT
    )
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //                                         Miscellaneous                                       //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  protected def invoke(
      obj: Any,
      methodName: String,
      args: (Class[_], AnyRef)*): Any = {
    val (types, values) = args.unzip
    val method = obj.getClass.getMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values.toSeq: _*)
  }

  protected def invoke(
      clazz: Class[_],
      obj: AnyRef,
      methodName: String,
      args: (Class[_], AnyRef)*): AnyRef = {
    val (types, values) = args.unzip
    val method = clazz.getMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values.toSeq: _*)
  }

  protected def getField(o: Any, fieldName: String): Any = {
    val field = o.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(o)
  }

  protected def matched(tableTypes: Set[String], tableType: String): Boolean = {
    val typ = if (tableType.equalsIgnoreCase("VIEW")) "VIEW" else "TABLE"
    tableTypes.exists(typ.equalsIgnoreCase)
  }

}

object SparkCatalogShim {
  def apply(): SparkCatalogShim = {
    sparkMajorMinorVersion match {
      case (3, _) => new CatalogShim_v3_0
      case (2, _) => new CatalogShim_v2_4
      case _ =>
        throw new IllegalArgumentException(s"Not Support spark version $sparkMajorMinorVersion")
    }
  }

  val SESSION_CATALOG: String = "spark_catalog"

  val sparkTableTypes = Set("VIEW", "TABLE")
}
