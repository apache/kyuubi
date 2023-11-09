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
package org.apache.kyuubi.spark.connector.tpcds

import io.trino.tpcds.{Table, TpcdsException}
import org.apache.spark.sql.SparkSession

class TPCDSGenerateContext(spark: SparkSession, database: Option[String]) {

  def persistAll(opts: TPCDSPersistOpts): Unit = {
    persistTables(opts, TPCDSSchemaUtils.BASE_TABLES.map(_.getName()): _*)
  }

  def persistTables(opts: TPCDSPersistOpts, tables: String*): Unit = {
    tables.foreach { table =>
      try {
        spark.sparkContext.setJobGroup(table, s"Generate $table", true)

        val tpcdsTable = Table.getTable(table)
        val tableName = if (database.isDefined) {
          s"${database.get}.$table"
        } else {
          table
        }
        val sparkTable = spark.table(tableName)

        val writer = sparkTable.write.format(opts.format)

        if (opts.overwrite) {
          writer.mode("overwrite")
        }

        val partitionColumns = TPCDSSchemaUtils
          .tablePartitionColumnNames(tpcdsTable, tpcdsConf.useTableSchema_2_6)
        if (partitionColumns.nonEmpty) {
          writer.partitionBy(partitionColumns: _*)
        }

        writer.saveAsTable(s"${opts.targetDb}.$table")
      } finally {
        spark.sparkContext.clearJobGroup()
      }
    }
  }

  def persistTable(opts: TPCDSPersistOpts, table: String): Unit = {
    persistTables(opts, table)
  }

  lazy val tpcdsConf: TPCDSConf = {
    val catalog = if (database.isEmpty) {
      spark.sessionState.catalogManager.currentCatalog
    } else {
      val dbParts = spark.sessionState.sqlParser.parseMultipartIdentifier(database.get)
      if (dbParts.size == 1) {
        spark.sessionState.catalogManager.currentCatalog
      } else {
        spark.sessionState.catalogManager.catalog(dbParts.head)
      }
    }
    catalog match {
      case c: TPCDSCatalog => c.tpcdsConf
      case _ =>
        val errorMsg = if (database.isEmpty) {
          "Current database is not a TPCDS database, please specify a TPCDS database."
        } else {
          s"Database[${database.get}] is not a TPCDS database"
        }
        throw new TpcdsException(errorMsg)
    }
  }
}

object TPCDSGenerateContext {

  def apply(): TPCDSGenerateContext =
    new TPCDSGenerateContext(SparkSession.active, None)

  def apply(database: String): TPCDSGenerateContext =
    new TPCDSGenerateContext(SparkSession.active, Some(database))

}

case class TPCDSPersistOpts(
    targetDb: String,
    format: String = "parquet",
    overwrite: Boolean = true)
