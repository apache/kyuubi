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

package org.apache.spark.sql

import java.io.File

import scala.util.Random

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import org.apache.kyuubi.sql.compact.CompactTable
import org.apache.kyuubi.sql.compact.merge.AbstractFileMerger

trait CompactTablSuiteBase extends KyuubiSparkSQLExtensionTest {
  def getTableSource(): String

  def getTableCodec(): Option[String]

  def getDataFiles(tableMetadata: CatalogTable): Seq[File] =
    getFiles(tableMetadata, "part-")

  private def getFiles(tableMetadata: CatalogTable, prefix: String): Seq[File] = {
    val location = tableMetadata.location
    val files = new File(location).listFiles()
    val suffix = getDataFileSuffix()
    files.filter(f =>
      f.getName.startsWith(prefix)
        && f.getName.endsWith(suffix))
  }

  def getDataFileSuffix(): String

  def getMergingFiles(tableMetadata: CatalogTable): Seq[File] =
    new File(tableMetadata.location.getPath + File.separator + ".compact").listFiles().flatMap(
      _.listFiles()).filter(_.getName.startsWith(AbstractFileMerger.mergingFilePrefix + "-"))

  def getMergedDataFiles(tableMetadata: CatalogTable): Seq[File] =
    getFiles(tableMetadata, AbstractFileMerger.mergedFilePrefix + "-")

  def withRandomTable(f: (String, Int, Int) => Unit)(implicit
      messageCountPerFile: Int = Random.nextInt(10000) + 1000,
      fileCount: Int = Random.nextInt(100) + 10): Unit = {
    val tableName =
      generateRandomTable(getTableSource(), messageCountPerFile, fileCount, getTableCodec())
    withTable(tableName) {
      f(tableName, messageCountPerFile, fileCount)
    }
  }

  def generateRandomTable(
      tableSource: String,
      messageCountPerFile: Int,
      fileCount: Int,
      codec: Option[String]): String = {
    val tableName = getRandomTableName()
    sql(s"CREATE TABLE $tableName (key INT, value STRING) USING ${tableSource}" +
      s" ${codec.map(c => s"OPTIONS('compression' '$c')").getOrElse("")}")
      .show()

    0 until fileCount foreach { i =>
      logInfo(s"inserting data into table ranges between ${i * messageCountPerFile} and $messageCountPerFile")

      sql(s"""insert into $tableName
        select /*+ COALESCE(1) */id, java_method('java.util.UUID', 'randomUUID')
        from range(${i * messageCountPerFile}, ${i * messageCountPerFile + messageCountPerFile})""")
        .show()
    }

    tableName
  }

  def getRandomTableName(): String = {
    s"small_file_table_${Random.alphanumeric.take(10).mkString}"
  }

  private def getAllFiles(tableMetadata: CatalogTable): Seq[File] =
    new File(tableMetadata.location).listFiles()

  test("generate random table") {
    withRandomTable { (tableName, messageCountPerFile, fileCount) =>
      val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val files = getDataFiles(tableMetadata)
      getAllFiles(tableMetadata).foreach(f => logInfo("all file: " + f.getAbsolutePath))
      assert(files.length == fileCount)
      val messageCount = sql(s"select count(1) from ${tableName}").collect().head.getLong(0)
      assert(messageCount == messageCountPerFile * fileCount)
    }
  }

  test(s"compact table") {
    withRandomTable { (tableName, messageCountPerFile, fileCount) =>
      val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val files = getDataFiles(tableMetadata)
      assert(files.length == fileCount)
      files.foreach(f => logInfo("merging file: " + f.getAbsolutePath))

      sql(s"compact table $tableName").show()
      val mergedTableMetadata =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val mergedFiles = getMergedDataFiles(mergedTableMetadata)
      mergedFiles.foreach(f => logInfo("merged file: " + f.getAbsolutePath))
      assert(mergedFiles.length == 1)
      sql(s"refresh table $tableName").show()
      val messageCount = sql(s"select count(1) from $tableName").collect().head.getLong(0)
      assert(messageCount == messageCountPerFile * fileCount)
    }
  }

  test(s"validating records") {
    withRandomTable { (tableName, messageCountPerFile, fileCount) =>
      val records =
        sql(s"select * from $tableName").collect().map(r => r.getInt(0) -> r.getString(1)).toMap

      records.foreach { r =>
        logInfo("records: " + r)
      }

      sql(s"compact table $tableName").show()
      sql(s"refresh table $tableName").show()
      val mergedRecords =
        sql(s"select * from $tableName").collect().map(r => r.getInt(0) -> r.getString(1)).toMap

      mergedRecords.foreach { r =>
        logInfo("merged records: " + r)
      }
      assert(records.size == mergedRecords.size)
      assert(records == mergedRecords)
    }
  }

  test(s"result view") {
    withRandomTable { (tableName, messageCountPerFile, fileCount) =>
      sql(s"compact table $tableName").show()
      val viewOpt = spark.sessionState.catalog.getTempView(
        CompactTable.mergedFilesCachedTableName)
      assert(viewOpt.isDefined)
      val view = viewOpt.get
      assert(view.isTempView)
      val result = sql(s"select * from ${CompactTable.mergedFilesCachedTableName}").collect()
      assert(result.length == 1)
      result.foreach { r =>
        logInfo("result: " + r)
      }
      assert(result.head.getString(2) == getTableSource())
      val mergedFileName =
        result.head.getList(4).get(0).asInstanceOf[GenericRowWithSchema].getString(1)
      val mergedTableMetadata =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val mergedFile = getMergedDataFiles(mergedTableMetadata).head
      assert(mergedFileName == mergedFile.getName)

    }(Random.nextInt(1000) + 100, 2)
  }

  test("compact table list") {
    withRandomTable { (tableName, messageCountPerFile, fileCount) =>
      val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val files = getDataFiles(tableMetadata)
      assert(files.length == fileCount)

      sql(s"compact table $tableName list").show()
      val mergedTableMetadata =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val mergedFiles = getMergedDataFiles(mergedTableMetadata)
      assert(mergedFiles.isEmpty)
      sql(s"select * from ${CompactTable.mergedFilesCachedTableName}").show(truncate = false)
      val result = sql(s"select * from ${CompactTable.mergedFilesCachedTableName}").collect()
      assert(result.length == 1)
      assert(result.head.getString(2) == getTableSource())
    }
  }

  test("compact table retain") {
    withRandomTable { (tableName, messageCountPerFile, fileCount) =>
      val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val files = getDataFiles(tableMetadata)
      assert(files.length == fileCount)

      sql(s"compact table $tableName retain").show()
      val mergedTableMetadata =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val mergedFiles = getMergedDataFiles(mergedTableMetadata)
      assert(mergedFiles.length == 1)

      val allFile = getMergingFiles(tableMetadata)
      assert(allFile.length == fileCount)
    }
  }

  test("recover compact table") {
    withRandomTable { (tableName, messageCountPerFile, fileCount) =>
      val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val files = getDataFiles(tableMetadata)
      assert(files.length == fileCount)

      sql(s"compact table $tableName retain").show()
      val mergedTableMetadata =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val mergedFiles = getMergedDataFiles(mergedTableMetadata)
      assert(mergedFiles.length == 1)
      sql(s"recover compact table $tableName").show()
      val recoveredFiles = getDataFiles(tableMetadata)
      assert(recoveredFiles.length == files.length)
      files.foreach { f =>
        assert(recoveredFiles.exists(_.getName == f.getName))
      }
    }
  }
}
