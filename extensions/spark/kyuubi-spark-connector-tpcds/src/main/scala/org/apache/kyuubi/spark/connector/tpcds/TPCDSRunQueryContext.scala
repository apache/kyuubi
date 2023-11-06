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

import java.io.{InputStream, PrintWriter, StringWriter}
import java.util
import java.util.Locale

import scala.io.{Codec, Source}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import org.apache.kyuubi.spark.connector.tpcds.TPCDSRunQueryContext._

class TPCDSRunQueryContext(spark: SparkSession, database: Option[String]) extends Logging {

  def runAllQueries(): DataFrame = {
    runQueries(allQueries: _*)
  }

  def runQueries(queries: String*): DataFrame = withDatabase {
    val results = new util.ArrayList[Row]()
    queries.foreach { query =>
      results.add(run(query.trim.toLowerCase(Locale.ROOT)))
    }

    spark.createDataFrame(results, outputSchema)
  }

  def runQuery(name: String): DataFrame = {
    runQueries(name)
  }

  private def run(name: String): Row = {
    var status = "SUCCESS"
    var duration = 0
    var error: String = null
    try {
      val query = querySQLs(name)
      spark.sparkContext.setJobGroup(name, s"$name:\n$query", true)
      val start = System.currentTimeMillis()
      spark.sql(query).collect()
      duration = (System.currentTimeMillis() - start).toInt
    } catch {
      case e: Exception =>
        logWarning(s"Failed to run query $name", e)
        status = "FAILED"
        error = TPCDSRunQueryContext.prettyPrintException(e)
    } finally {
      spark.sparkContext.clearJobGroup()
    }

    Row.apply(name, status, duration, error)
  }

  private def withDatabase[T](f: => T): T = {
    if (database.isEmpty) {
      f
    } else {
      val currentCatalog = spark.catalog.currentCatalog()
      val originalDatabase = spark.catalog.currentDatabase
      try {
        spark.sql(s"use ${database.get}")
        f
      } finally {
        spark.catalog.setCurrentCatalog(currentCatalog)
        spark.catalog.setCurrentDatabase(originalDatabase)
      }
    }
  }
}

object TPCDSRunQueryContext {

  def apply(): TPCDSRunQueryContext = {
    new TPCDSRunQueryContext(SparkSession.active, None)
  }

  def apply(database: String): TPCDSRunQueryContext = {
    new TPCDSRunQueryContext(SparkSession.active, Some(database))
  }

  val outputSchema: StructType = StructType(
    StructField("name", StringType) ::
      StructField("status", StringType) ::
      StructField("duration", IntegerType) ::
      StructField("error", StringType) :: Nil)

  val tpcdsRootDir: String = "kyuubi/tpcds_3.2"

  val allQueries = Seq(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14a",
    "q14b",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21",
    "q22",
    "q23a",
    "q23b",
    "q24a",
    "q24b",
    "q25",
    "q26",
    "q27",
    "q28",
    "q29",
    "q30",
    "q31",
    "q32",
    "q33",
    "q34",
    "q35",
    "q36",
    "q37",
    "q38",
    "q39a",
    "q39b",
    "q40",
    "q41",
    "q42",
    "q43",
    "q44",
    "q45",
    "q46",
    "q47",
    "q48",
    "q49",
    "q50",
    "q51",
    "q52",
    "q53",
    "q54",
    "q55",
    "q56",
    "q57",
    "q58",
    "q59",
    "q60",
    "q61",
    "q62",
    "q63",
    "q64",
    "q65",
    "q66",
    "q67",
    "q68",
    "q69",
    "q70",
    "q71",
    "q72",
    "q73",
    "q74",
    "q75",
    "q76",
    "q77",
    "q78",
    "q79",
    "q80",
    "q81",
    "q82",
    "q83",
    "q84",
    "q85",
    "q86",
    "q87",
    "q88",
    "q89",
    "q90",
    "q91",
    "q92",
    "q93",
    "q94",
    "q95",
    "q96",
    "q97",
    "q98",
    "q99")

  // all queries: Map(name -> sql)
  lazy val querySQLs: Map[String, String] = {
    allQueries.map(name => {
      (name, getQuerySql(s"$tpcdsRootDir/$name.sql"))
    }).toMap
  }

  private def getQuerySql(resource: String): String = {
    var in: InputStream = null
    try {
      in = getClass.getClassLoader.getResourceAsStream(resource)
      Source.fromInputStream(in)(Codec.UTF8).getLines()
        .filter(!_.startsWith("--"))
        .mkString("\n")
    } finally {
      if (in != null) in.close()
    }
  }

  /**
   * Return a nice string representation of the exception. It will call "printStackTrace" to
   * recursively generate the stack trace including the exception and its causes.
   *
   * Copy from `org.apache.kyuubi.Utils#prettyPrint(java.lang.Throwable)`
   */
  private def prettyPrintException(e: Throwable): String = {
    if (e == null) {
      ""
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }
}
