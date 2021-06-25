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

package org.apache.kyuubi.tpcds

import java.io.InputStream
import java.lang.ProcessBuilder.Redirect
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.PosixFilePermissions._

import scala.io.Source

import org.apache.spark.{KyuubiSparkUtils, SparkEnv}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

case class TableGenerator(
    name: String,
    partitionCols: Seq[String],
    fields: StructField*) {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  private val schema: StructType = StructType(fields)
  private val rawSchema: StructType = StructType(fields.map(f => StructField(f.name, StringType)))

  private var scaleFactor: Int = 1

  private var parallelism: Int = scaleFactor * 2

  private val ss: SparkSession = SparkSession.active
  private val format: String = ss.conf.get("spark.sql.sources.default", "parquet")

  private def radix: Int = {
    math.min(math.max(5, scaleFactor / 100), parallelism)
  }

  private def toDF: DataFrame = {
    val rawRDD = ss.sparkContext.parallelize(1 to parallelism, parallelism).flatMap { i =>
      val os = System.getProperty("os.name").split(' ')(0).toLowerCase
      val loader = Thread.currentThread().getContextClassLoader

      val tempDir = KyuubiSparkUtils.createTempDir(SparkEnv.get.conf)
      tempDir.toPath
      val dsdgen = Paths.get(tempDir.toString, "dsdgen")
      val idx = Paths.get(tempDir.toString, "tpcds.idx")

      Seq(dsdgen, idx).foreach { file =>
        val in: InputStream = loader.getResourceAsStream(s"bin/$os/${file.toFile.getName}")
        Files.createFile(file, asFileAttribute(fromString("rwx------")))
        val outputStream = Files.newOutputStream(file)
        try {
          val buffer = new Array[Byte](8192)
          var bytesRead = 0
          val canRead = () => {
            bytesRead = in.read(buffer)
            bytesRead != -1
          }
          while (canRead()) {
            outputStream.write(buffer, 0, bytesRead)
          }
        } finally {
          outputStream.flush()
          outputStream.close()
          in.close()
        }
      }

      val cmd = s"./dsdgen" +
        s" -TABLE $name" +
        s" -SCALE $scaleFactor" +
        s" -PARALLEL $parallelism" +
        s" -child $i" +
        s" -DISTRIBUTIONS tpcds.idx" +
        s" -FORCE Y" +
        s" -QUIET Y"

      val builder = new ProcessBuilder(cmd.split(" "): _*)
      builder.directory(tempDir)
      builder.redirectError(Redirect.INHERIT)
      logger.info(s"Start $cmd at ${builder.directory()}")
      val process = builder.start()
      val res = process.waitFor()

      logger.info(s"Finish w/ $res $cmd")
      val data = Paths.get(tempDir.toString, s"${name}_${i}_$parallelism.dat")
      val iterator = if (Files.exists(data)) {
        // ... realized that when opening the dat files I should use the “Cp1252” encoding.
        // https://github.com/databricks/spark-sql-perf/pull/104
        // noinspection SourceNotClosed
        Source.fromFile(data.toFile, "cp1252", 8192).getLines
      } else {
        logger.warn(s"No data generated in child $i")
        Nil
      }
      iterator
    }

    val rowRDD = rawRDD.mapPartitions { iter =>
      iter.map { line =>
        val v = line.split("\\|", -1).dropRight(1).map(Option(_).filter(_.nonEmpty).orNull)
        Row.fromSeq(v)
      }
    }

    val columns = fields.map { f => col(f.name).cast(f.dataType).as(f.name) }
    ss.createDataFrame(rowRDD, rawSchema).select(columns: _*)
  }

  def setScaleFactor(scale: Int): Unit = {
    this.scaleFactor = scale
  }

  def setParallelism(parallel: Int): Unit = {
    this.parallelism = math.max(2, parallel)
  }

  def create(): Unit = {
    val data = if (partitionCols.isEmpty) {
      toDF.repartition(radix)
    } else {
      toDF.persist()
    }

    val tempViewName = s"${name}_view"

    data.createOrReplaceTempView(tempViewName)

    val writer = if (partitionCols.nonEmpty) {
      val query =
        s"""
           |(SELECT
           | ${fields.map(_.name).mkString(", ")}
           |FROM
           | $tempViewName WHERE ${partitionCols.head} IS NOT NULL
           |DISTRIBUTE BY ${partitionCols.head})
           |UNION ALL
           |(SELECT
           | ${fields.map(_.name).mkString(", ")}
           |FROM
           | $tempViewName
           |WHERE ${partitionCols.head} IS NULL
           |DISTRIBUTE BY CAST(RAND() * $radix AS INT))
           |""".stripMargin

      ss.sql(query).write.partitionBy(partitionCols: _*)
    } else {
      data.write
    }
    ss.sql(s"DROP TABLE IF EXISTS $name")
    writer.format(format).saveAsTable(name)
    data.unpersist()
    ss.sqlContext.dropTempTable(tempViewName)
  }

  override def toString: String = {
    s"""
       |Table: $name
       |Format: $format
       |ScaleFactor: ${scaleFactor}GB
       |Schema:${schema.sql}
       |Partitions: ${partitionCols.mkString("[", ", ", "]")}
       |""".stripMargin
  }
}
