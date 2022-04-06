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

package org.apache.kyuubi.plugin.spark.authz

import java.nio.file.Files

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.{DataFrame, SparkSession, SparkSessionExtensions}

import org.apache.kyuubi.Utils

trait SparkSessionProvider {
  protected val catalogImpl: String
  protected def format: String = if (catalogImpl == "hive") "hive" else "parquet"
  protected val isSparkV2: Boolean = SPARK_VERSION.split("\\.").head == "2"
  protected val isSparkV31OrGreater: Boolean = {
    val parts = SPARK_VERSION.split("\\.").map(_.toInt)
    (parts.head > 3) || (parts.head == 3 && parts(1) >= 1)
  }
  protected val isSparkV32OrGreater: Boolean = {
    val parts = SPARK_VERSION.split("\\.").map(_.toInt)
    (parts.head > 3) || (parts.head == 3 && parts(1) >= 2)
  }

  protected val extension: SparkSessionExtensions => Unit = _ => Unit
  protected lazy val spark: SparkSession = {
    val metastore = {
      val path = Utils.createTempDir(namePrefix = "hms")
      Files.delete(path)
      path
    }
    SparkSession.builder()
      .master("local")
      .config("spark.ui.enabled", "false")
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$metastore;create=true")
      .config("spark.sql.catalogImplementation", catalogImpl)
      .config(
        "spark.sql.warehouse.dir",
        Utils.createTempDir(namePrefix = "spark-warehouse").toString)
      .withExtensions(extension)
      .getOrCreate()
  }

  protected val sql: String => DataFrame = spark.sql

}
