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

import org.apache.spark.sql.{DataFrame, SparkSession, SparkSessionExtensions}

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

trait SparkSessionProvider {
  protected val catalogImpl: String
  protected def format: String = if (catalogImpl == "hive") "hive" else "parquet"
  protected val isSparkV2: Boolean = isSparkVersionAtMost("2.4")
  protected val isSparkV31OrGreater: Boolean = isSparkVersionAtLeast("3.1")
  protected val isSparkV32OrGreater: Boolean = isSparkVersionAtLeast("3.2")
  protected val isSparkV33OrGreater: Boolean = isSparkVersionAtLeast("3.3")

  protected val extension: SparkSessionExtensions => Unit = _ => Unit
  protected lazy val spark: SparkSession = {
    val metastore = {
      val path = Files.createTempDirectory("hms")
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
        Files.createTempDirectory("spark-warehouse").toString)
      .withExtensions(extension)
      .getOrCreate()
  }

  protected val sql: String => DataFrame = spark.sql

}
