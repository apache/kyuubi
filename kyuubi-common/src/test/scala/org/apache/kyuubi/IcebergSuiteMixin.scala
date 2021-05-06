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

package org.apache.kyuubi

import java.nio.file.Path

trait IcebergSuiteMixin extends DataLakeSuiteMixin {

  override protected val format: String = "iceberg"

  override protected val catalog: String = "hadoop_prod"

  override protected val warehouse: Path = Utils.createTempDir()

  override protected val extraJars: String = {
    System.getProperty("java.class.path")
      .split(":")
      .filter(_.contains("iceberg-spark")).head
  }

  override protected val extraConfigs = Map(
    "spark.sql.defaultCatalog" -> catalog,
    "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog" -> "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.spark_catalog.type" -> "hive",
    s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
    s"spark.sql.catalog.$catalog.type" -> "hadoop",
    s"spark.sql.catalog.$catalog.warehouse" -> warehouse.toString,
    "spark.jars" -> extraJars)
}
