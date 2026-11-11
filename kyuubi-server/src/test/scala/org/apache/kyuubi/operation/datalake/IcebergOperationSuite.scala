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

package org.apache.kyuubi.operation.datalake

import org.apache.kyuubi.{WithKyuubiServer, WithSimpleHMSContainer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.IcebergMetadataTests
import org.apache.kyuubi.tags.IcebergTest

@IcebergTest
class IcebergOperationSuite extends WithKyuubiServer
  with IcebergMetadataTests
  with WithSimpleHMSContainer {

  override def extraConfigs: Map[String, String] = Map(
    "spark.sql.catalogImplementation" -> "hive",
    "spark.hadoop.hive.metastore.uris" -> hmsThriftUris,
    "spark.sql.defaultCatalog" -> catalog,
    "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog" -> "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.spark_catalog.type" -> "hive",
    "spark.sql.catalog.spark_catalog.uris" -> hmsThriftUris,
    "spark.sql.catalog.spark_catalog.cache-enabled" -> "false",
    "spark.hadoop.iceberg.engine.hive.lock-enabled" -> "false",
    "spark.hadoop.iceberg.engine.hive.enabled" -> "true",
    s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
    s"spark.sql.catalog.$catalog.type" -> "hadoop",
    s"spark.sql.catalog.$catalog.warehouse" -> warehouse.toString,
    "spark.jars" -> extraJars)

  override protected lazy val conf: KyuubiConf = {
    val kyuubiConf = KyuubiConf().set(KyuubiConf.ENGINE_IDLE_TIMEOUT, 20000L)
    extraConfigs.foreach { case (k, v) => kyuubiConf.set(k, v) }
    kyuubiConf
  }

  override def jdbcUrl: String = getJdbcUrl
}
