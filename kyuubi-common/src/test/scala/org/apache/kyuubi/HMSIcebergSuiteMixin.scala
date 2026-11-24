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

/**
 * Runs the Iceberg suites against a real Hive Metastore provided by [[WithSimpleHMSContainer]],
 * where the session catalog `spark_catalog` is backed by Iceberg's `SparkSessionCatalog`,
 * instead of the in-memory one from [[IcebergSuiteMixin]].
 */
trait HMSIcebergSuiteMixin extends IcebergSuiteMixin {
  self: WithSimpleHMSContainer =>

  override protected def extraConfigs: Map[String, String] = super.extraConfigs ++ Map(
    "spark.sql.catalogImplementation" -> "hive",
    "spark.hadoop.hive.metastore.uris" -> hmsThriftUris,
    "spark.sql.catalog.spark_catalog.uris" -> hmsThriftUris,
    "spark.sql.catalog.spark_catalog.cache-enabled" -> "false",
    "spark.hadoop.iceberg.engine.hive.lock-enabled" -> "false",
    // Reproduces https://github.com/apache/kyuubi/issues/7673: with this flag the Hive
    // storage handler is registered in the metastore but missing from the Iceberg runtime,
    // so the batched v1 lookup fails and GetTables falls back. Tables created with
    // TBLPROPERTIES ('engine.hive.enabled' = 'false') exercise the successful batch.
    "spark.hadoop.iceberg.engine.hive.enabled" -> "true")
}
