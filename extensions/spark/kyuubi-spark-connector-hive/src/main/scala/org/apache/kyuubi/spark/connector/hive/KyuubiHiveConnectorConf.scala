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

package org.apache.kyuubi.spark.connector.hive

object KyuubiHiveConnectorConf {

  import org.apache.spark.sql.internal.SQLConf.buildStaticConf

  val EXTERNAL_CATALOG_POOL_ENABLED =
    buildStaticConf("spark.sql.kyuubi.hive.connector.externalCatalogPool.enabled")
      .internal()
      .doc("Indicate to whether enable kyuubi hive connector external catalog pool, The " +
        "CatalogManager in Apache Spark is session level, and Kyuubi supports multiple " +
        "sessions. If the number of sessions is large and Kyuubi hive connector is used, " +
        "the JVM memory usage may be large due to the loading of multiple external catalogs. " +
        "In addition, multiple Hive clients will be created, So, can enable this configure to " +
        "pool the ExternalCatalog to solve this issue.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  val EXTERNAL_CATALOG_POOL_SIZE =
    buildStaticConf("spark.sql.kyuubi.hive.connector.externalCatalogPool.threshold")
      .internal()
      .doc("The maximum size of pool of the single catalog.")
      .version("2.4.6")
      .intConf
      .checkValue(thres => thres >= 2 && thres <= 128, "The threshold must be in [2,128].")
      .createWithDefault(3)
}
