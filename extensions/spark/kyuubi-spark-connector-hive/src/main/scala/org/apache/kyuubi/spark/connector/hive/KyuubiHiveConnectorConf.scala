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

import java.util.Locale

import org.apache.spark.sql.internal.SQLConf.buildConf

object KyuubiHiveConnectorConf {

  import org.apache.spark.sql.internal.SQLConf.buildStaticConf

  val EXTERNAL_CATALOG_SHARE_POLICY =
    buildStaticConf("spark.sql.kyuubi.hive.connector.externalCatalog.share.policy")
      .internal()
      .doc(s"Indicates the share policy for the externalCatalog in the Kyuubi Connector, we use" +
        "'all' by default. " +
        "<li>ONE_FOR_ONE: Indicate to an external catalog is used by only one HiveCatalog. </li> " +
        "<li>ONE_FOR_ALL: Indicate to an external catalog is shared globally with the " +
        "HiveCatalogs with the same catalogName. </li> ")
      .version("1.7.0")
      .stringConf
      .transform(policy => policy.toUpperCase(Locale.ROOT))
      .checkValue(
        policy => Set("ONE_FOR_ONE", "ONE_FOR_ALL").contains(policy),
        "Invalid value for 'spark.sql.kyuubi.hive.connector.externalCatalog.share.policy'." +
          "Valid values are 'ONE_FOR_ONE', 'ONE_FOR_ALL'.")
      .createWithDefault(OneForAllPolicy.name)

  val READ_CONVERT_METASTORE_ORC =
    buildConf("spark.sql.kyuubi.hive.connector.read.convertMetastoreOrc")
      .doc("When enabled, the data source ORC reader is used to process " +
        "ORC tables created by using the HiveQL syntax, instead of Hive SerDe.")
      .version("1.11.0")
      .booleanConf
      .createWithDefault(true)

  val READ_CONVERT_METASTORE_PARQUET =
    buildConf("spark.sql.kyuubi.hive.connector.read.convertMetastoreParquet")
      .doc("When enabled, the data source PARQUET reader is used to process " +
        "PARQUET tables created by using the HiveQL syntax, instead of Hive SerDe.")
      .version("1.11.0")
      .booleanConf
      .createWithDefault(true)

  val HIVE_FILE_STATUS_CACHE_SCOPE =
    buildConf("spark.sql.kyuubi.hive.file.status.cache.scope")
      .doc("The scope of hive file status cache, global, session and none.")
      .version("1.11.0")
      .stringConf
      .transform(policy => policy.toUpperCase(Locale.ROOT))
      .checkValue(
        policy => Set("SESSION", "NONE").contains(policy),
        "Invalid value for 'spark.sql.kyuubi.hive.file.status.cache.scope'." +
          "Valid values are 'SESSION', 'NONE'.")
      .createWithDefault("SESSION")
}
