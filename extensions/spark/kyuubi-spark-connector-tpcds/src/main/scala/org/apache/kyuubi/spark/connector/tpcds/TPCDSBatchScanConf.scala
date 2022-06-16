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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.spark.connector.common.SparkConfParser
import org.apache.kyuubi.spark.connector.tpcds.TPCDSBatchScanConf._

case class TPCDSBatchScanConf(
    spark: SparkSession,
    table: Table,
    options: CaseInsensitiveStringMap) {

  private val confParser: SparkConfParser =
    SparkConfParser(options, spark.conf, table.properties())

  lazy val maxPartitionBytes: Long = confParser.longConf()
    .option(MAX_PARTITION_BYTES_CONF)
    .sessionConf(s"$TPCDS_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF")
    .tableProperty(MAX_PARTITION_BYTES_CONF)
    .defaultValue(MAX_PARTITION_BYTES_DEFAULT)
    .parse()

}

object TPCDSBatchScanConf {
  val TPCDS_CONNECTOR_READ_CONF_PREFIX = "spark.connector.tpcds.read"

  val MAX_PARTITION_BYTES_CONF = "maxPartitionBytes"
  val MAX_PARTITION_BYTES_DEFAULT = 128 * 1024 * 1024L
}
