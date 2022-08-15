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
import org.apache.kyuubi.spark.connector.tpcds.TPCDSConf._

case class TPCDSConf(spark: SparkSession, options: CaseInsensitiveStringMap) {

  private val confParser: SparkConfParser = SparkConfParser(options, spark.conf, null)

  lazy val excludeDatabases: Array[String] = confParser.stringConf()
    .option(EXCLUDE_DATABASES)
    .parseOptional()
    .map(_.split(",").map(_.toLowerCase.trim).filter(_.nonEmpty))
    .getOrElse(Array.empty)

  // When true, use CHAR VARCHAR; otherwise use STRING
  lazy val useAnsiStringType: Boolean = confParser.booleanConf()
    .option(USE_ANSI_STRING_TYPE)
    .sessionConf(s"$TPCDS_CONNECTOR_CONF_PREFIX.$USE_ANSI_STRING_TYPE")
    .defaultStringValue(USE_ANSI_STRING_TYPE_DEFAULT)
    .parse()

  // 09-26-2017 v2.6.0
  // Replaced two occurrences of "c_last_review_date" with "c_last_review_date_sk" to be consistent
  // with Table 2-14 (Customer Table Column Definitions) in section 2.4.7 of the specification
  // (fogbugz 2046).
  //
  // https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf
  lazy val useTableSchema_2_6: Boolean = confParser.booleanConf()
    .option(USE_TABLE_SCHEMA_2_6)
    .sessionConf(s"$TPCDS_CONNECTOR_CONF_PREFIX.$USE_TABLE_SCHEMA_2_6")
    .defaultStringValue(USE_TABLE_SCHEMA_2_6_DEFAULT)
    .parse()
}

case class TPCDSReadConf(
    spark: SparkSession,
    table: Table,
    options: CaseInsensitiveStringMap) {

  private val confParser: SparkConfParser =
    SparkConfParser(options, spark.conf, table.properties)

  lazy val maxPartitionBytes: Long = confParser.bytesConf()
    .option(MAX_PARTITION_BYTES_CONF)
    .sessionConf(s"$TPCDS_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF")
    .tableProperty(s"$TPCDS_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF")
    .defaultStringValue(MAX_PARTITION_BYTES_DEFAULT)
    .parse()
}

object TPCDSConf {
  val EXCLUDE_DATABASES = "excludeDatabases"

  val TPCDS_CONNECTOR_CONF_PREFIX = "spark.connector.tpcds"
  val USE_ANSI_STRING_TYPE = "useAnsiStringType"
  val USE_ANSI_STRING_TYPE_DEFAULT = "false"
  val USE_TABLE_SCHEMA_2_6 = "useTableSchema_2_6"
  val USE_TABLE_SCHEMA_2_6_DEFAULT = "true"

  val TPCDS_CONNECTOR_READ_CONF_PREFIX = s"$TPCDS_CONNECTOR_CONF_PREFIX.read"
  val MAX_PARTITION_BYTES_CONF = "maxPartitionBytes"
  val MAX_PARTITION_BYTES_DEFAULT = "128m"
}
