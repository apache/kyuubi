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

package org.apache.kyuubi.spark.connector.tpch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.spark.connector.common.SparkConfParser
import org.apache.kyuubi.spark.connector.tpch.TPCHConf._

case class TPCHConf(spark: SparkSession, options: CaseInsensitiveStringMap) {

  private val confParser: SparkConfParser = SparkConfParser(options, spark.conf, null)

  lazy val excludeDatabases: Array[String] = confParser.stringConf()
    .option(EXCLUDE_DATABASES)
    .parseOptional()
    .map(_.split(",").map(_.toLowerCase.trim).filter(_.nonEmpty))
    .getOrElse(Array.empty)

  // When true, use CHAR VARCHAR; otherwise use STRING
  lazy val useAnsiStringType: Boolean = confParser.booleanConf()
    .option(USE_ANSI_STRING_TYPE)
    .sessionConf(s"$TPCH_CONNECTOR_CONF_PREFIX.$USE_ANSI_STRING_TYPE")
    .defaultStringValue(USE_ANSI_STRING_TYPE_DEFAULT)
    .parse()
}

case class TPCHReadConf(
    spark: SparkSession,
    table: Table,
    options: CaseInsensitiveStringMap) {

  private val confParser: SparkConfParser =
    SparkConfParser(options, spark.conf, table.properties)

  lazy val maxPartitionBytes: Long = confParser.bytesConf()
    .option(MAX_PARTITION_BYTES_CONF)
    .sessionConf(s"$TPCH_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF")
    .tableProperty(s"$TPCH_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF")
    .defaultStringValue(MAX_PARTITION_BYTES_DEFAULT)
    .parse()
}

object TPCHConf {
  val EXCLUDE_DATABASES = "excludeDatabases"

  val TPCH_CONNECTOR_CONF_PREFIX = "spark.connector.tpch"
  val USE_ANSI_STRING_TYPE = "useAnsiStringType"
  val USE_ANSI_STRING_TYPE_DEFAULT = "false"

  val TPCH_CONNECTOR_READ_CONF_PREFIX = s"$TPCH_CONNECTOR_CONF_PREFIX.read"
  val MAX_PARTITION_BYTES_CONF = "maxPartitionBytes"
  val MAX_PARTITION_BYTES_DEFAULT = "128m"
}
