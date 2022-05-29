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

import io.trino.tpcds.{Scaling, Table}
import io.trino.tpcds.Table._

import org.apache.kyuubi.spark.connector.tpcds.TPCDSSchemaUtils._

// https://tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v3.2.0.pdf
// Page 42 Table 3-2 Database Row Counts
object TPCDSStatisticsUtils {

  // https://github.com/Teradata/tpcds/issues/26
  def numRows(table: Table, scale: Double): Long = {
    val nScale = normalize(scale)
    require(SCALES.contains(nScale), s"Unsupported scale $nScale")
    (table, nScale) match {
      case (_, "0") => 0L
      case (CATALOG_RETURNS, "0.01") => 8923L
      case (CATALOG_RETURNS, "1") => 144067L
      case (CATALOG_RETURNS, "10") => 1439749L
      case (CATALOG_RETURNS, "100") => 14404374L
      case (CATALOG_RETURNS, "300") => 43193472L
      case (CATALOG_RETURNS, "1000") => 143996756L
      case (CATALOG_RETURNS, "3000") => 432018033L
      case (CATALOG_RETURNS, "10000") => 1440033112L
      case (CATALOG_RETURNS, "30000") => 4319925093L
      case (CATALOG_RETURNS, "100000") => 14400175879L
      case (CATALOG_SALES, "0.01") => 89807L
      case (CATALOG_SALES, "1") => 1441548L
      case (CATALOG_SALES, "10") => 14401261L
      case (CATALOG_SALES, "100") => 143997065L
      case (CATALOG_SALES, "300") => 431969836L
      case (CATALOG_SALES, "1000") => 1439980416L
      case (CATALOG_SALES, "3000") => 4320078880L
      case (CATALOG_SALES, "10000") => 14399964710L
      case (CATALOG_SALES, "30000") => 43200404822L
      case (CATALOG_SALES, "100000") => 143999334399L
      case (STORE_RETURNS, "0.01") => 11925L
      case (STORE_RETURNS, "1") => 287514L
      case (STORE_RETURNS, "10") => 2875432L
      case (STORE_RETURNS, "100") => 28795080L
      case (STORE_RETURNS, "300") => 86393244L
      case (STORE_RETURNS, "1000") => 287999764L
      case (STORE_RETURNS, "3000") => 863989652L
      case (STORE_RETURNS, "10000") => 2879970104L
      case (STORE_RETURNS, "30000") => 8639952111L
      case (STORE_RETURNS, "100000") => 28800018820L
      case (STORE_SALES, "0.01") => 120527L
      case (STORE_SALES, "1") => 2880404L
      case (STORE_SALES, "10") => 28800991L
      case (STORE_SALES, "100") => 287997024L
      case (STORE_SALES, "300") => 864001869L
      case (STORE_SALES, "1000") => 2879987999L
      case (STORE_SALES, "3000") => 8639936081L
      case (STORE_SALES, "10000") => 28799983563L
      case (STORE_SALES, "30000") => 86399341874L
      case (STORE_SALES, "100000") => 287997818084L
      case (WEB_RETURNS, "0.01") => 1152L
      case (WEB_RETURNS, "1") => 71763L
      case (WEB_RETURNS, "10") => 719217L
      case (WEB_RETURNS, "100") => 7197670L
      case (WEB_RETURNS, "300") => 21599377L
      case (WEB_RETURNS, "1000") => 71997522L
      case (WEB_RETURNS, "3000") => 216003761L
      case (WEB_RETURNS, "10000") => 720020485L
      case (WEB_RETURNS, "30000") => 2160007345L
      case (WEB_RETURNS, "100000") => 7199904459L
      case (WEB_SALES, "0.01") => 11876L
      case (WEB_SALES, "1") => 719384L
      case (WEB_SALES, "10") => 7197566L
      case (WEB_SALES, "100") => 72001237L
      case (WEB_SALES, "300") => 216009853L
      case (WEB_SALES, "1000") => 720000376L
      case (WEB_SALES, "3000") => 2159968881L
      case (WEB_SALES, "10000") => 7199963324L
      case (WEB_SALES, "30000") => 21600036511L
      case (WEB_SALES, "100000") => 71999670164L
      case (t, s) => new Scaling(s.toDouble).getRowCount(t)
    }
  }

  def sizeInBytes(table: Table, scale: Double): Long =
    numRows(table, scale) * TABLE_AVG_ROW_BYTES(table)

  private val TABLE_AVG_ROW_BYTES: Map[Table, Long] = Map(
    CALL_CENTER -> 305,
    CATALOG_PAGE -> 139,
    CATALOG_RETURNS -> 166,
    CATALOG_SALES -> 226,
    CUSTOMER -> 132,
    CUSTOMER_ADDRESS -> 110,
    CUSTOMER_DEMOGRAPHICS -> 42,
    DATE_DIM -> 141,
    HOUSEHOLD_DEMOGRAPHICS -> 21,
    INCOME_BAND -> 16,
    INVENTORY -> 16,
    ITEM -> 281,
    PROMOTION -> 124,
    REASON -> 38,
    SHIP_MODE -> 56,
    STORE -> 263,
    STORE_RETURNS -> 134,
    STORE_SALES -> 164,
    TIME_DIM -> 59,
    WAREHOUSE -> 117,
    WEB_PAGE -> 96,
    WEB_RETURNS -> 162,
    WEB_SALES -> 226,
    WEB_SITE -> 292)
}
