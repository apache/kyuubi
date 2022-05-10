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

object TPCDSTableUtils {

  // https://tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v3.2.0.pdf
  // Page 42 Table 3-2 Database Row Counts
  val tableAvrRowSizeInBytes: Map[String, Long] = Map(
    "call_center" -> 305,
    "catalog_page" -> 139,
    "catalog_returns" -> 166,
    "catalog_sales" -> 226,
    "customer" -> 132,
    "customer_address" -> 110,
    "customer_demographics" -> 42,
    "date_dim" -> 141,
    "household_demographics" -> 21,
    "income_band" -> 16,
    "inventory" -> 16,
    "item" -> 281,
    "promotion" -> 124,
    "reason" -> 38,
    "ship_mode" -> 56,
    "store" -> 263,
    "store_returns" -> 134,
    "store_sales" -> 164,
    "time_dim" -> 59,
    "warehouse" -> 117,
    "web_page" -> 96,
    "web_returns" -> 162,
    "web_sales" -> 226,
    "web_site" -> 292)

}
