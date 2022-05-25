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

import io.trino.tpch.TpchTable
import io.trino.tpch.TpchTable._

// https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.0.pdf
// Page 88 Table 3: Estimated Database Size
object TPCHStatisticsUtils {

  val SCALES: Array[Int] = Array(0, 1, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000)

  def numRows(table: TpchTable[_], scale: Int): Long = {
    require(SCALES.contains(scale), s"Unsupported scale $scale")
    (table, scale) match {
      case (_, 0) => 0L
      case (CUSTOMER, scale) => 150000L * scale
      case (ORDERS, scale) => 1500000L * scale
      case (LINE_ITEM, 1) => 6001215L
      case (LINE_ITEM, 10) => 59986052L
      case (LINE_ITEM, 30) => 179998372L
      case (LINE_ITEM, 100) => 600037902L
      case (LINE_ITEM, 300) => 1799989091L
      case (LINE_ITEM, 1000) => 5999989709L
      case (LINE_ITEM, 3000) => 18000048306L
      case (LINE_ITEM, 10000) => 59999994267L
      case (LINE_ITEM, 30000) => 179999978268L
      case (LINE_ITEM, 100000) => 599999969200L
      case (PART, scale) => 200000L * scale
      case (PART_SUPPLIER, scale) => 800000L * scale
      case (SUPPLIER, scale) => 10000L * scale
      case (NATION, _) => 25L
      case (REGION, _) => 25L
    }
  }

  def sizeInBytes(table: TpchTable[_], scale: Int): Long =
    numRows(table, scale) * TABLE_AVG_ROW_BYTES(table)

  private val TABLE_AVG_ROW_BYTES: Map[TpchTable[_], Long] = Map(
    CUSTOMER -> 179,
    ORDERS -> 104,
    LINE_ITEM -> 112,
    PART -> 155,
    PART_SUPPLIER -> 144,
    SUPPLIER -> 159,
    NATION -> 128,
    REGION -> 124)
}
