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

package org.apache.spark.sql

import java.util.OptionalLong

import org.apache.spark.sql.connector._
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class ReportStatisticsDataSource extends SimpleWritableDataSource {

  class MyScanBuilder extends SimpleScanBuilder
    with SupportsReportStatistics {

    override def estimateStatistics(): Statistics = {
      new Statistics {
        override def sizeInBytes(): OptionalLong = OptionalLong.of(80)

        override def numRows(): OptionalLong = OptionalLong.of(10)
      }
    }

    override def planInputPartitions(): Array[InputPartition] = {
      Array(RangeInputPartition(0, 5), RangeInputPartition(5, 10))
    }

  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new SimpleBatchTable {
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new MyScanBuilder
      }
    }
  }
}
