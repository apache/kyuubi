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

package org.apache.kyuubi.spark.connector.yarn

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class YarnLogScanBuilder(options: CaseInsensitiveStringMap, schema: StructType) {

  private var pushed: Array[Filter] = Array.empty

  def pushedFilters(): Array[Filter] = pushed

  def build(): Scan = YarnAppScan(options, schema, pushed)

  def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supportedFilter, unsupportedFilter) = filters.partition {
      case filter: EqualTo =>
        filter match {
          case EqualTo("app_id", _) => true
          case EqualTo("user", _) => true
          case EqualTo("container_id", _) => true
          case _ => false
        }
      case _ => false
    }
    pushed = supportedFilter
    unsupportedFilter
  }
}
