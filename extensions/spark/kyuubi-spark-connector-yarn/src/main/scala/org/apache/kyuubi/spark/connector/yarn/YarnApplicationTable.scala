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

import java.util

import scala.jdk.CollectionConverters.setAsJavaSetConverter

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class YarnApplicationTable extends Table with SupportsRead {
  override def name(): String = "apps"

  override def schema(): StructType =
    new StructType(Array(
      StructField("id", StringType, nullable = false),
      StructField("type", StringType, nullable = false),
      StructField("user", StringType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("state", StringType, nullable = false),
      StructField("queue", StringType, nullable = false),
      StructField("attempt_id", StringType, nullable = false),
      StructField("submit_time", LongType, nullable = false),
      StructField("launch_time", LongType, nullable = false),
      StructField("start_time", LongType, nullable = false),
      StructField("finish_time", LongType, nullable = false)))

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder =
    new YarnAppScan(
      caseInsensitiveStringMap,
      schema())
}
