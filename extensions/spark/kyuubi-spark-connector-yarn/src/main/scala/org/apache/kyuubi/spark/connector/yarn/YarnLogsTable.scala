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
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class YarnLogsTable extends Table with SupportsRead {
  override def name(): String = "app_log"

  override def schema(): StructType =
    new StructType(Array(
      StructField("appId", StringType, nullable = false),
      StructField("user", StringType, nullable = false),
      StructField("rowIndex", IntegerType, nullable = false),
      StructField("message", StringType, nullable = true)))

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): Scan =
    new YarnLogsScan(
      caseInsensitiveStringMap,
      schema())
}
