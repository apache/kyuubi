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

class CompactJsonTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "json"
  override def getTableCodec(): Option[String] = None
  override def getDataFileSuffix(): String = ".json"
}

class CompactGzJsonTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "json"
  override def getTableCodec(): Option[String] = Some("gzip")
  override def getDataFileSuffix(): String = ".json.gz"
}

class CompactCsvTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "csv"
  override def getTableCodec(): Option[String] = None
  override def getDataFileSuffix(): String = ".csv"
}

class CompactGzCsvTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "csv"
  override def getTableCodec(): Option[String] = Some("gzip")
  override def getDataFileSuffix(): String = ".csv.gz"
}

class CompactParquetTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "parquet"
  override def getTableCodec(): Option[String] = None
  override def getDataFileSuffix(): String = ".parquet"
}

class CompactSnappyParquetTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "parquet"
  override def getTableCodec(): Option[String] = Some("snappy")
  override def getDataFileSuffix(): String = ".snappy.parquet"
}

class CompactAvroTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "avro"
  override def getTableCodec(): Option[String] = None
  override def getDataFileSuffix(): String = ".avro"
}

class CompactSnappyAvroTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "avro"
  override def getTableCodec(): Option[String] = Some("snappy")
  override def getDataFileSuffix(): String = ".avro"
}
