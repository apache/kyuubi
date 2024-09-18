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

class CompactDeflateJsonTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "json"
  override def getTableCodec(): Option[String] = Some("deflate")
  override def getDataFileSuffix(): String = ".json.deflate"
}

class CompactLz4JsonTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "json"
  override def getTableCodec(): Option[String] = Some("lz4")
  override def getDataFileSuffix(): String = ".json.lz4"
}

class CompactSnappyJsonTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "json"
  override def getTableCodec(): Option[String] = Some("snappy")
  override def getDataFileSuffix(): String = ".json.snappy"
}

class CompactGzJsonTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "json"
  override def getTableCodec(): Option[String] = Some("gzip")
  override def getDataFileSuffix(): String = ".json.gz"
}

class CompactBzipJsonTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "json"
  override def getTableCodec(): Option[String] = Some("bzip2")
  override def getDataFileSuffix(): String = ".json.bz2"
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

class CompactBzipCsvTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "csv"
  override def getTableCodec(): Option[String] = Some("bzip2")
  override def getDataFileSuffix(): String = ".csv.bz2"
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

class CompactZstdParquetTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "parquet"
  override def getTableCodec(): Option[String] = Some("zstd")
  override def getDataFileSuffix(): String = ".zstd.parquet"
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

class CompactOrcTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "orc"
  override def getTableCodec(): Option[String] = None
  override def getDataFileSuffix(): String = ".orc"
}

class CompactLz4OrcTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "orc"
  override def getTableCodec(): Option[String] = Some("lz4")
  override def getDataFileSuffix(): String = ".lz4.orc"
}

class CompactZlibOrcTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "orc"
  override def getTableCodec(): Option[String] = Some("zlib")
  override def getDataFileSuffix(): String = ".zlib.orc"
}

class CompactSnappyOrcTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "orc"
  override def getTableCodec(): Option[String] = Some("snappy")
  override def getDataFileSuffix(): String = ".snappy.orc"
}

class CompactZstdOrcTableSuiteBase extends CompactTablSuiteBase {
  override def getTableSource(): String = "orc"
  override def getTableCodec(): Option[String] = Some("zstd")
  override def getDataFileSuffix(): String = ".zstd.orc"
}
