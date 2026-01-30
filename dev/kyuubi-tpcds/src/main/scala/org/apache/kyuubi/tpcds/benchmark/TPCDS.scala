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

package org.apache.kyuubi.tpcds.benchmark

import scala.io.{Codec, Source}

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.tpcds.benchmark.ExecutionMode._

/**
 * TPC-DS benchmark's dataset.
 */
class TPCDS(@transient sparkSession: SparkSession)
  extends Benchmark(sparkSession)
  with TPCDS_2_4_Queries
  with Serializable {

  override val tpcds2_4Queries: Seq[Query] = queryNames.map { queryName =>
    val in = getClass.getClassLoader.getResourceAsStream(s"tpcds_2_4/$queryName.sql")
    var queryContent: String = ""
    try {
      queryContent = Source.fromInputStream(in)(Codec.UTF8).mkString
  } finally {
    in.close()
  }

    val modeName: String = sparkSession.conf.get("spark.sql.benchmark.executionMode")
    val resultsLocation: String = sparkSession.conf.get("spark.sql.perf.results")

    val executionMode: ExecutionMode = modeName match {
      case "collect" => CollectResults
      case "foreach" => ForeachResults
      case "hash" => HashResults
      case "saveToParquet" => WriteParquet(s"${resultsLocation}_query_results")
      case _ =>
        throw new IllegalArgumentException(s"Unsupported mode: $modeName")
    }
    Query(
      queryName + "-v2.4",
      queryContent,
      description = "TPC-DS 2.4 Query",
      executionMode = executionMode)
  }
}
