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

package org.apache.spark.sql.kyuubi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.kyuubi.engine.spark.schema.RowSet

object SparkDatasetHelper {
  def toArrowBatchRdd[T](ds: Dataset[T]): RDD[Array[Byte]] = {
    ds.toArrowBatchRdd
  }

  def convertTopLevelComplexTypeToHiveString(
      df: DataFrame,
      timestampAsString: Boolean): DataFrame = {

    val quotedCol = (name: String) => col(quoteIfNeeded(name))

    // an udf to call `RowSet.toHiveString` on complex types(struct/array/map) and timestamp type.
    val toHiveStringUDF = udf[String, Row, String]((row, schemaDDL) => {
      val dt = DataType.fromDDL(schemaDDL)
      dt match {
        case StructType(Array(StructField(_, st: StructType, _, _))) =>
          RowSet.toHiveString((row, st), nested = true)
        case StructType(Array(StructField(_, at: ArrayType, _, _))) =>
          RowSet.toHiveString((row.toSeq.head, at), nested = true)
        case StructType(Array(StructField(_, mt: MapType, _, _))) =>
          RowSet.toHiveString((row.toSeq.head, mt), nested = true)
        case StructType(Array(StructField(_, tt: TimestampType, _, _))) =>
          RowSet.toHiveString((row.toSeq.head, tt), nested = true)
        case _ =>
          throw new UnsupportedOperationException
      }
    })

    val cols = df.schema.map {
      case sf @ StructField(name, _: StructType, _, _) =>
        toHiveStringUDF(quotedCol(name), lit(sf.toDDL)).as(name)
      case sf @ StructField(name, _: MapType | _: ArrayType, _, _) =>
        toHiveStringUDF(struct(quotedCol(name)), lit(sf.toDDL)).as(name)
      case sf @ StructField(name, _: TimestampType, _, _) if timestampAsString =>
        toHiveStringUDF(struct(quotedCol(name)), lit(sf.toDDL)).as(name)
      case StructField(name, _, _, _) => quotedCol(name)
    }
    df.select(cols: _*)
  }

  /**
   * Fork from Apache Spark-3.3.1 org.apache.spark.sql.catalyst.util.quoteIfNeeded to adapt to
   * Spark-3.1.x
   */
  def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }
}
