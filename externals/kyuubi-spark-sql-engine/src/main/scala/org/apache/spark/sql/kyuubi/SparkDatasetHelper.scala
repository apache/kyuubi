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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.{CollectLimitExec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.arrow.{ArrowConverters, KyuubiArrowConverters}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.kyuubi.engine.spark.KyuubiSparkUtil
import org.apache.kyuubi.engine.spark.schema.RowSet
import org.apache.kyuubi.reflection.DynMethods

object SparkDatasetHelper extends Logging {

  def executeCollect(df: DataFrame): Array[Array[Byte]] = withNewExecutionId(df) {
    executeArrowBatchCollect(df.queryExecution.executedPlan)
  }

  def executeArrowBatchCollect: SparkPlan => Array[Array[Byte]] = {
    case adaptiveSparkPlan: AdaptiveSparkPlanExec =>
      executeArrowBatchCollect(finalPhysicalPlan(adaptiveSparkPlan))
    // TODO: avoid extra shuffle if `offset` > 0
    case collectLimit: CollectLimitExec if offset(collectLimit) > 0 =>
      logWarning("unsupported offset > 0, an extra shuffle will be introduced.")
      toArrowBatchRdd(collectLimit).collect()
    case collectLimit: CollectLimitExec if collectLimit.limit >= 0 =>
      doCollectLimit(collectLimit)
    case collectLimit: CollectLimitExec if collectLimit.limit < 0 =>
      executeArrowBatchCollect(collectLimit.child)
    case plan: SparkPlan =>
      toArrowBatchRdd(plan).collect()
  }

  def toArrowBatchRdd[T](ds: Dataset[T]): RDD[Array[Byte]] = {
    ds.toArrowBatchRdd
  }

  /**
   * Forked from [[Dataset.toArrowBatchRdd(plan: SparkPlan)]].
   * Convert to an RDD of serialized ArrowRecordBatches.
   */
  def toArrowBatchRdd(plan: SparkPlan): RDD[Array[Byte]] = {
    val schemaCaptured = plan.schema
    // TODO: SparkPlan.session introduced in SPARK-35798, replace with SparkPlan.session once we
    // drop Spark-3.1.x support.
    val maxRecordsPerBatch = SparkSession.active.sessionState.conf.arrowMaxRecordsPerBatch
    val timeZoneId = SparkSession.active.sessionState.conf.sessionLocalTimeZone
    plan.execute().mapPartitionsInternal { iter =>
      val context = TaskContext.get()
      ArrowConverters.toBatchIterator(
        iter,
        schemaCaptured,
        maxRecordsPerBatch,
        timeZoneId,
        context)
    }
  }

  def toArrowBatchLocalIterator(df: DataFrame): Iterator[Array[Byte]] = {
    withNewExecutionId(df) {
      toArrowBatchRdd(df).toLocalIterator
    }
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
  private def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }

  private lazy val maxBatchSize: Long = {
    // respect spark connect config
    KyuubiSparkUtil.globalSparkContext
      .getConf
      .getOption("spark.connect.grpc.arrow.maxBatchSize")
      .orElse(Option("4m"))
      .map(JavaUtils.byteStringAs(_, ByteUnit.MiB))
      .get
  }

  private def doCollectLimit(collectLimit: CollectLimitExec): Array[Array[Byte]] = {
    // TODO: SparkPlan.session introduced in SPARK-35798, replace with SparkPlan.session once we
    // drop Spark-3.1.x support.
    val timeZoneId = SparkSession.active.sessionState.conf.sessionLocalTimeZone
    val maxRecordsPerBatch = SparkSession.active.sessionState.conf.arrowMaxRecordsPerBatch

    val batches = KyuubiArrowConverters.takeAsArrowBatches(
      collectLimit,
      maxRecordsPerBatch,
      maxBatchSize,
      timeZoneId)

    // note that the number of rows in the returned arrow batches may be >= `limit`, perform
    // the slicing operation of result
    val result = ArrayBuffer[Array[Byte]]()
    var i = 0
    var rest = collectLimit.limit
    while (i < batches.length && rest > 0) {
      val (batch, size) = batches(i)
      if (size <= rest) {
        result += batch
        // returned ArrowRecordBatch has less than `limit` row count, safety to do conversion
        rest -= size.toInt
      } else { // size > rest
        result += KyuubiArrowConverters.slice(collectLimit.schema, timeZoneId, batch, 0, rest)
        rest = 0
      }
      i += 1
    }
    result.toArray
  }

  /**
   * This method provides a reflection-based implementation of
   * [[AdaptiveSparkPlanExec.finalPhysicalPlan]] that enables us to adapt to the Spark runtime
   * without patching SPARK-41914.
   *
   * TODO: Once we drop support for Spark 3.1.x, we can directly call
   * [[AdaptiveSparkPlanExec.finalPhysicalPlan]].
   */
  def finalPhysicalPlan(adaptiveSparkPlanExec: AdaptiveSparkPlanExec): SparkPlan = {
    withFinalPlanUpdate(adaptiveSparkPlanExec, identity)
  }

  /**
   * A reflection-based implementation of [[AdaptiveSparkPlanExec.withFinalPlanUpdate]].
   */
  private def withFinalPlanUpdate[T](
      adaptiveSparkPlanExec: AdaptiveSparkPlanExec,
      fun: SparkPlan => T): T = {
    val getFinalPhysicalPlan = DynMethods.builder("getFinalPhysicalPlan")
      .hiddenImpl(adaptiveSparkPlanExec.getClass)
      .build()
    val plan = getFinalPhysicalPlan.invoke[SparkPlan](adaptiveSparkPlanExec)
    val result = fun(plan)
    val finalPlanUpdate = DynMethods.builder("finalPlanUpdate")
      .hiddenImpl(adaptiveSparkPlanExec.getClass)
      .build()
    finalPlanUpdate.invoke[Unit](adaptiveSparkPlanExec)
    result
  }

  /**
   * offset support was add since Spark-3.4(set SPARK-28330), to ensure backward compatibility with
   * earlier versions of Spark, this function uses reflective calls to the "offset".
   */
  private def offset(collectLimitExec: CollectLimitExec): Int = {
    Option(
      DynMethods.builder("offset")
        .impl(collectLimitExec.getClass)
        .orNoop()
        .build()
        .invoke[Int](collectLimitExec))
      .getOrElse(0)
  }

  /**
   * refer to org.apache.spark.sql.Dataset#withAction(), assign a new execution id for arrow-based
   * operation, so that we can track the arrow-based queries on the UI tab.
   */
  private def withNewExecutionId[T](df: DataFrame)(body: => T): T = {
    SQLExecution.withNewExecutionId(df.queryExecution, Some("collectAsArrow")) {
      df.queryExecution.executedPlan.resetMetrics()
      body
    }
  }
}
