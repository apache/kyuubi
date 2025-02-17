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

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.execution.{CollectLimitExec, HiveResult, LocalTableScanExec, QueryExecution, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.arrow.KyuubiArrowConverters
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.kyuubi.engine.spark.KyuubiSparkUtil
import org.apache.kyuubi.engine.spark.schema.RowSet
import org.apache.kyuubi.engine.spark.util.SparkCatalogUtils.quoteIfNeeded
import org.apache.kyuubi.util.reflect.{DynClasses, DynMethods}
import org.apache.kyuubi.util.reflect.ReflectUtils._

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
    // TODO: replace with pattern match once we drop Spark 3.1 support.
    case command: SparkPlan if isCommandResultExec(command) =>
      doCommandResultExec(command)
    case localTableScan: LocalTableScanExec =>
      doLocalTableScan(localTableScan)
    case plan: SparkPlan =>
      toArrowBatchRdd(plan).collect()
  }

  private val datasetClz = DynClasses.builder()
    .impl("org.apache.spark.sql.classic.Dataset") // SPARK-49700 (4.0.0)
    .impl("org.apache.spark.sql.Dataset")
    .build()

  private val toArrowBatchRddMethod =
    DynMethods.builder("toArrowBatchRdd")
      .impl(datasetClz)
      .buildChecked()

  def toArrowBatchRdd[T](ds: Dataset[T]): RDD[Array[Byte]] = {
    toArrowBatchRddMethod.bind(ds).invoke()
  }

  /**
   * Forked from [[Dataset.toArrowBatchRdd(plan: SparkPlan)]].
   * Convert to an RDD of serialized ArrowRecordBatches.
   */
  def toArrowBatchRdd(plan: SparkPlan): RDD[Array[Byte]] = {
    val schemaCaptured = plan.schema
    // TODO: SparkPlan.session introduced in SPARK-35798, replace with SparkPlan.session once we
    // drop Spark 3.1 support.
    val maxRecordsPerBatch = SparkSession.active.sessionState.conf.arrowMaxRecordsPerBatch
    val timeZoneId = SparkSession.active.sessionState.conf.sessionLocalTimeZone
    // note that, we can't pass the lazy variable `maxBatchSize` directly, this is because input
    // arguments are serialized and sent to the executor side for execution.
    val maxBatchSizePerBatch = maxBatchSize
    plan.execute().mapPartitionsInternal { iter =>
      KyuubiArrowConverters.toBatchIterator(
        iter,
        schemaCaptured,
        maxRecordsPerBatch,
        maxBatchSizePerBatch,
        -1,
        timeZoneId)
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
    // TODO: reuse the timeFormatters on greater scale if possible,
    //       recreate timeFormatters each time may cause performance issue, see KYUUBI #5811
    val toHiveStringUDF = udf[String, Row, String]((row, schemaDDL) => {
      val dt = DataType.fromDDL(schemaDDL)
      dt match {
        case StructType(Array(StructField(_, st: StructType, _, _))) =>
          RowSet.toHiveString(
            (row, st),
            nested = true,
            timeFormatters = HiveResult.getTimeFormatters,
            binaryFormatter = RowSet.getBinaryFormatter)
        case StructType(Array(StructField(_, at: ArrayType, _, _))) =>
          RowSet.toHiveString(
            (row.toSeq.head, at),
            nested = true,
            timeFormatters = HiveResult.getTimeFormatters,
            binaryFormatter = RowSet.getBinaryFormatter)
        case StructType(Array(StructField(_, mt: MapType, _, _))) =>
          RowSet.toHiveString(
            (row.toSeq.head, mt),
            nested = true,
            timeFormatters = HiveResult.getTimeFormatters,
            binaryFormatter = RowSet.getBinaryFormatter)
        case StructType(Array(StructField(_, tt: TimestampType, _, _))) =>
          RowSet.toHiveString(
            (row.toSeq.head, tt),
            nested = true,
            timeFormatters = HiveResult.getTimeFormatters,
            binaryFormatter = RowSet.getBinaryFormatter)
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
    // drop Spark 3.1 support.
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

  private lazy val commandResultExecRowsMethod = DynMethods.builder("rows")
    .impl("org.apache.spark.sql.execution.CommandResultExec")
    .build()

  private def doCommandResultExec(command: SparkPlan): Array[Array[Byte]] = {
    val spark = SparkSession.active
    // TODO: replace with `command.rows` once we drop Spark 3.1 support.
    val rows = commandResultExecRowsMethod.invoke[Seq[InternalRow]](command)
    command.longMetric("numOutputRows").add(rows.size)
    sendDriverMetrics(spark.sparkContext, command.metrics)
    KyuubiArrowConverters.toBatchIterator(
      rows.iterator,
      command.schema,
      spark.sessionState.conf.arrowMaxRecordsPerBatch,
      maxBatchSize,
      -1,
      spark.sessionState.conf.sessionLocalTimeZone).toArray
  }

  private def doLocalTableScan(localTableScan: LocalTableScanExec): Array[Array[Byte]] = {
    val spark = SparkSession.active
    localTableScan.longMetric("numOutputRows").add(localTableScan.rows.size)
    sendDriverMetrics(spark.sparkContext, localTableScan.metrics)
    KyuubiArrowConverters.toBatchIterator(
      localTableScan.rows.iterator,
      localTableScan.schema,
      spark.sessionState.conf.arrowMaxRecordsPerBatch,
      maxBatchSize,
      -1,
      spark.sessionState.conf.sessionLocalTimeZone).toArray
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
    val plan = invokeAs[SparkPlan](adaptiveSparkPlanExec, "getFinalPhysicalPlan")
    val result = fun(plan)
    invokeAs[Unit](adaptiveSparkPlanExec, "finalPlanUpdate")
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

  private def isCommandResultExec(sparkPlan: SparkPlan): Boolean = {
    // scalastyle:off line.size.limit
    // the CommandResultExec was introduced in SPARK-35378 (Spark 3.2), after SPARK-35378 the
    // physical plan of runnable command is CommandResultExec.
    // for instance:
    // ```
    // scala> spark.sql("show tables").queryExecution.executedPlan
    // res0: org.apache.spark.sql.execution.SparkPlan =
    // CommandResult <empty>, [namespace#0, tableName#1, isTemporary#2]
    //   +- ShowTables [namespace#0, tableName#1, isTemporary#2], V2SessionCatalog(spark_catalog), [default]
    //
    // scala > spark.sql("show tables").queryExecution.executedPlan.getClass
    // res1: Class[_ <: org.apache.spark.sql.execution.SparkPlan] = class org.apache.spark.sql.execution.CommandResultExec
    // ```
    // scalastyle:on line.size.limit
    sparkPlan.getClass.getName == "org.apache.spark.sql.execution.CommandResultExec"
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

  private def sendDriverMetrics(sc: SparkContext, metrics: Map[String, SQLMetric]): Unit = {
    val executionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sc, executionId, metrics.values.toSeq)
  }

  private[kyuubi] def optimizedPlanLimit(queryExecution: QueryExecution): Option[Long] =
    queryExecution.optimizedPlan match {
      case globalLimit: GlobalLimit => globalLimit.maxRows
      case _ => None
    }

  def shouldSaveResultToFs(
      resultMaxRows: Int,
      minSize: Long,
      minRows: Long,
      result: DataFrame): Boolean = {
    if (isCommandExec(result) ||
      (resultMaxRows > 0 && resultMaxRows < minRows) ||
      result.queryExecution.optimizedPlan.stats.rowCount.getOrElse(
        BigInt(Long.MaxValue)) < minRows) {
      return false
    }
    val finalLimit: Option[Long] = optimizedPlanLimit(result.queryExecution) match {
      case Some(limit) if resultMaxRows > 0 => Some(math.min(limit, resultMaxRows))
      case Some(limit) => Some(limit)
      case None if resultMaxRows > 0 => Some(resultMaxRows)
      case _ => None
    }
    if (finalLimit.exists(_ < minRows)) {
      return false
    }
    val sizeInBytes = result.queryExecution.optimizedPlan.stats.sizeInBytes
    val stats = finalLimit.map { limit =>
      val estimateSize =
        limit * EstimationUtils.getSizePerRow(result.queryExecution.executedPlan.output)
      estimateSize min sizeInBytes
    }.getOrElse(sizeInBytes)
    val colSize =
      if (result == null || result.schema.isEmpty) {
        0
      } else {
        result.schema.size
      }
    minSize > 0 && colSize > 0 && stats >= minSize
  }

  def isCommandExec(result: DataFrame): Boolean = {
    val nodeName = result.queryExecution.executedPlan.getClass.getName
    nodeName == "org.apache.spark.sql.execution.command.ExecutedCommandExec" ||
    nodeName == "org.apache.spark.sql.execution.CommandResultExec"
  }
}
